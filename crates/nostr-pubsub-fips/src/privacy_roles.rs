use super::{
    Arc, ClientInner, Filter, FipsEndpoint, FipsPubsubClient, FipsPubsubClientOptions,
    FipsPubsubClientPolicies, FipsPubsubWireMessage, HashSet, PeerIdentity, Result, VerifiedEvent,
    default_peerfinding_filter, invalid_option, poisoned, storage_error,
};
use nostr::PublicKey;

/// One public lookup subject. Reverse device authorization is restricted to `AppKeys`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FipsDiscoveryScope {
    Author(PublicKey),
    Device(PublicKey),
}

impl FipsDiscoveryScope {
    pub fn from_filter(filter: &Filter) -> Result<Self> {
        let value = serde_json::to_value(filter).map_err(|e| storage_error("encode filter", e))?;
        let authors = value.get("authors").and_then(serde_json::Value::as_array);
        let devices = value.get("#p").and_then(serde_json::Value::as_array);
        let key = match (authors, devices) {
            (Some(keys), None) if keys.len() == 1 => (true, &keys[0]),
            (None, Some(keys)) if keys.len() == 1 => (false, &keys[0]),
            _ => {
                return Err(invalid_option(
                    "discovery requires exactly one author or AppKeys device",
                ));
            }
        };
        let public_key = PublicKey::from_hex(key.1.as_str().unwrap_or_default())
            .map_err(|e| invalid_option(format!("invalid discovery key: {e}")))?;
        let scope = if key.0 {
            Self::Author(public_key)
        } else {
            Self::Device(public_key)
        };
        scope.validate(filter)?;
        Ok(scope)
    }

    fn validate(&self, filter: &Filter) -> Result<()> {
        let value = serde_json::to_value(filter).map_err(|e| storage_error("encode filter", e))?;
        let (field, forbidden, key) = match self {
            Self::Author(key) => ("authors", "#p", key),
            Self::Device(key) => ("#p", "authors", key),
        };
        let keys = value.get(field).and_then(serde_json::Value::as_array);
        let valid_key =
            keys.is_some_and(|keys| keys.len() == 1 && keys[0].as_str() == Some(&key.to_hex()));
        let allowed = filter.kinds.as_ref().is_some_and(|kinds| {
            !kinds.is_empty()
                && kinds.iter().all(|kind| match self {
                    Self::Author(_) => matches!(
                        kind.as_u16(),
                        0 | 1059 | 10002 | 10050 | 30078 | 37368 | 37195
                    ),
                    Self::Device(_) => kind.as_u16() == 37368,
                })
        });
        let group_type = value
            .get("#type")
            .and_then(serde_json::Value::as_array)
            .is_some_and(|values| values.iter().any(|v| v.as_str() == Some("group_roster")));
        let group_roster = group_type
            || value
                .get("#d")
                .and_then(serde_json::Value::as_array)
                .is_some_and(|values| {
                    values
                        .iter()
                        .any(|v| v.as_str().is_some_and(|s| s.starts_with("group_roster")))
                });
        if !valid_key || !allowed || value.get(forbidden).is_some() || group_roster {
            return Err(invalid_option(
                "discovery filter crosses its single public subject",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub(super) enum ClientRole {
    Full,
    Receiver(Vec<PublicKey>),
    Discovery(FipsDiscoveryScope),
    Publisher,
}

pub(super) fn is_private(kind: u16) -> bool {
    matches!(kind, 1059 | 1060 | 21111)
}

impl ClientRole {
    pub(super) fn advertises(&self) -> bool {
        matches!(self, Self::Full)
    }

    pub(super) fn validate_filters(&self, filters: &[Filter]) -> Result<()> {
        for filter in filters {
            let mut peer_filter = filter.clone();
            peer_filter.limit = None;
            if peer_filter == default_peerfinding_filter() {
                continue;
            }
            let allowed = match self {
                Self::Full => true,
                Self::Publisher => false,
                Self::Discovery(scope) => {
                    scope.validate(filter)?;
                    true
                }
                Self::Receiver(public_authors) => {
                    let value = serde_json::to_value(filter)
                        .map_err(|e| storage_error("encode filter", e))?;
                    let explicitly_public = filter.authors.as_ref().is_some_and(|authors| {
                        !authors.is_empty()
                            && authors.iter().all(|author| public_authors.contains(author))
                    });
                    filter.kinds.as_ref().is_some_and(|kinds| {
                        !kinds.is_empty()
                            && kinds.iter().all(|kind| {
                                let kind = kind.as_u16();
                                if is_private(kind) {
                                    value.get("#p").is_some()
                                        || filter.authors.as_ref().is_some_and(|v| !v.is_empty())
                                } else if matches!(kind, 30078 | 37368) {
                                    explicitly_public
                                } else if matches!(kind, 0 | 10002 | 10050 | 37195) {
                                    explicitly_public
                                        || (filter.authors.is_none() && value.get("#p").is_none())
                                } else {
                                    filter.authors.is_none() || explicitly_public
                                }
                            })
                    })
                }
            };
            if !allowed {
                return Err(invalid_option(
                    "subscription is not allowed on this FIPS privacy role",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn validate_publication(&self, event: &VerifiedEvent) -> Result<()> {
        if matches!(self, Self::Publisher | Self::Discovery(_))
            || (matches!(self, Self::Receiver(_)) && is_private(event.as_event().kind.as_u16()))
        {
            return Err(invalid_option(
                "publication is not allowed on this FIPS privacy role",
            ));
        }
        Ok(())
    }
}

impl FipsPubsubClient {
    /// Receive only session/invite traffic. Cached private envelopes are never
    /// exported to requesters, including exact-ID and retained replay queries.
    pub async fn start_receive_only(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
    ) -> Result<Self> {
        Self::start_for_role(
            endpoint,
            options,
            None,
            HashSet::new(),
            FipsPubsubClientPolicies::default(),
            ClientRole::Receiver(Vec::new()),
        )
        .await
    }

    /// Permit only these explicit local/public service authors on the receiver.
    /// Never include private contacts or group participants in this list.
    pub async fn start_receive_only_with_public_authors(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
        public_authors: Vec<PublicKey>,
    ) -> Result<Self> {
        Self::start_for_role(
            endpoint,
            options,
            None,
            HashSet::new(),
            FipsPubsubClientPolicies::default(),
            ClientRole::Receiver(public_authors),
        )
        .await
    }

    /// Anonymous, one-subject public lookup connection. The caller supplies a
    /// fresh endpoint distinct from its identified live/receive endpoint.
    pub async fn start_anonymous_discovery(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
        scope: FipsDiscoveryScope,
    ) -> Result<Self> {
        if options.routed_peers.is_empty() {
            return Err(invalid_option(
                "anonymous discovery requires explicit providers",
            ));
        }
        Self::start_for_role(
            endpoint,
            options,
            None,
            HashSet::new(),
            FipsPubsubClientPolicies::default(),
            ClientRole::Discovery(scope),
        )
        .await
    }
}

impl ClientInner {
    pub(super) fn validate_outbound(&self, peer: &PeerIdentity, frame: &[u8]) -> Result<()> {
        let message = self.codec.decode_frame(frame)?;
        let allowed = match (&self.role, &message) {
            (ClientRole::Full, _)
            | (_, FipsPubsubWireMessage::Close { .. })
            | (
                ClientRole::Receiver(_) | ClientRole::Discovery(_),
                FipsPubsubWireMessage::Want { .. },
            ) => true,
            (_, FipsPubsubWireMessage::Req { filters, .. }) => {
                self.role.validate_filters(filters)?;
                true
            }

            (ClientRole::Receiver(_), FipsPubsubWireMessage::Event { event, .. }) => {
                !is_private(event.as_event().kind.as_u16())
            }
            (ClientRole::Receiver(_), FipsPubsubWireMessage::Inv { event_kind, .. }) => {
                !is_private(*event_kind)
            }

            (
                ClientRole::Publisher,
                FipsPubsubWireMessage::Event {
                    subscription_id: None,
                    event,
                },
            ) => self
                .publications
                .lock()
                .map_err(|_| poisoned("pending publication"))?
                .get(&event.as_event().id)
                .is_some_and(|pending| pending.providers.contains(&peer.npub())),
            _ => false,
        };
        if allowed {
            Ok(())
        } else {
            Err(invalid_option(
                "outbound frame is not allowed on this FIPS privacy role",
            ))
        }
    }
}
