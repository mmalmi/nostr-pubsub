use super::{
    Arc, ClientInner, Duration, EventId, EventSource, FipsEndpoint, FipsPubsubClient,
    FipsPubsubClientOptions, FipsPubsubClientPolicies, FipsPubsubWireMessage, HashSet,
    PeerIdentity, PubsubError, Result, SourceId, VerifiedEvent, invalid_option, oneshot, poisoned,
};
use crate::privacy_roles::{ClientRole, is_private};

/// Evidence that this exact authenticated provider admitted this event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FipsRemotePublication {
    pub event_id: EventId,
    pub provider: String,
}

pub(super) struct PendingPublication {
    pub(super) providers: HashSet<String>,
    pub(super) reply: Option<oneshot::Sender<Result<FipsRemotePublication>>>,
}

/// A one-event, send-only operation. Create a new random endpoint for every
/// event, recipient copy and retry; never use an account/live endpoint here.
pub struct FipsAnonymousPublisher {
    pub(super) client: FipsPubsubClient,
}

impl FipsAnonymousPublisher {
    pub async fn start(
        endpoint: Arc<FipsEndpoint>,
        options: FipsPubsubClientOptions,
    ) -> Result<Self> {
        if options.routed_peers.is_empty() || options.max_inbound_routed_peers != 0 {
            return Err(invalid_option(
                "anonymous publisher requires explicit providers and no inbound clients",
            ));
        }
        let client = FipsPubsubClient::start_for_role(
            endpoint,
            options,
            None,
            HashSet::new(),
            FipsPubsubClientPolicies::default(),
            ClientRole::Publisher,
        )
        .await?;
        Ok(Self { client })
    }

    /// Consumes the operation and waits for a provider-bound remote OK.
    /// Failure or cancellation must leave the caller's durable queue pending.
    pub async fn publish_once(
        self,
        event: VerifiedEvent,
        timeout: Duration,
    ) -> Result<FipsRemotePublication> {
        let result = self.publish_inner(event, timeout).await;
        self.client.shutdown_shared().await;
        result
    }

    async fn publish_inner(
        &self,
        event: VerifiedEvent,
        timeout: Duration,
    ) -> Result<FipsRemotePublication> {
        validate_envelope(&event)?;
        if timeout.is_zero() {
            return Err(invalid_option("publication timeout must be positive"));
        }
        let event_id = event.as_event().id;
        let peers = self.client.inner.connected_peers().await?;
        if peers.is_empty() {
            return Err(invalid_option(
                "anonymous publisher has no selected provider",
            ));
        }
        let (reply, response) = oneshot::channel();
        self.client
            .inner
            .publications
            .lock()
            .map_err(|_| poisoned("pending publication"))?
            .insert(
                event_id,
                PendingPublication {
                    providers: peers.iter().map(|peer| peer.npub.clone()).collect(),
                    reply: Some(reply),
                },
            );
        let frame = self
            .client
            .inner
            .codec
            .encode_frame(&FipsPubsubWireMessage::publish(event))?;
        for peer in peers {
            self.client.inner.send_frame(peer.identity, frame.clone())?;
        }
        tokio::time::timeout(timeout, response)
            .await
            .map_err(|_| PubsubError::Storage("anonymous provider receipt timed out".into()))?
            .map_err(|_| PubsubError::Storage("anonymous provider receipt channel closed".into()))?
    }
}

fn validate_envelope(event: &VerifiedEvent) -> Result<()> {
    let event = event.as_event();
    let tags = event
        .tags
        .iter()
        .map(nostr::Tag::as_slice)
        .collect::<Vec<_>>();
    let recipients = tags
        .iter()
        .filter(|tag| tag.first().is_some_and(|v| v == "p"))
        .collect::<Vec<_>>();
    let recipient_ok = recipients.len() == 1
        && recipients[0].len() == 2
        && recipients[0][1].len() == 64
        && recipients[0][1].bytes().all(|c| c.is_ascii_hexdigit());
    let v2 = tags
        .iter()
        .filter(|tag| tag.first().is_some_and(|v| v == "v"))
        .collect::<Vec<_>>();
    if !is_private(event.kind.as_u16())
        || !recipient_ok
        || (event.kind.as_u16() == 1060 && (v2.len() != 1 || (v2[0].len() != 2 || v2[0][1] != "2")))
    {
        return Err(invalid_option(
            "anonymous publisher requires one recipient-bound private envelope",
        ));
    }
    Ok(())
}

impl ClientInner {
    pub(super) fn handle_publication_receipt(
        &self,
        peer: &str,
        event_id: EventId,
        accepted: bool,
        message: &str,
    ) {
        let Ok(mut pending) = self.publications.lock() else {
            return;
        };
        let Some(operation) = pending.get_mut(&event_id) else {
            return;
        };
        if !operation.providers.remove(peer) {
            return;
        }
        if accepted || operation.providers.is_empty() {
            if let Some(reply) = operation.reply.take() {
                let result = if accepted {
                    Ok(FipsRemotePublication {
                        event_id,
                        provider: peer.to_owned(),
                    })
                } else {
                    Err(PubsubError::Storage(format!(
                        "provider rejected anonymous publication: {message}"
                    )))
                };
                let _ = reply.send(result);
            }
            pending.remove(&event_id);
        }
    }

    pub(super) async fn accept_publication(&self, peer: PeerIdentity, event: VerifiedEvent) {
        let source = EventSource::fips_endpoint(peer.npub());
        let mut accepted = self.event_is_admitted(&event, &source).await;
        let event_id = event.as_event().id;
        if accepted {
            // Admission is independent of sender subscription state. Inbound
            // anonymous clients are not added to the outgoing provider roster.
            let _ = self.deliver_local_event(&peer.npub(), &event);
            match self.remember_event(event.clone(), source, self.options.max_hops) {
                Ok(true) => {
                    if let Ok(targets) =
                        self.peer_delivery_targets(&event, Some(&SourceId::new(peer.npub())))
                    {
                        let _ = self.send_inventories(targets, &event, self.options.max_hops);
                    }
                }
                Ok(false) => {}
                Err(_) => accepted = false,
            }
        }
        let message = FipsPubsubWireMessage::Ok {
            event_id,
            accepted,
            message: if accepted {
                String::new()
            } else {
                "blocked: event policy".into()
            },
        };
        if let Ok(frame) = self.codec.encode_frame(&message) {
            let _ = self.send_frame(peer, frame);
        }
    }
}
