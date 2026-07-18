use std::collections::HashSet;

use nostr::JsonUtil;
use serde_json::{Value, json};

use crate::{
    ClientMessage, EventId, Filter, PubsubError, PubsubPeerSubscription,
    PubsubPeerSubscriptionStore, PubsubSubscriptionUpdate, RelayMessage, Result, SourceId,
    SubscriptionId, VerifiedEvent,
};

pub const DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES: usize = 64 * 1024;

/// A supported Nostr message carried by a FIPS pubsub transport frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FipsPubsubWireMessage {
    Req {
        subscription_id: SubscriptionId,
        filters: Vec<Filter>,
    },
    Close {
        subscription_id: SubscriptionId,
    },
    Event {
        subscription_id: Option<SubscriptionId>,
        event: VerifiedEvent,
    },
    /// A subscription-scoped live-event inventory announcement.
    Inv {
        subscription_ids: Vec<SubscriptionId>,
        event_id: EventId,
        event_kind: u16,
        payload_bytes: u32,
        hop_limit: u8,
    },
    /// A request for one advertised event. The response is an ordinary
    /// subscription-scoped `EVENT` frame.
    Want {
        event_id: EventId,
    },
}

impl FipsPubsubWireMessage {
    #[must_use]
    pub fn req(subscription_id: SubscriptionId, filters: Vec<Filter>) -> Self {
        Self::Req {
            subscription_id,
            filters,
        }
    }

    #[must_use]
    pub fn close(subscription_id: SubscriptionId) -> Self {
        Self::Close { subscription_id }
    }

    #[must_use]
    pub fn publish(event: VerifiedEvent) -> Self {
        Self::Event {
            subscription_id: None,
            event,
        }
    }

    #[must_use]
    pub fn deliver(subscription_id: SubscriptionId, event: VerifiedEvent) -> Self {
        Self::Event {
            subscription_id: Some(subscription_id),
            event,
        }
    }

    #[must_use]
    pub fn inv(
        subscription_ids: Vec<SubscriptionId>,
        event_id: EventId,
        event_kind: u16,
        payload_bytes: u32,
        hop_limit: u8,
    ) -> Self {
        Self::Inv {
            subscription_ids,
            event_id,
            event_kind,
            payload_bytes,
            hop_limit,
        }
    }

    #[must_use]
    pub fn want(event_id: EventId) -> Self {
        Self::Want { event_id }
    }
}

/// Encodes and decodes one transport-provided FIPS pubsub payload frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FipsPubsubWireCodec {
    max_frame_bytes: usize,
}

impl Default for FipsPubsubWireCodec {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_FIPS_PUBSUB_MAX_FRAME_BYTES,
        }
    }
}

impl FipsPubsubWireCodec {
    pub fn new(max_frame_bytes: usize) -> Result<Self> {
        if max_frame_bytes == 0 {
            return Err(PubsubError::Validation(
                "FIPS pubsub max frame bytes must be greater than zero".to_string(),
            ));
        }
        Ok(Self { max_frame_bytes })
    }

    #[must_use]
    pub const fn max_frame_bytes(self) -> usize {
        self.max_frame_bytes
    }

    pub fn encode_frame(&self, message: &FipsPubsubWireMessage) -> Result<Vec<u8>> {
        let json = match message {
            FipsPubsubWireMessage::Req {
                subscription_id,
                filters,
            } => ClientMessage::req(subscription_id.clone(), filters.clone()).as_json(),
            FipsPubsubWireMessage::Close { subscription_id } => {
                ClientMessage::close(subscription_id.clone()).as_json()
            }
            FipsPubsubWireMessage::Event {
                subscription_id: None,
                event,
            } => ClientMessage::event(event.as_event().clone()).as_json(),
            FipsPubsubWireMessage::Event {
                subscription_id: Some(subscription_id),
                event,
            } => RelayMessage::event(subscription_id.clone(), event.as_event().clone()).as_json(),
            FipsPubsubWireMessage::Inv {
                subscription_ids,
                event_id,
                event_kind,
                payload_bytes,
                hop_limit,
            } => json!([
                "INV",
                subscription_ids
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>(),
                event_id.to_hex(),
                event_kind,
                payload_bytes,
                hop_limit,
            ])
            .to_string(),
            FipsPubsubWireMessage::Want { event_id } => {
                json!(["WANT", event_id.to_hex()]).to_string()
            }
        };
        let frame = json.into_bytes();
        self.check_frame_size(frame.len())?;
        Ok(frame)
    }

    pub fn decode_frame(&self, frame: &[u8]) -> Result<FipsPubsubWireMessage> {
        self.check_frame_size(frame.len())?;
        if frame.is_empty() {
            return Err(invalid_frame("frame is empty"));
        }

        let value: Value = serde_json::from_slice(frame)
            .map_err(|error| invalid_frame(format!("invalid JSON: {error}")))?;
        let (message_type, field_count) = {
            let fields = value
                .as_array()
                .ok_or_else(|| invalid_frame("message must be a JSON array"))?;
            let message_type = fields
                .first()
                .and_then(Value::as_str)
                .ok_or_else(|| invalid_frame("message type must be a string"))?;
            (message_type.to_string(), fields.len())
        };

        match (message_type.as_str(), field_count) {
            ("REQ", 3..) => decode_req(value),
            ("CLOSE", 2) => decode_close(value),
            ("EVENT", 2) => decode_published_event(value),
            ("EVENT", 3) => decode_delivered_event(value),
            ("INV", 6) => decode_inv(&value),
            ("WANT", 2) => decode_want(&value),
            ("REQ", _) => Err(invalid_frame("REQ requires an id and at least one filter")),
            ("CLOSE", _) => Err(invalid_frame("CLOSE requires exactly an id")),
            ("EVENT", _) => Err(invalid_frame(
                "EVENT requires an event and optional subscription id",
            )),
            ("INV", _) => Err(invalid_frame(
                "INV requires subscription ids, event id, kind, byte size, and hop limit",
            )),
            ("WANT", _) => Err(invalid_frame("WANT requires exactly an event id")),
            (other, _) => Err(invalid_frame(format!(
                "unsupported Nostr message type {other}"
            ))),
        }
    }

    fn check_frame_size(self, frame_bytes: usize) -> Result<()> {
        if frame_bytes > self.max_frame_bytes {
            return Err(invalid_frame(format!(
                "frame has {frame_bytes} bytes, limit is {}",
                self.max_frame_bytes
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FipsPubsubInbound {
    pub message: FipsPubsubWireMessage,
    pub subscription_update: PubsubSubscriptionUpdate,
}

/// Bridges transport frames to verified events and bounded peer subscriptions.
#[derive(Debug, Clone)]
pub struct FipsPubsubWireAdapter {
    codec: FipsPubsubWireCodec,
    subscriptions: PubsubPeerSubscriptionStore,
}

impl Default for FipsPubsubWireAdapter {
    fn default() -> Self {
        Self::new(
            FipsPubsubWireCodec::default(),
            PubsubPeerSubscriptionStore::default(),
        )
    }
}

impl FipsPubsubWireAdapter {
    #[must_use]
    pub fn new(codec: FipsPubsubWireCodec, subscriptions: PubsubPeerSubscriptionStore) -> Self {
        Self {
            codec,
            subscriptions,
        }
    }

    #[must_use]
    pub const fn codec(&self) -> FipsPubsubWireCodec {
        self.codec
    }

    #[must_use]
    pub fn subscriptions(&self) -> &PubsubPeerSubscriptionStore {
        &self.subscriptions
    }

    /// Drop every subscription retained for a transport peer that disconnected.
    ///
    /// Transport integrations should call this after a connection is
    /// permanently removed so stale filters cannot consume routing state.
    pub fn disconnect_peer(&mut self, peer_id: &SourceId) -> Vec<PubsubPeerSubscription> {
        self.subscriptions.remove_peer(peer_id)
    }

    pub fn decode_inbound(&mut self, peer_id: SourceId, frame: &[u8]) -> Result<FipsPubsubInbound> {
        let message = self.codec.decode_frame(frame)?;
        let subscription_update = match &message {
            FipsPubsubWireMessage::Req {
                subscription_id,
                filters,
            } => {
                self.subscriptions.upsert_filters(
                    peer_id,
                    subscription_id.to_string(),
                    filters.clone(),
                )?;
                PubsubSubscriptionUpdate::Subscribed
            }
            FipsPubsubWireMessage::Close { subscription_id } => {
                self.subscriptions
                    .remove(&peer_id, &subscription_id.to_string());
                PubsubSubscriptionUpdate::Closed
            }
            FipsPubsubWireMessage::Event { .. }
            | FipsPubsubWireMessage::Inv { .. }
            | FipsPubsubWireMessage::Want { .. } => PubsubSubscriptionUpdate::Ignored,
        };
        Ok(FipsPubsubInbound {
            message,
            subscription_update,
        })
    }

    pub fn encode_outbound(&self, message: &FipsPubsubWireMessage) -> Result<Vec<u8>> {
        self.codec.encode_frame(message)
    }
}

fn decode_req(value: Value) -> Result<FipsPubsubWireMessage> {
    let message = ClientMessage::from_value(value).map_err(|error| message_error(&error))?;
    let ClientMessage::Req {
        subscription_id,
        filters,
    } = message
    else {
        return Err(invalid_frame("expected REQ message"));
    };
    Ok(FipsPubsubWireMessage::req(
        subscription_id.into_owned(),
        filters
            .into_iter()
            .map(std::borrow::Cow::into_owned)
            .collect(),
    ))
}

fn decode_close(value: Value) -> Result<FipsPubsubWireMessage> {
    let message = ClientMessage::from_value(value).map_err(|error| message_error(&error))?;
    let ClientMessage::Close(subscription_id) = message else {
        return Err(invalid_frame("expected CLOSE message"));
    };
    Ok(FipsPubsubWireMessage::close(subscription_id.into_owned()))
}

fn decode_published_event(value: Value) -> Result<FipsPubsubWireMessage> {
    let message = ClientMessage::from_value(value).map_err(|error| message_error(&error))?;
    let ClientMessage::Event(event) = message else {
        return Err(invalid_frame("expected client EVENT message"));
    };
    Ok(FipsPubsubWireMessage::publish(VerifiedEvent::try_from(
        event.into_owned(),
    )?))
}

fn decode_delivered_event(value: Value) -> Result<FipsPubsubWireMessage> {
    let message = RelayMessage::from_value(value).map_err(|error| message_error(&error))?;
    let RelayMessage::Event {
        subscription_id,
        event,
    } = message
    else {
        return Err(invalid_frame("expected subscription EVENT message"));
    };
    Ok(FipsPubsubWireMessage::deliver(
        subscription_id.into_owned(),
        VerifiedEvent::try_from(event.into_owned())?,
    ))
}

fn decode_inv(value: &Value) -> Result<FipsPubsubWireMessage> {
    let fields = value
        .as_array()
        .ok_or_else(|| invalid_frame("INV must be a JSON array"))?;
    let subscription_ids = decode_subscription_ids(fields, 1)?;
    let event_id = decode_event_id(fields, 2, "INV")?;
    let event_kind = decode_unsigned::<u16>(fields, 3, "INV event kind")?;
    let payload_bytes = decode_unsigned::<u32>(fields, 4, "INV payload bytes")?;
    let hop_limit = decode_unsigned::<u8>(fields, 5, "INV hop limit")?;
    if hop_limit == 0 {
        return Err(invalid_frame("INV hop limit must be greater than zero"));
    }
    Ok(FipsPubsubWireMessage::inv(
        subscription_ids,
        event_id,
        event_kind,
        payload_bytes,
        hop_limit,
    ))
}

fn decode_subscription_ids(fields: &[Value], index: usize) -> Result<Vec<SubscriptionId>> {
    let values = fields
        .get(index)
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_frame("INV subscription ids must be an array"))?;
    if values.is_empty() {
        return Err(invalid_frame("INV requires at least one subscription id"));
    }
    let mut seen = HashSet::with_capacity(values.len());
    values
        .iter()
        .map(|value| {
            let id = value
                .as_str()
                .ok_or_else(|| invalid_frame("INV subscription id must be a string"))?;
            if !seen.insert(id) {
                return Err(invalid_frame("INV subscription ids must be unique"));
            }
            Ok(SubscriptionId::new(id))
        })
        .collect()
}

fn decode_want(value: &Value) -> Result<FipsPubsubWireMessage> {
    let fields = value
        .as_array()
        .ok_or_else(|| invalid_frame("WANT must be a JSON array"))?;
    Ok(FipsPubsubWireMessage::want(decode_event_id(
        fields, 1, "WANT",
    )?))
}

fn decode_event_id(fields: &[Value], index: usize, message_type: &str) -> Result<EventId> {
    let value = fields
        .get(index)
        .and_then(Value::as_str)
        .ok_or_else(|| invalid_frame(format!("{message_type} event id must be a string")))?;
    EventId::from_hex(value)
        .map_err(|error| invalid_frame(format!("{message_type} event id is invalid: {error}")))
}

fn decode_unsigned<T>(fields: &[Value], index: usize, name: &str) -> Result<T>
where
    T: TryFrom<u64>,
{
    let value = fields
        .get(index)
        .and_then(Value::as_u64)
        .ok_or_else(|| invalid_frame(format!("{name} must be an unsigned integer")))?;
    T::try_from(value).map_err(|_| invalid_frame(format!("{name} is out of range")))
}

fn message_error(error: &nostr::message::MessageHandleError) -> PubsubError {
    invalid_frame(error.to_string())
}

fn invalid_frame(message: impl Into<String>) -> PubsubError {
    PubsubError::Validation(format!("invalid FIPS pubsub frame: {}", message.into()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subscription_scoped_inv_and_want_round_trip() {
        let codec = FipsPubsubWireCodec::default();
        let subscription_id = SubscriptionId::new("live-feed");
        let event_id = EventId::from_byte_array([7; 32]);
        let inv = FipsPubsubWireMessage::inv(
            vec![subscription_id.clone(), SubscriptionId::new("alerts")],
            event_id,
            1,
            4_096,
            4,
        );
        let want = FipsPubsubWireMessage::want(event_id);

        assert_eq!(
            codec
                .decode_frame(&codec.encode_frame(&inv).unwrap())
                .unwrap(),
            inv
        );
        assert_eq!(
            codec
                .decode_frame(&codec.encode_frame(&want).unwrap())
                .unwrap(),
            want
        );
    }

    #[test]
    fn inv_rejects_empty_subscriptions_zero_hops_and_noncanonical_event_ids() {
        let codec = FipsPubsubWireCodec::default();
        assert!(
            codec
                .decode_frame(
                    br#"["INV",["live"],"0707070707070707070707070707070707070707070707070707070707070707",1,10,0]"#,
                )
                .is_err()
        );
        assert!(
            codec
                .decode_frame(
                    br#"["INV",[],"0707070707070707070707070707070707070707070707070707070707070707",1,10,4]"#,
                )
                .is_err()
        );
        assert!(
            codec
                .decode_frame(br#"["WANT","not-an-event-id"]"#)
                .is_err()
        );
    }
}
