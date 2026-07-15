use nostr::JsonUtil;
use serde_json::Value;

use crate::{
    ClientMessage, Filter, PubsubError, PubsubPeerSubscription, PubsubPeerSubscriptionStore,
    PubsubSubscriptionUpdate, RelayMessage, Result, SourceId, SubscriptionId, VerifiedEvent,
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
            ("REQ", _) => Err(invalid_frame("REQ requires an id and at least one filter")),
            ("CLOSE", _) => Err(invalid_frame("CLOSE requires exactly an id")),
            ("EVENT", _) => Err(invalid_frame(
                "EVENT requires an event and optional subscription id",
            )),
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
            FipsPubsubWireMessage::Event { .. } => PubsubSubscriptionUpdate::Ignored,
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

fn message_error(error: &nostr::message::MessageHandleError) -> PubsubError {
    invalid_frame(error.to_string())
}

fn invalid_frame(message: impl Into<String>) -> PubsubError {
    PubsubError::Validation(format!("invalid FIPS pubsub frame: {}", message.into()))
}
