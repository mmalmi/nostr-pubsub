use std::fmt;

use nostr::Event;
use serde::de::{self, Deserializer, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::{PubsubError, Result};

/// Network-neutral inventory/want/frame messages for signed Nostr events.
///
/// The containing transport chooses the protocol name, version and framing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InvWantWireMessage {
    Inventory {
        event_id: String,
        event_kind: u16,
        payload_bytes: u32,
        hop_limit: u8,
    },
    Want {
        event_id: String,
    },
    Frame {
        event_id: String,
        event: Box<Event>,
    },
}

impl<'de> Deserialize<'de> for InvWantWireMessage {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MessageVisitor;

        impl<'de> Visitor<'de> for MessageVisitor {
            type Value = InvWantWireMessage;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an inv/want wire message object")
            }

            fn visit_map<A>(self, mut map: A) -> std::result::Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut kind: Option<String> = None;
                let mut event_id: Option<String> = None;
                let mut event_kind: Option<u16> = None;
                let mut payload_bytes: Option<u32> = None;
                let mut hop_limit: Option<u8> = None;
                let mut event: Option<Box<Event>> = None;
                // The normal encoder puts `type` first. Buffer only variant
                // fields preceding it, because they may be irrelevant to the
                // selected variant and must then be ignored regardless of type.
                let mut pending = Vec::new();

                while let Some(key) = map.next_key::<String>()? {
                    match (kind.as_deref(), key.as_str()) {
                        (_, "type") => {
                            if kind.replace(map.next_value()?).is_some() {
                                return Err(de::Error::duplicate_field("type"));
                            }
                        }
                        (_, "event_id") => {
                            if event_id.replace(map.next_value()?).is_some() {
                                return Err(de::Error::duplicate_field("event_id"));
                            }
                        }
                        (None, "event_kind" | "payload_bytes" | "hop_limit" | "event") => {
                            pending.push((key, map.next_value::<Box<RawValue>>()?));
                        }
                        (Some("inventory"), "event_kind") => {
                            if event_kind.replace(map.next_value()?).is_some() {
                                return Err(de::Error::duplicate_field("event_kind"));
                            }
                        }
                        (Some("inventory"), "payload_bytes") => {
                            if payload_bytes.replace(map.next_value()?).is_some() {
                                return Err(de::Error::duplicate_field("payload_bytes"));
                            }
                        }
                        (Some("inventory"), "hop_limit") => {
                            if hop_limit.replace(map.next_value()?).is_some() {
                                return Err(de::Error::duplicate_field("hop_limit"));
                            }
                        }
                        (Some("frame"), "event") => {
                            if event.replace(map.next_value()?).is_some() {
                                return Err(de::Error::duplicate_field("event"));
                            }
                        }
                        _ => {
                            let _ = map.next_value::<IgnoredAny>()?;
                        }
                    }
                }

                let kind = kind.ok_or_else(|| de::Error::missing_field("type"))?;
                for (key, value) in pending {
                    match (kind.as_str(), key.as_str()) {
                        ("inventory", "event_kind") => {
                            read_pending(&mut event_kind, &value, "event_kind")?;
                        }
                        ("inventory", "payload_bytes") => {
                            read_pending(&mut payload_bytes, &value, "payload_bytes")?;
                        }
                        ("inventory", "hop_limit") => {
                            read_pending(&mut hop_limit, &value, "hop_limit")?;
                        }
                        ("frame", "event") => {
                            read_pending(&mut event, &value, "event")?;
                        }
                        _ => {}
                    }
                }

                match kind.as_str() {
                    "inventory" => Ok(InvWantWireMessage::Inventory {
                        event_id: event_id.ok_or_else(|| de::Error::missing_field("event_id"))?,
                        event_kind: event_kind
                            .ok_or_else(|| de::Error::missing_field("event_kind"))?,
                        payload_bytes: payload_bytes
                            .ok_or_else(|| de::Error::missing_field("payload_bytes"))?,
                        hop_limit: hop_limit
                            .ok_or_else(|| de::Error::missing_field("hop_limit"))?,
                    }),
                    "want" => Ok(InvWantWireMessage::Want {
                        event_id: event_id.ok_or_else(|| de::Error::missing_field("event_id"))?,
                    }),
                    "frame" => Ok(InvWantWireMessage::Frame {
                        event_id: event_id.ok_or_else(|| de::Error::missing_field("event_id"))?,
                        event: event.ok_or_else(|| de::Error::missing_field("event"))?,
                    }),
                    other => Err(de::Error::unknown_variant(
                        other,
                        &["inventory", "want", "frame"],
                    )),
                }
            }
        }

        deserializer.deserialize_map(MessageVisitor)
    }
}

fn read_pending<T: de::DeserializeOwned, E: de::Error>(
    slot: &mut Option<T>,
    value: &RawValue,
    field: &'static str,
) -> std::result::Result<(), E> {
    if slot.is_some() {
        return Err(E::duplicate_field(field));
    }
    *slot = Some(serde_json::from_str(value.get()).map_err(E::custom)?);
    Ok(())
}

#[derive(Debug, Serialize)]
struct InvWantEnvelopeRef<'a> {
    protocol: &'a str,
    version: u8,
    message: &'a InvWantWireMessage,
}

#[derive(Debug, Deserialize)]
struct InvWantEnvelope {
    protocol: String,
    version: u8,
    message: InvWantWireMessage,
}

/// A bounded JSON envelope codec. Transport adapters can preserve their own
/// deployed namespace and version while sharing the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvWantCodec {
    protocol: String,
    version: u8,
    max_wire_bytes: usize,
}

impl InvWantCodec {
    #[must_use]
    pub fn new(protocol: impl Into<String>, version: u8, max_wire_bytes: usize) -> Self {
        Self {
            protocol: protocol.into(),
            version,
            max_wire_bytes: max_wire_bytes.max(1),
        }
    }

    #[must_use]
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    #[must_use]
    pub const fn version(&self) -> u8 {
        self.version
    }

    pub fn encode(&self, message: &InvWantWireMessage) -> Result<Vec<u8>> {
        let encoded = serde_json::to_vec(&InvWantEnvelopeRef {
            protocol: &self.protocol,
            version: self.version,
            message,
        })
        .map_err(|error| validation(format!("invalid inv/want JSON: {error}")))?;
        self.check_wire_len(encoded.len())?;
        Ok(encoded)
    }

    pub fn decode(&self, payload: &[u8]) -> Result<InvWantWireMessage> {
        self.check_wire_len(payload.len())?;
        let envelope: InvWantEnvelope = serde_json::from_slice(payload)
            .map_err(|error| validation(format!("invalid inv/want JSON: {error}")))?;
        if envelope.protocol != self.protocol {
            return Err(validation(format!(
                "unsupported inv/want protocol {:?}",
                envelope.protocol
            )));
        }
        if envelope.version != self.version {
            return Err(validation(format!(
                "unsupported inv/want version {}",
                envelope.version
            )));
        }
        Ok(envelope.message)
    }

    fn check_wire_len(&self, len: usize) -> Result<()> {
        if len > self.max_wire_bytes {
            return Err(validation(format!(
                "inv/want wire payload is {len} bytes, maximum is {}",
                self.max_wire_bytes
            )));
        }
        Ok(())
    }
}

fn validation(message: impl Into<String>) -> PubsubError {
    PubsubError::Validation(message.into())
}

#[cfg(test)]
mod tests {
    use nostr::{EventBuilder, Keys};

    use super::*;

    #[test]
    fn irrelevant_variant_fields_do_not_depend_on_json_key_order() {
        // Serde's original tagged enum ignores fields belonging to another
        // variant. Moving the tag must not change the accepted wire messages.
        for json in [
            r#"{"type":"want","event_id":"abc","event":"extension"}"#,
            r#"{"event":"extension","event_id":"abc","type":"want"}"#,
            r#"{"event_kind":{},"payload_bytes":null,"hop_limit":[],"event_id":"abc","type":"want"}"#,
            r#"{"event_kind":1e999,"event_id":"abc","type":"want"}"#,
        ] {
            assert_eq!(
                serde_json::from_str::<InvWantWireMessage>(json).unwrap(),
                InvWantWireMessage::Want {
                    event_id: "abc".to_owned()
                },
                "{json}",
            );
        }
        for json in [
            r#"{"type":"frame","event_id":"abc","event":"invalid"}"#,
            r#"{"event":"invalid","event_id":"abc","type":"frame"}"#,
            r#"{"event_kind":{},"payload_bytes":1,"hop_limit":1,"event_id":"abc","type":"inventory"}"#,
            r#"{"event_kind":1,"event_kind":2,"payload_bytes":1,"hop_limit":1,"event_id":"abc","type":"inventory"}"#,
        ] {
            assert!(
                serde_json::from_str::<InvWantWireMessage>(json).is_err(),
                "{json}"
            );
        }
    }

    #[test]
    fn streaming_codec_round_trips_and_accepts_reordered_keys() {
        let codec = InvWantCodec::new("nostr.pubsub.test", 1, 64 * 1024);
        let keys = Keys::generate();
        let event = EventBuilder::text_note("codec-test")
            .sign_with_keys(&keys)
            .unwrap();
        let id = event.id.to_hex();

        for message in [
            InvWantWireMessage::Inventory {
                event_id: id.clone(),
                event_kind: 1,
                payload_bytes: 256,
                hop_limit: 8,
            },
            InvWantWireMessage::Want {
                event_id: id.clone(),
            },
            InvWantWireMessage::Frame {
                event_id: id.clone(),
                event: Box::new(event.clone()),
            },
        ] {
            let encoded = codec.encode(&message).unwrap();
            assert_eq!(codec.decode(&encoded).unwrap(), message);
        }

        let event_json = serde_json::to_string(&event).unwrap();
        let reordered = format!(
            r#"{{"protocol":"nostr.pubsub.test","version":1,"message":{{"event":{event_json},"event_id":"{id}","extra":true,"type":"frame"}}}}"#
        );
        assert_eq!(
            codec.decode(reordered.as_bytes()).unwrap(),
            InvWantWireMessage::Frame {
                event_id: id,
                event: Box::new(event),
            }
        );

        assert!(
            codec
                .decode(
                    br#"{"protocol":"nostr.pubsub.test","version":1,"message":{"type":"unknown"}}"#
                )
                .is_err()
        );
        assert!(
            codec
                .decode(
                    br#"{"protocol":"nostr.pubsub.test","version":1,"message":{"type":"want"}}"#
                )
                .is_err()
        );
    }
}
