//! Test-only process boundary for Rust/TypeScript public feed synchronization.
//! One JSON command and one JSON result per line, frames as hex.

use std::collections::HashSet;
use std::io::{self, BufRead, Write};

use nostr::{Event, Filter, JsonUtil};
use nostr_pubsub::{FipsPubsubWireCodec, FipsPubsubWireMessage, SubscriptionId, VerifiedEvent};
use nostr_pubsub_fips::FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES;
use nostr_pubsub_fips::feed::{
    FeedSync, FeedSyncFrame, FeedSyncOutput, feed_wire_id, valid_public_feed_filter,
};
use serde_json::{Value, json};

struct Fixture {
    sync: FeedSync,
    opened: HashSet<(String, String)>,
}

fn events(value: &Value) -> Result<Vec<VerifiedEvent>, String> {
    value
        .as_array()
        .ok_or("expected events")?
        .iter()
        .map(|event| {
            let event = Event::from_json(event.to_string()).map_err(|e| e.to_string())?;
            VerifiedEvent::try_from(event).map_err(|e| e.to_string())
        })
        .collect()
}

fn text<'a>(value: &'a Value, field: &str) -> Result<&'a str, String> {
    value[field]
        .as_str()
        .ok_or_else(|| format!("expected {field}"))
}

fn output(output: FeedSyncOutput) -> Result<Value, String> {
    let frames = output
        .frames
        .iter()
        .map(|frame| frame.encode().map(hex::encode).map_err(|e| e.to_string()))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(json!({
        "frames": frames,
        "finish": output.finish.map(|finish| json!({
            "events": finish.events.iter().map(|event| event.as_event().id.to_hex()).collect::<Vec<_>>(),
            "fallback": finish.fallback,
        })),
    }))
}

fn command(fixture: &mut Option<Fixture>, value: &Value) -> Result<Value, String> {
    let now = value["now"].as_u64().unwrap_or(0);
    match value["command"].as_str() {
        Some("new") => {
            let max_frame_bytes = value["maxFrameBytes"]
                .as_u64()
                .map_or(Ok(FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES), usize::try_from)
                .map_err(|e| e.to_string())?;
            let max_sessions = value["maxSessions"]
                .as_u64()
                .map_or(Ok(64), usize::try_from)
                .map_err(|e| e.to_string())?;
            *fixture = Some(Fixture {
                sync: FeedSync::new(max_frame_bytes, max_sessions),
                opened: HashSet::new(),
            });
            Ok(json!({}))
        }
        Some("offer") => {
            let fixture = fixture.as_mut().ok_or("no fixture")?;
            let (peer, id) = (text(value, "peer")?, text(value, "id")?);
            output(
                fixture
                    .sync
                    .offer(peer, id, None, events(&value["events"])?, now),
            )
        }
        Some("receive") => {
            let fixture = fixture.as_mut().ok_or("no fixture")?;
            let peer = text(value, "peer")?;
            let bytes = hex::decode(text(value, "frame")?).map_err(|e| e.to_string())?;
            let frame = FeedSyncFrame::decode(&bytes).map_err(|e| e.to_string())?;
            let local = match &value["window"] {
                Value::Null => None,
                window => Some((None, events(window)?)),
            };
            let key = (peer.to_string(), frame.subscription_id.clone());
            let opened = &mut fixture.opened;
            output(
                fixture
                    .sync
                    .receive(peer, &frame, local, || opened.insert(key), now),
            )
        }
        Some("expire") => {
            let fixture = fixture.as_mut().ok_or("no fixture")?;
            let outputs = fixture.sync.expire(now);
            Ok(Value::Array(
                outputs.into_iter().map(output).collect::<Result<_, _>>()?,
            ))
        }
        Some("request") => {
            let filter: Filter =
                serde_json::from_value(value["filter"].clone()).map_err(|e| e.to_string())?;
            let id = feed_wire_id(text(value, "key")?, value["warm"].as_bool() == Some(true));
            let codec = FipsPubsubWireCodec::new(FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES)
                .map_err(|e| e.to_string())?;
            let frame = codec
                .encode_frame(&FipsPubsubWireMessage::req(
                    SubscriptionId::new(id),
                    vec![filter.clone()],
                ))
                .map_err(|e| e.to_string())?;
            Ok(json!({ "frame": hex::encode(frame), "valid": valid_public_feed_filter(&filter) }))
        }
        _ => Err("unknown command".into()),
    }
}

fn main() {
    let mut fixture = None;
    let mut stdout = io::stdout().lock();
    for line in io::stdin().lock().lines() {
        let result = line
            .map_err(|e| e.to_string())
            .and_then(|line| serde_json::from_str(&line).map_err(|e| e.to_string()))
            .and_then(|value| command(&mut fixture, &value));
        let response = match result {
            Ok(result) => json!({"ok": true, "result": result}),
            Err(error) => json!({"ok": false, "error": error}),
        };
        writeln!(stdout, "{response}").expect("write fixture response");
        stdout.flush().expect("flush fixture response");
    }
}
