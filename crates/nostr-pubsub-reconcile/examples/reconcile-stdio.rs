//! Interoperability fixture: one JSON command/result per line, no networking.
use nostr_pubsub_reconcile::{Filter, Limits, Record, Session};
use serde_json::{Value, json};
use std::io::{self, BufRead};

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        write!(&mut out, "{byte:02x}").unwrap();
        out
    })
}
fn unhex(value: &Value) -> Result<Vec<u8>, String> {
    let text = value.as_str().ok_or("expected hex string")?;
    if text.len() % 2 != 0 {
        return Err("odd hex length".into());
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| {
            let pair = std::str::from_utf8(pair).map_err(|e| e.to_string())?;
            u8::from_str_radix(pair, 16).map_err(|e| e.to_string())
        })
        .collect()
}
fn number(value: &Value) -> Result<u64, String> {
    value
        .as_str()
        .ok_or("expected decimal string")?
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())
}
fn command(session: &mut Option<Session>, value: &Value) -> Result<Value, String> {
    match value["command"].as_str() {
        Some("new") => {
            let records = value["records"]
                .as_array()
                .ok_or("expected records")?
                .iter()
                .map(|r| {
                    Ok(Record {
                        timestamp: number(&r["timestamp"])?,
                        id: unhex(&r["id"])?.try_into().map_err(|_| "invalid ID")?,
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            *session = Some(Session::new(
                records,
                Filter {
                    since: number(&value["since"])?,
                    until: number(&value["until"])?,
                },
                Limits {
                    max_frame_bytes: usize::try_from(
                        value["frameBytes"].as_u64().unwrap_or(16_384),
                    )
                    .map_err(|_| "invalid frame limit")?,
                    ..Limits::default()
                },
            )?);
            Ok(json!({}))
        }
        Some("initiate") => {
            Ok(json!({"next": hex(&session.as_mut().ok_or("no session")?.initiate()?)}))
        }
        Some("respond") => Ok(
            json!({"next": hex(&session.as_mut().ok_or("no session")?.respond(&unhex(&value["frame"])?)?)}),
        ),
        Some("reconcile") => {
            let step = session
                .as_mut()
                .ok_or("no session")?
                .reconcile(&unhex(&value["frame"])?)?;
            Ok(
                json!({"next": step.next.map(|v| hex(&v)), "have": step.have.iter().map(|v| hex(v)).collect::<Vec<_>>(), "need": step.need.iter().map(|v| hex(v)).collect::<Vec<_>>() }),
            )
        }
        _ => Err("unknown command".into()),
    }
}
fn main() {
    let mut session = None;
    for line in io::stdin().lock().lines() {
        let result = line
            .map_err(|e| e.to_string())
            .and_then(|line| serde_json::from_str(&line).map_err(|e| e.to_string()))
            .and_then(|v| command(&mut session, &v));
        println!(
            "{}",
            match result {
                Ok(v) => json!({"ok": true, "result": v}),
                Err(error) => json!({"ok": false, "error": error}),
            }
        );
    }
}
