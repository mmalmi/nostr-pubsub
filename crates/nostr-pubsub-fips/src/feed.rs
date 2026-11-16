//! Public peer feeds with bounded Negentropy catch-up, wire-compatible with the
//! TypeScript client.
//!
//! A feed subscription is an ordinary `REQ` with one public filter whose id is
//! `wf1-ts-N` when the subscriber already holds events of that feed, or
//! `cf1-ts-N` when it holds none. A provider answering a warm request opens a
//! `FEED-SYNC/1` session over the events it can offer; the subscriber answers
//! from its feed window, and the provider then announces only the events the
//! subscriber lacks through ordinary `INV`/`WANT`. Cold requests, declined or
//! failed sessions receive the latest few events instead.

use std::collections::{HashMap, VecDeque};

use nostr::{Alphabet, EventId, Filter, Kind, SingleLetterTag};
use nostr_pubsub::{PubsubError, PubsubPeerInterest, Result, VerifiedEvent};
use serde_json::{Value, json};

mod sync;
pub use sync::{FeedSync, FeedSyncFinish, FeedSyncOutput};

/// First element of a feed synchronization frame.
pub const FEED_SYNC_FRAME_TYPE: &str = "FEED-SYNC/1";
/// Events one feed window keeps, and the most one session reconciles.
pub const FEED_WINDOW_LIMIT: usize = 256;
const MAX_SYNC_PAYLOAD_HEX: usize = 16_384;
const MAX_FEED_KINDS: usize = 64;
const MAX_FEED_AUTHORS: usize = 256;
const NOTIFICATION_KINDS: [u16; 8] = [1, 6, 7, 16, 20, 1111, 9735, 30023];

/// One step of a feed synchronization session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FeedSyncAction {
    /// Provider to subscriber: the first reconciliation frame.
    Open,
    /// Provider to subscriber: a further reconciliation frame.
    Step,
    /// Subscriber to provider: the answer to `open` or `step`.
    Reply,
    /// Provider to subscriber: the session is over.
    End,
    /// Subscriber to provider: the session cannot run.
    Decline,
}

impl FeedSyncAction {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Open => "open",
            Self::Step => "step",
            Self::Reply => "reply",
            Self::End => "end",
            Self::Decline => "decline",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "open" => Self::Open,
            "step" => Self::Step,
            "reply" => Self::Reply,
            "end" => Self::End,
            "decline" => Self::Decline,
            _ => return None,
        })
    }

    const fn carries_payload(self) -> bool {
        !matches!(self, Self::End | Self::Decline)
    }
}

/// `["FEED-SYNC/1", action, subscription id, hex payload]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedSyncFrame {
    pub action: FeedSyncAction,
    pub subscription_id: String,
    pub payload: Vec<u8>,
}

impl FeedSyncFrame {
    #[must_use]
    pub fn new(action: FeedSyncAction, subscription_id: &str, payload: Vec<u8>) -> Self {
        Self {
            action,
            subscription_id: subscription_id.to_string(),
            payload,
        }
    }

    /// Whether a transport frame is a feed synchronization frame, without
    /// parsing the rest of it.
    #[must_use]
    pub fn is_feed_sync(frame: &[u8]) -> bool {
        let rest = frame.trim_ascii_start();
        let Some(rest) = rest.strip_prefix(b"[") else {
            return false;
        };
        rest.trim_ascii_start()
            .strip_prefix(b"\"")
            .and_then(|rest| rest.strip_prefix(FEED_SYNC_FRAME_TYPE.as_bytes()))
            .is_some_and(|rest| rest.starts_with(b"\""))
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let frame = json!([
            FEED_SYNC_FRAME_TYPE,
            self.action.as_str(),
            self.subscription_id,
            hex_encode(&self.payload),
        ]);
        let bytes = frame.to_string().into_bytes();
        Self::decode(&bytes)?;
        Ok(bytes)
    }

    pub fn decode(frame: &[u8]) -> Result<Self> {
        let invalid = || PubsubError::Validation("Invalid public feed sync frame".into());
        let value: Value = serde_json::from_slice(frame).map_err(|_| invalid())?;
        let [kind, action, id, payload] =
            value.as_array().map(Vec::as_slice).ok_or_else(invalid)?
        else {
            return Err(invalid());
        };
        let action = action.as_str().and_then(FeedSyncAction::parse);
        let (Some(FEED_SYNC_FRAME_TYPE), Some(action), Some(id), Some(payload)) =
            (kind.as_str(), action, id.as_str(), payload.as_str())
        else {
            return Err(invalid());
        };
        if !is_warm_feed_id(id) {
            return Err(invalid());
        }
        let payload = if action.carries_payload() {
            if payload.is_empty() || payload.len() > MAX_SYNC_PAYLOAD_HEX {
                return Err(invalid());
            }
            hex_decode(payload).ok_or_else(invalid)?
        } else if payload.is_empty() {
            Vec::new()
        } else {
            return Err(invalid());
        };
        Ok(Self {
            action,
            subscription_id: id.to_string(),
            payload,
        })
    }
}

/// `wf1-ts-N` or `cf1-ts-N`, as the TypeScript client names feed requests.
#[must_use]
pub fn is_public_feed_id(id: &str) -> bool {
    matches!(id.as_bytes().first(), Some(b'c' | b'w')) && is_feed_id_tail(&id[1..])
}

/// A feed request from a subscriber that already holds events of the feed.
#[must_use]
pub fn is_warm_feed_id(id: &str) -> bool {
    id.starts_with('w') && is_feed_id_tail(&id[1..])
}

fn is_feed_id_tail(tail: &str) -> bool {
    tail.strip_prefix("f1-ts-").is_some_and(|sequence| {
        (1..=12).contains(&sequence.len())
            && sequence
                .bytes()
                .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
    })
}

/// The local subscription a feed request id belongs to.
#[must_use]
pub fn feed_subscription_key(wire_id: &str) -> Option<&str> {
    is_public_feed_id(wire_id).then(|| &wire_id[4..])
}

/// The request id for local feed subscription `key` (`ts-N`).
#[must_use]
pub fn feed_wire_id(key: &str, warm: bool) -> String {
    format!("{}f1-{key}", if warm { 'w' } else { 'c' })
}

/// Private messages, encrypted envelopes and group rosters never travel in a
/// public peer feed.
#[must_use]
pub fn private_peer_event(event: &VerifiedEvent) -> bool {
    let event = event.as_event();
    private_kind(event.kind.as_u16())
        || (event.kind == Kind::Custom(37368)
            && event.tags.iter().any(|tag| {
                let tag = tag.as_slice();
                tag.first().map(String::as_str) == Some("type")
                    && tag.get(1).map(String::as_str) == Some("group_roster")
            }))
}

const fn private_kind(kind: u16) -> bool {
    matches!(kind, 4 | 13 | 14 | 1059 | 1060 | 21111)
}

/// The filters a feed may carry: public kinds only, optionally narrowed to at
/// most 256 authors, or to one recipient of public notification kinds.
#[must_use]
pub fn valid_public_feed_filter(filter: &Filter) -> bool {
    let Some(kinds) = filter.kinds.as_ref() else {
        return false;
    };
    if kinds.is_empty()
        || kinds.len() > MAX_FEED_KINDS
        || kinds.iter().any(|kind| private_kind(kind.as_u16()))
        || filter.ids.is_some()
        || filter.search.is_some()
        || filter.since.is_some()
        || filter.until.is_some()
        || filter.limit.is_some()
    {
        return false;
    }
    match (&filter.authors, filter.generic_tags.len()) {
        (None, 0) => true,
        (Some(authors), 0) => (1..=MAX_FEED_AUTHORS).contains(&authors.len()),
        (None, 1) => filter.generic_tags.iter().all(|(tag, values)| {
            *tag == SingleLetterTag::lowercase(Alphabet::P)
                && values.len() == 1
                && kinds
                    .iter()
                    .all(|kind| NOTIFICATION_KINDS.contains(&kind.as_u16()))
        }),
        _ => false,
    }
}

/// The bounded events one local feed subscription has seen: what it published
/// and what was delivered to it. Never seeded from a general event cache.
#[derive(Debug, Clone)]
pub struct FeedWindow {
    filter: Filter,
    limit: usize,
    order: VecDeque<EventId>,
    records: HashMap<EventId, VerifiedEvent>,
}

impl FeedWindow {
    #[must_use]
    pub fn new(filter: Filter, limit: usize) -> Self {
        Self {
            filter,
            limit,
            order: VecDeque::new(),
            records: HashMap::new(),
        }
    }

    #[must_use]
    pub fn filter(&self) -> &Filter {
        &self.filter
    }

    pub fn remember(&mut self, event: &VerifiedEvent) {
        if !self.accepts(event) {
            return;
        }
        let id = event.as_event().id;
        if self.records.insert(id, event.clone()).is_none() {
            self.order.push_back(id);
        }
        while self.order.len() > self.limit {
            if let Some(oldest) = self.order.pop_front() {
                self.records.remove(&oldest);
            }
        }
    }

    #[must_use]
    pub fn accepts(&self, event: &VerifiedEvent) -> bool {
        !private_peer_event(event)
            && !event.as_event().is_expired()
            && PubsubPeerInterest::from_filters(std::slice::from_ref(&self.filter), event)
                == PubsubPeerInterest::Subscribed
    }

    #[must_use]
    pub fn get(&self, id: &EventId) -> Option<&VerifiedEvent> {
        self.records.get(id)
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.snapshot().is_empty()
    }

    /// Held events, oldest first.
    #[must_use]
    pub fn snapshot(&self) -> Vec<VerifiedEvent> {
        self.order
            .iter()
            .filter_map(|id| self.records.get(id))
            .filter(|event| self.accepts(event))
            .cloned()
            .collect()
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::new(), |mut out, byte| {
        let _ = write!(out, "{byte:02x}");
        out
    })
}

fn hex_decode(text: &str) -> Option<Vec<u8>> {
    let bytes = text.as_bytes();
    if !bytes.len().is_multiple_of(2)
        || !bytes
            .iter()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(byte))
    {
        return None;
    }
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok())
        .collect()
}

#[cfg(test)]
mod tests;
