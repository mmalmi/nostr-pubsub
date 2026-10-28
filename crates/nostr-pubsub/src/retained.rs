//! Bounded, verifiable snapshots for application-selected event storage.
//! Storage adapters own durability; this model owns Nostr retention semantics.
use crate::{EventRetentionPolicy, Filter, MatchEventOptions, QueryOptions, VerifiedEvent};
use nostr::Event;
use std::collections::HashSet;

pub const MAX_RETAINED_EVENT_BYTES: usize = 64 * 1024;
pub const MAX_RETAINED_SNAPSHOT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_RETAINED_EVENTS: usize = 4096;

/// A local cache, never a source of peer authorization. Only explicitly selected
/// filters are retained. Cloning creates a candidate snapshot for atomic commits.
#[derive(Clone)]
pub struct RetainedEvents {
    policy: EventRetentionPolicy,
    events: Vec<(VerifiedEvent, usize)>,
    bytes: usize,
}

impl RetainedEvents {
    pub fn new(policy: EventRetentionPolicy) -> crate::Result<Self> {
        if policy.max_events == 0
            || policy.max_events > MAX_RETAINED_EVENTS
            || policy.filters.is_empty()
        {
            return Err(crate::PubsubError::Validation(
                "retention requires filters and 1..=4096 events".into(),
            ));
        }
        Ok(Self {
            policy,
            events: Vec::new(),
            bytes: 0,
        })
    }

    /// Recheck disk contents; neither a stored ID nor a signature flag is trusted.
    pub fn restore(&mut self, events: impl IntoIterator<Item = Event>, now: u64) {
        for event in events {
            if let Ok(event) = VerifiedEvent::try_from(event) {
                self.insert(event, now);
            }
        }
    }

    pub fn insert(&mut self, event: VerifiedEvent, now: u64) -> bool {
        let raw = event.as_event();
        if !self.policy.accepts(&event)
            || !retained_event_is_current(raw, now)
            || self
                .events
                .iter()
                .any(|(old, _)| old.as_event().id == raw.id)
        {
            return false;
        }
        let Ok(encoded) = serde_json::to_vec(raw) else {
            return false;
        };
        if encoded.len() > MAX_RETAINED_EVENT_BYTES {
            return false;
        }
        let coordinate = event_coordinate(raw);
        if let Some(coordinate) = &coordinate {
            if self.events.iter().any(|(old, _)| {
                event_coordinate(old.as_event()).as_ref() == Some(coordinate)
                    && !event_supersedes(raw, old.as_event())
            }) {
                return false;
            }
        }
        self.events.retain(|(old, _)| {
            retained_event_is_current(old.as_event(), now)
                && (coordinate.is_none() || event_coordinate(old.as_event()) != coordinate)
        });
        self.events.push((event.clone(), encoded.len()));
        self.events.sort_by(|(a, _), (b, _)| {
            b.as_event()
                .created_at
                .cmp(&a.as_event().created_at)
                .then_with(|| a.as_event().id.cmp(&b.as_event().id))
        });
        self.bytes = self.events.iter().map(|(_, size)| *size + 1).sum();
        // Reserve space for the snapshot envelope as well as commas.
        while self.events.len() > self.policy.max_events
            || self.bytes + 128 > MAX_RETAINED_SNAPSHOT_BYTES
        {
            if let Some((_, size)) = self.events.pop() {
                self.bytes -= size + 1;
            }
        }
        self.events
            .iter()
            .any(|(stored, _)| stored.as_event().id == event.as_event().id)
    }

    pub fn snapshot(&self, now: u64) -> Vec<Event> {
        self.events
            .iter()
            .filter(|(event, _)| retained_event_is_current(event.as_event(), now))
            .map(|(event, _)| event.as_event().clone())
            .collect()
    }

    pub fn query(&self, filters: &[Filter], options: QueryOptions, now: u64) -> Vec<VerifiedEvent> {
        let mut selected = HashSet::new();
        for filter in filters {
            let mut matched = 0;
            for (event, _) in &self.events {
                if filter.limit.is_some_and(|limit| matched >= limit) {
                    break;
                }
                if retained_event_is_current(event.as_event(), now)
                    && filter.match_event(event.as_event(), MatchEventOptions::new())
                {
                    selected.insert(event.as_event().id);
                    matched += 1;
                }
            }
        }
        self.events
            .iter()
            .filter(|(event, _)| selected.contains(&event.as_event().id))
            .take(options.limit.unwrap_or(self.policy.max_events))
            .map(|(event, _)| event.clone())
            .collect()
    }
}

/// NIP-01 ephemeral events are never durable. Reject future-dated cache poisoning
/// and honor NIP-40 on every read, including events that expired since startup.
pub fn retained_event_is_current(event: &Event, now: u64) -> bool {
    let expiration = event.tags.iter().find(|tag| {
        tag.as_slice()
            .first()
            .is_some_and(|value| value == "expiration")
    });
    let unexpired = expiration.is_none_or(|tag| {
        tag.as_slice()
            .get(1)
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .and_then(|value| value.parse::<u64>().ok())
            .is_some_and(|expires| expires > now)
    });
    !(20_000..30_000).contains(&event.kind.as_u16())
        && event.created_at.as_secs() <= now.saturating_add(300)
        && unexpired
}

/// NIP-01 replacement order, including the lower event ID on timestamp ties.
pub fn event_supersedes(candidate: &Event, previous: &Event) -> bool {
    candidate.created_at > previous.created_at
        || (candidate.created_at == previous.created_at && candidate.id < previous.id)
}

fn event_coordinate(event: &Event) -> Option<(u16, nostr::PublicKey, String)> {
    let kind = event.kind.as_u16();
    if matches!(kind, 0 | 3 | 10_000..=19_999) {
        Some((kind, event.pubkey, String::new()))
    } else if (30_000..40_000).contains(&kind) {
        Some((
            kind,
            event.pubkey,
            event.tags.identifier().unwrap_or_default().to_owned(),
        ))
    } else {
        None
    }
}
