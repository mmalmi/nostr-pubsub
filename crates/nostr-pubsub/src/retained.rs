//! Bounded, verifiable snapshots for application-selected event storage.
//! Storage adapters own durability; this model owns Nostr retention semantics.
use crate::{
    EventRetentionPolicy, Filter, QueryOptions, VerifiedEvent, filter_matches_event,
    json_encoded_len,
};
use nostr::Event;
use std::cmp::Ordering;
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
    ///
    /// At most `max_events` candidate winners and 256 batch entries are retained
    /// while reading. The final snapshot byte limit is applied after all
    /// replacements, so a smaller replacement can free space for an older row.
    pub fn restore(&mut self, events: impl IntoIterator<Item = Event>, now: u64) {
        let batch_limit = self.policy.max_events.min(256);
        let mut accepted = Vec::new();
        for event in events {
            let Ok(event) = VerifiedEvent::try_from(event) else {
                continue;
            };
            let raw = event.as_event();
            if !self.policy.accepts(&event) || !retained_event_is_current(raw, now) {
                continue;
            }
            let Ok(encoded_len) = json_encoded_len(raw) else {
                continue;
            };
            if encoded_len <= MAX_RETAINED_EVENT_BYTES {
                accepted.push((event, encoded_len));
                if accepted.len() == batch_limit {
                    self.restore_batch(&mut accepted, now);
                }
            }
        }
        self.restore_batch(&mut accepted, now);
        // Delay byte eviction until all replacements have been considered: a
        // later smaller event can free room for an older unrelated event.
        self.enforce_bounds();
    }

    fn restore_batch(&mut self, accepted: &mut Vec<(VerifiedEvent, usize)>, now: u64) {
        if accepted.is_empty() {
            return;
        }
        self.events
            .retain(|(old, _)| retained_event_is_current(old.as_event(), now));
        self.events.append(accepted);
        self.events
            .sort_by(|(a, _), (b, _)| newest_first(a.as_event(), b.as_event()));
        let mut seen_ids = HashSet::with_capacity(self.events.len());
        let mut seen_coords = HashSet::new();
        self.events.retain(|(event, _)| {
            let raw = event.as_event();
            if !seen_ids.insert(raw.id) {
                return false;
            }
            event_coordinate(raw)
                .map(|(kind, pk, d)| (kind, pk, d.to_owned()))
                .is_none_or(|coord| seen_coords.insert(coord))
        });
        self.bytes = self.events.iter().map(|(_, size)| *size + 1).sum();
        // Keeping only the newest count-limited coordinate winners is safe
        // across batches. Candidates are bounded by max_events (at most 4096)
        // and the 64 KiB event limit, plus at most 256 incoming batch entries.
        while self.events.len() > self.policy.max_events {
            if let Some((_, size)) = self.events.pop() {
                self.bytes -= size + 1;
            }
        }
    }

    /// Whether `insert` would refuse the event before changing anything.
    pub(crate) fn rejects(&self, event: &VerifiedEvent, now: u64) -> bool {
        let raw = event.as_event();
        !self.policy.accepts(event)
            || !retained_event_is_current(raw, now)
            || self
                .events
                .iter()
                .any(|(old, _)| old.as_event().id == raw.id)
    }

    pub fn insert(&mut self, event: VerifiedEvent, now: u64) -> bool {
        if self.rejects(&event, now) {
            return false;
        }
        let raw = event.as_event();
        let id = raw.id;
        let Ok(encoded_len) = json_encoded_len(raw) else {
            return false;
        };
        if encoded_len > MAX_RETAINED_EVENT_BYTES {
            return false;
        }
        let coordinate = event_coordinate(raw);
        if coordinate.as_ref().is_some_and(|coordinate| {
            self.events.iter().any(|(old, _)| {
                event_coordinate(old.as_event()).as_ref() == Some(coordinate)
                    && !event_supersedes(raw, old.as_event())
            })
        }) {
            return false;
        }
        self.events.retain(|(old, _)| {
            retained_event_is_current(old.as_event(), now)
                && (coordinate.is_none() || event_coordinate(old.as_event()) != coordinate)
        });
        let index = self.events.partition_point(|(existing, _)| {
            newest_first(existing.as_event(), raw) == Ordering::Less
        });
        self.events.insert(index, (event, encoded_len));
        self.bytes = self.events.iter().map(|(_, size)| *size + 1).sum();
        self.enforce_bounds();
        self.events
            .iter()
            .any(|(stored, _)| stored.as_event().id == id)
    }

    fn enforce_bounds(&mut self) {
        // Reserve space for the snapshot envelope as well as commas.
        while self.events.len() > self.policy.max_events
            || self.bytes + 128 > MAX_RETAINED_SNAPSHOT_BYTES
        {
            if let Some((_, size)) = self.events.pop() {
                self.bytes -= size + 1;
            }
        }
    }

    #[must_use]
    pub fn snapshot(&self, now: u64) -> Vec<Event> {
        self.current(now).cloned().collect()
    }

    /// The events a snapshot taken at `now` contains, without copying them.
    pub(crate) fn current(&self, now: u64) -> impl Iterator<Item = &Event> {
        self.events
            .iter()
            .map(|(event, _)| event.as_event())
            .filter(move |event| retained_event_is_current(event, now))
    }

    #[must_use]
    pub fn query(&self, filters: &[Filter], options: QueryOptions, now: u64) -> Vec<VerifiedEvent> {
        let mut selected = HashSet::new();
        for filter in filters {
            let mut matched = 0;
            for (event, _) in &self.events {
                if filter.limit.is_some_and(|limit| matched >= limit) {
                    break;
                }
                if retained_event_is_current(event.as_event(), now)
                    && filter_matches_event(filter, event.as_event())
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

/// Query order: newest first, lower event ID first on timestamp ties.
pub(crate) fn newest_first(left: &Event, right: &Event) -> Ordering {
    right
        .created_at
        .cmp(&left.created_at)
        .then_with(|| left.id.cmp(&right.id))
}

fn event_coordinate(event: &Event) -> Option<(u16, nostr::PublicKey, &str)> {
    let kind = event.kind.as_u16();
    if matches!(kind, 0 | 3 | 10_000..=19_999) {
        Some((kind, event.pubkey, ""))
    } else if (30_000..40_000).contains(&kind) {
        Some((
            kind,
            event.pubkey,
            event.tags.identifier().unwrap_or_default(),
        ))
    } else {
        None
    }
}
