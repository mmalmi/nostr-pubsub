//! Bounded Negentropy sessions for public peer feeds. The provider starts, so
//! only its own verified events the subscriber lacks are announced, through
//! ordinary `INV`/`WANT`. Time is passed in, so the state machine is
//! deterministic.

use std::collections::{HashMap, HashSet};

use nostr::EventId;
use nostr_pubsub::VerifiedEvent;
use nostr_pubsub_reconcile::{Filter as History, Limits, Record, Session};

use super::{FEED_WINDOW_LIMIT, FeedSyncAction, FeedSyncFrame};

const SESSION_TIMEOUT_MS: u64 = 2_500;
const MAX_SESSIONS: usize = 16;
const MAX_SESSIONS_PER_PEER: usize = 4;
const MAX_ROUNDS: usize = 16;
const MIN_FRAME_BYTES: usize = 4_096;
const MAX_FRAME_BYTES: usize = 8_192;
const FRAME_OVERHEAD_BYTES: usize = 96;
const HISTORY: History = History {
    since: 0,
    until: (1 << 63) - 1,
};

type Key = (String, String);

struct State {
    engine: Session,
    deadline_ms: u64,
    snapshot: Vec<VerifiedEvent>,
    positions: HashMap<EventId, usize>,
    missing: Vec<usize>,
    missing_seen: HashSet<usize>,
    owner: Option<String>,
}

impl State {
    fn events(&self, positions: &[usize]) -> Vec<VerifiedEvent> {
        positions
            .iter()
            .map(|position| self.snapshot[*position].clone())
            .collect()
    }
}

/// Inventory a provider announces once its session ends.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedSyncFinish {
    pub subscription_id: String,
    pub events: Vec<VerifiedEvent>,
    /// The session did not run to completion; announce only the latest few.
    pub fallback: bool,
}

/// Frames for one peer, and on the provider what to announce to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedSyncOutput {
    pub peer: String,
    pub frames: Vec<FeedSyncFrame>,
    pub finish: Option<FeedSyncFinish>,
}

impl FeedSyncOutput {
    fn new(peer: &str) -> Self {
        Self {
            peer: peer.to_string(),
            frames: Vec::new(),
            finish: None,
        }
    }

    fn frame(mut self, action: FeedSyncAction, id: &str, payload: Vec<u8>) -> Self {
        self.frames.push(FeedSyncFrame::new(action, id, payload));
        self
    }

    fn finish(mut self, id: &str, events: Vec<VerifiedEvent>, fallback: bool) -> Self {
        self.finish = Some(FeedSyncFinish {
            subscription_id: id.to_string(),
            events,
            fallback,
        });
        self
    }

    /// The provider ends a session it started, then announces.
    fn end(self, id: &str, events: Vec<VerifiedEvent>, fallback: bool) -> Self {
        self.frame(FeedSyncAction::End, id, Vec::new())
            .finish(id, events, fallback)
    }
}

/// Feed sessions this client provides (`offering`) and receives.
pub struct FeedSync {
    offering: HashMap<Key, State>,
    receiving: HashMap<Key, State>,
    frame_bytes: usize,
    max_sessions: usize,
}

impl FeedSync {
    #[must_use]
    pub fn new(max_frame_bytes: usize, max_sessions: usize) -> Self {
        Self {
            offering: HashMap::new(),
            receiving: HashMap::new(),
            frame_bytes: MAX_FRAME_BYTES
                .min(max_frame_bytes.saturating_sub(FRAME_OVERHEAD_BYTES) / 2),
            max_sessions: MAX_SESSIONS.min(max_sessions),
        }
    }

    #[must_use]
    pub fn session_count(&self) -> usize {
        self.offering.len() + self.receiving.len()
    }

    /// Answer a warm feed request with the events this provider can offer.
    pub fn offer(
        &mut self,
        peer: &str,
        id: &str,
        owner: Option<&str>,
        inventory: Vec<VerifiedEvent>,
        now_ms: u64,
    ) -> FeedSyncOutput {
        let output = FeedSyncOutput::new(peer);
        let key = (peer.to_string(), id.to_string());
        if !self.start(true, &key, owner, inventory.clone(), now_ms) {
            return output.finish(id, inventory, true);
        }
        let state = self.offering.get_mut(&key).expect("started session");
        if let Ok(opening) = state.engine.initiate_fingerprint_first() {
            return output.frame(FeedSyncAction::Open, id, opening);
        }
        self.offering.remove(&key);
        output.end(id, inventory, true)
    }

    /// Handle one frame. `local` is the subscriber's own feed for this request
    /// (its owner and window); `allow_open` admits one session per link.
    pub fn receive(
        &mut self,
        peer: &str,
        frame: &FeedSyncFrame,
        local: Option<(Option<&str>, Vec<VerifiedEvent>)>,
        allow_open: impl FnOnce() -> bool,
        now_ms: u64,
    ) -> FeedSyncOutput {
        let id = frame.subscription_id.as_str();
        let output = FeedSyncOutput::new(peer);
        let key = (peer.to_string(), id.to_string());
        let provider = matches!(
            frame.action,
            FeedSyncAction::Reply | FeedSyncAction::Decline
        );
        match frame.action {
            FeedSyncAction::End => {
                self.receiving.remove(&key);
                return output;
            }
            FeedSyncAction::Open => {
                let Some((owner, snapshot)) = local else {
                    return output;
                };
                if self.receiving.contains_key(&key) || !allow_open() {
                    return output;
                }
                if !self.start(false, &key, owner, snapshot, now_ms) {
                    return output.frame(FeedSyncAction::Decline, id, Vec::new());
                }
            }
            FeedSyncAction::Decline => {
                return match self.offering.remove(&key) {
                    Some(state) => output.end(id, state.snapshot, true),
                    None => output,
                };
            }
            FeedSyncAction::Step | FeedSyncAction::Reply => {}
        }
        let map = if provider {
            &mut self.offering
        } else {
            &mut self.receiving
        };
        let Some(state) = map.get_mut(&key) else {
            return output;
        };
        if !provider {
            if let Ok(reply) = state.engine.respond(&frame.payload) {
                return output.frame(FeedSyncAction::Reply, id, reply);
            }
            map.remove(&key);
            return output.frame(FeedSyncAction::Decline, id, Vec::new());
        }
        let Ok(step) = state.engine.reconcile(&frame.payload) else {
            let state = map.remove(&key).expect("session in map");
            return output.end(id, state.snapshot, true);
        };
        for have in step.have {
            let position = state.positions.get(&EventId::from_byte_array(have));
            if let Some(position) = position.copied()
                && state.missing_seen.insert(position)
            {
                state.missing.push(position);
            }
        }
        if let Some(next) = step.next {
            return output.frame(FeedSyncAction::Step, id, next);
        }
        let state = map.remove(&key).expect("session in map");
        let missing = state.events(&state.missing);
        output.end(id, missing, false)
    }

    /// End sessions past their deadline: the provider falls back to bounded
    /// replay and the subscriber declines.
    pub fn expire(&mut self, now_ms: u64) -> Vec<FeedSyncOutput> {
        let mut outputs = Vec::new();
        for (offering, map) in [(true, &mut self.offering), (false, &mut self.receiving)] {
            let due = map
                .iter()
                .filter(|(_, state)| state.deadline_ms <= now_ms)
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>();
            for (peer, id) in due {
                let Some(state) = map.remove(&(peer.clone(), id.clone())) else {
                    continue;
                };
                let output = FeedSyncOutput::new(&peer);
                outputs.push(if offering {
                    output.end(&id, state.snapshot, true)
                } else {
                    output.frame(FeedSyncAction::Decline, &id, Vec::new())
                });
            }
        }
        outputs
    }

    /// Drop matching sessions without sending anything.
    pub fn cancel(
        &mut self,
        peer: Option<&str>,
        id: Option<&str>,
        owner: Option<&str>,
        offering_only: bool,
    ) {
        let matches = |(session_peer, session_id): &Key, state: &State| {
            peer.is_none_or(|peer| peer == session_peer)
                && id.is_none_or(|id| id == session_id)
                && owner.is_none_or(|owner| state.owner.as_deref() == Some(owner))
        };
        self.offering.retain(|key, state| !matches(key, state));
        if !offering_only {
            self.receiving.retain(|key, state| !matches(key, state));
        }
    }

    fn start(
        &mut self,
        offering: bool,
        key: &Key,
        owner: Option<&str>,
        events: Vec<VerifiedEvent>,
        now_ms: u64,
    ) -> bool {
        let total = self.session_count();
        let map = if offering {
            &mut self.offering
        } else {
            &mut self.receiving
        };
        let per_peer = map.keys().filter(|(peer, _)| *peer == key.0).count();
        if self.frame_bytes < MIN_FRAME_BYTES
            || total >= self.max_sessions
            || map.contains_key(key)
            || per_peer >= MAX_SESSIONS_PER_PEER
        {
            return false;
        }
        let records = events.iter().map(|event| Record {
            id: event.as_event().id.to_bytes(),
            timestamp: event.as_event().created_at.as_secs(),
        });
        let limits = Limits {
            max_records: FEED_WINDOW_LIMIT,
            max_frame_bytes: self.frame_bytes,
            max_rounds: MAX_ROUNDS,
        };
        let Ok(engine) = Session::new(records, HISTORY, limits) else {
            return false;
        };
        let positions = events
            .iter()
            .enumerate()
            .map(|(position, event)| (event.as_event().id, position))
            .collect();
        map.insert(
            key.clone(),
            State {
                engine,
                deadline_ms: now_ms.saturating_add(SESSION_TIMEOUT_MS),
                snapshot: events,
                positions,
                missing: Vec::new(),
                missing_seen: HashSet::new(),
                owner: owner.map(str::to_string),
            },
        );
        true
    }
}
