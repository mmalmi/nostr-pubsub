//! Transport-independent set reconciliation. Callers own authentication, durable
//! storage, retention policy and record transfer. A completed round compares only
//! its immutable filtered snapshot; it does not acknowledge durable delivery.

use std::collections::BTreeMap;

use engine::Negentropy;
use negentropy::{Id, NegentropyStorageVector};
mod codec;
mod engine;

mod validation;

pub type Result<T> = std::result::Result<T, String>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
/// IDs must be cryptographic hashes of unique records (for example Nostr event IDs).
/// Sequential/padded counters do not provide the collision resistance required
/// by Negentropy’s additive fingerprints.
pub struct Record {
    pub timestamp: u64,
    pub id: [u8; 32],
}

/// Inclusive, agreed history window. Changing retention starts a new session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Filter {
    pub since: u64,
    pub until: u64,
}

impl Filter {
    #[must_use]
    pub fn contains(self, timestamp: u64) -> bool {
        timestamp >= self.since && timestamp <= self.until
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub max_records: usize,
    pub max_frame_bytes: usize,
    pub max_rounds: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_records: 100_000,
            max_frame_bytes: 16_384,
            max_rounds: 256,
        }
    }
}

#[derive(Debug)]
pub struct Step {
    pub next: Option<Vec<u8>>,
    /// Records present locally and absent remotely. The remote device still
    /// decides whether it has permission, space and interest to accept them.
    pub have: Vec<[u8; 32]>,
    pub need: Vec<[u8; 32]>,
}

/// An immutable snapshot and bounded Negentropy v1 conversation. No filesystem,
/// network, clock, runtime or OS APIs: the browser peer uses a wire-compatible TypeScript implementation.
pub struct Session {
    engine: Negentropy<'static, NegentropyStorageVector>,
    limits: Limits,
    rounds: usize,
    finished: bool,
}

impl Session {
    pub fn new(
        records: impl IntoIterator<Item = Record>,
        filter: Filter,
        limits: Limits,
    ) -> Result<Self> {
        if filter.since > filter.until || filter.until == u64::MAX {
            return Err("invalid reconciliation window".into());
        }
        if limits.max_records == 0
            || limits.max_records > 1_000_000
            || !(4096..=65_536).contains(&limits.max_frame_bytes)
            || limits.max_rounds == 0
        {
            return Err("invalid reconciliation limits".into());
        }
        let mut unique = BTreeMap::new();
        for record in records {
            if !filter.contains(record.timestamp) {
                continue;
            }
            if let Some(previous) = unique.insert(record.id, record.timestamp)
                && previous != record.timestamp
            {
                return Err("one record ID has conflicting timestamps".into());
            }
            if unique.len() > limits.max_records {
                return Err("reconciliation window exceeds record limit".into());
            }
        }
        let mut storage = NegentropyStorageVector::with_capacity(unique.len());
        for (id, timestamp) in unique {
            storage
                .insert(timestamp, Id::from_byte_array(id))
                .map_err(|e| e.to_string())?;
        }
        storage.seal().map_err(|e| e.to_string())?;
        let engine =
            Negentropy::owned(storage, limits.max_frame_bytes).map_err(|e| e.to_string())?;
        Ok(Self {
            engine,
            limits,
            rounds: 0,
            finished: false,
        })
    }

    pub fn initiate(&mut self) -> Result<Vec<u8>> {
        self.tick()?;
        self.engine.initiate().map_err(|e| e.to_string())
    }

    pub fn respond(&mut self, frame: &[u8]) -> Result<Vec<u8>> {
        self.accept(frame)?;
        self.engine.reconcile(frame).map_err(|e| e.to_string())
    }

    pub fn reconcile(&mut self, frame: &[u8]) -> Result<Step> {
        self.accept(frame)?;
        let (mut have, mut need) = (Vec::new(), Vec::new());
        let next = self
            .engine
            .reconcile_with_ids(frame, &mut have, &mut need)
            .map_err(|e| e.to_string())?;
        self.finished = next.is_none();
        Ok(Step {
            next,
            have: have.into_iter().map(|id| *id.as_bytes()).collect(),
            need: need.into_iter().map(|id| *id.as_bytes()).collect(),
        })
    }

    fn tick(&mut self) -> Result<()> {
        if self.finished || self.rounds >= self.limits.max_rounds {
            return Err("reconciliation session finished or round limit exceeded".into());
        }
        self.rounds += 1;
        Ok(())
    }

    fn accept(&mut self, frame: &[u8]) -> Result<()> {
        self.tick()?;
        if frame.len() > self.limits.max_frame_bytes {
            return Err("reconciliation frame exceeds byte limit".into());
        }
        validation::validate(frame)
    }
}
