//! A bounded history cursor for one source and one Nostr filter.
//!
//! Record verified wire deliveries before application filtering or cross-source
//! deduplication. A timeout does not prove an interval complete. The caller owns
//! transport cancellation, query branching, time budgets, and when to load more.
use std::{
    collections::HashSet,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_CURSOR_ID: AtomicU64 = AtomicU64::new(1);

use nostr::Timestamp;

use crate::{EventId, Filter, MatchEventOptions, PubsubError, QueryEvent, Result, SourceId};

/// Opaque generation of one requested history page.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryPageToken {
    cursor: u64,
    page: u64,
}

/// Feed this filter to the selected source and retain its token for callbacks.
#[derive(Debug, Clone)]
pub struct HistoryPage {
    pub token: HistoryPageToken,
    pub source: SourceId,
    pub filter: Filter,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryPageCompletion {
    /// The source explicitly finished this page, for example with EOSE.
    Complete,
    /// Timeout, disconnect, cancellation by the transport, or unknown completion.
    Partial,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryPageStatus {
    Advanced,
    Retry,
    Exhausted,
    Limited,
    Stale,
}

/// Observations are useful diagnostics, not a claim of complete history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryPageOutcome {
    pub status: HistoryPageStatus,
    pub oldest: Option<Timestamp>,
    pub unique_events: usize,
}

#[derive(Debug)]
struct ActivePage {
    token: HistoryPageToken,
    seen: HashSet<EventId>,
    oldest: Option<Timestamp>,
    overflow: bool,
}

/// Construct separate instances for every source and filter branch.
#[derive(Debug)]
pub struct HistoryCursor {
    source: SourceId,
    filter: Filter,
    initial_limit: usize,
    max_limit: usize,
    identity: u64,
    generation: u64,
    active: Option<ActivePage>,
    stopped: bool,
}

impl HistoryCursor {
    /// `max_limit` bounds both dense-timestamp retries and retained event IDs.
    pub fn new(
        source: SourceId,
        mut filter: Filter,
        now: Timestamp,
        max_limit: usize,
    ) -> Result<Self> {
        let limit = filter.limit.unwrap_or(100);
        if limit == 0 || limit > max_limit || max_limit > 65_536 {
            return Err(PubsubError::Validation(
                "invalid history page limits".into(),
            ));
        }
        let until = filter.until.unwrap_or(now);
        if filter.since.is_some_and(|since| since > until) {
            return Err(PubsubError::Validation(
                "history since exceeds until".into(),
            ));
        }
        filter.limit = Some(limit);
        filter.until = Some(until);
        let mut identity = NEXT_CURSOR_ID.load(Ordering::Relaxed);
        loop {
            let next = identity.checked_add(1).ok_or_else(|| {
                PubsubError::Validation("history cursor identity exhausted".into())
            })?;
            match NEXT_CURSOR_ID.compare_exchange_weak(
                identity,
                next,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => identity = current,
            }
        }
        Ok(Self {
            source,
            filter,
            initial_limit: limit,
            max_limit,
            identity,
            generation: 0,
            active: None,
            stopped: false,
        })
    }

    /// Returns `None` when another page is active, cancelled, or finished.
    pub fn begin_page(&mut self) -> Option<HistoryPage> {
        if self.stopped || self.active.is_some() {
            return None;
        }
        self.generation = self.generation.checked_add(1)?;
        let token = HistoryPageToken {
            cursor: self.identity,
            page: self.generation,
        };
        self.active = Some(ActivePage {
            token,
            seen: HashSet::new(),
            oldest: None,
            overflow: false,
        });
        Some(HistoryPage {
            token,
            source: self.source.clone(),
            filter: self.filter.clone(),
        })
    }

    /// Returns whether this is a new, in-scope observation for this page.
    /// A duplicate in another source's cursor must still be observed here.
    pub fn observe(&mut self, token: HistoryPageToken, event: &QueryEvent) -> bool {
        let Some(page) = self.active.as_mut().filter(|page| page.token == token) else {
            return false;
        };
        let value = event.event.as_event();
        if event.source.id != self.source
            || !self.filter.match_event(value, MatchEventOptions::new())
            || page.seen.contains(&value.id)
        {
            return false;
        }
        if page.seen.len() == self.max_limit {
            page.overflow = true;
            return false;
        }
        page.seen.insert(value.id);
        page.oldest = Some(
            page.oldest
                .map_or(value.created_at, |oldest| oldest.min(value.created_at)),
        );
        true
    }

    pub fn finish_page(
        &mut self,
        token: HistoryPageToken,
        completion: HistoryPageCompletion,
    ) -> HistoryPageOutcome {
        if self.active.as_ref().is_none_or(|page| page.token != token) {
            return HistoryPageOutcome {
                status: HistoryPageStatus::Stale,
                oldest: None,
                unique_events: 0,
            };
        }
        let page = self.active.take().expect("active page checked");
        let unique_events = page.seen.len();
        let status = if page.overflow {
            self.stopped = true;
            HistoryPageStatus::Limited
        } else if completion == HistoryPageCompletion::Partial {
            // Partial delivery cannot prove that newer unseen events are absent.
            HistoryPageStatus::Retry
        } else if let Some(oldest) = page.oldest {
            let until = self.filter.until.expect("constructor sets until");
            let limit = self.filter.limit.expect("constructor sets limit");
            if oldest < until {
                self.filter.until = Some(oldest); // NIP-01 boundaries are inclusive.
                self.filter.limit = Some(self.initial_limit);
                HistoryPageStatus::Advanced
            } else if unique_events >= limit {
                if limit == self.max_limit {
                    self.stopped = true;
                    HistoryPageStatus::Limited
                } else {
                    self.filter.limit = Some(limit.saturating_mul(2).min(self.max_limit));
                    HistoryPageStatus::Advanced
                }
            } else if until.as_secs() == 0 || self.filter.since.is_some_and(|since| since >= until)
            {
                self.stopped = true;
                HistoryPageStatus::Exhausted
            } else {
                self.filter.until = Some(Timestamp::from(until.as_secs() - 1));
                self.filter.limit = Some(self.initial_limit);
                HistoryPageStatus::Advanced
            }
        } else {
            self.stopped = true;
            HistoryPageStatus::Exhausted
        };
        HistoryPageOutcome {
            status,
            oldest: page.oldest,
            unique_events,
        }
    }

    /// Invalidate outstanding callbacks. A new query needs a new cursor.
    pub fn cancel(&mut self) {
        self.active = None;
        self.stopped = true;
    }
}
