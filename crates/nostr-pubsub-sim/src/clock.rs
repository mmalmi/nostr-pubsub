use std::collections::{BTreeMap, VecDeque};

/// Deterministic, wall-clock-free scheduler for discrete simulation events.
///
/// Events are ordered by their scheduled millisecond and retain insertion order
/// when several events share the same timestamp. Scheduling an event in the past
/// clamps it to the current virtual time, so [`Self::now_ms`] never moves
/// backwards.
#[derive(Debug)]
pub struct VirtualScheduler<T> {
    now_ms: u64,
    events: BTreeMap<u64, VecDeque<T>>,
    pending: usize,
    peak_pending: usize,
}

impl<T> Default for VirtualScheduler<T> {
    fn default() -> Self {
        Self::new(0)
    }
}

impl<T> VirtualScheduler<T> {
    /// Creates an empty scheduler at `now_ms`.
    #[must_use]
    pub fn new(now_ms: u64) -> Self {
        Self {
            now_ms,
            events: BTreeMap::new(),
            pending: 0,
            peak_pending: 0,
        }
    }

    /// Returns the current virtual time in milliseconds.
    #[must_use]
    pub const fn now_ms(&self) -> u64 {
        self.now_ms
    }

    /// Schedules `event` at an absolute virtual timestamp.
    ///
    /// Timestamps earlier than the current virtual time are clamped to
    /// [`Self::now_ms`]. The returned value is the effective timestamp.
    pub fn schedule_at(&mut self, at_ms: u64, event: T) -> u64 {
        let effective_at_ms = at_ms.max(self.now_ms);
        self.events
            .entry(effective_at_ms)
            .or_default()
            .push_back(event);
        self.pending = self.pending.saturating_add(1);
        self.peak_pending = self.peak_pending.max(self.pending);
        effective_at_ms
    }

    /// Schedules `event` relative to the current virtual time.
    ///
    /// Addition saturates at [`u64::MAX`]. The returned value is the effective
    /// absolute timestamp.
    pub fn schedule_after(&mut self, delay_ms: u64, event: T) -> u64 {
        self.schedule_at(self.now_ms.saturating_add(delay_ms), event)
    }

    /// Removes the next event and advances virtual time to its timestamp.
    pub fn pop_next(&mut self) -> Option<T> {
        let at_ms = *self.events.first_key_value()?.0;
        let (event, bucket_empty) = {
            let bucket = self
                .events
                .get_mut(&at_ms)
                .expect("first event bucket must exist");
            let event = bucket
                .pop_front()
                .expect("scheduled event bucket must not be empty");
            (event, bucket.is_empty())
        };

        if bucket_empty {
            self.events.remove(&at_ms);
        }
        self.pending -= 1;
        self.now_ms = at_ms;
        Some(event)
    }

    /// Returns the number of queued events.
    #[must_use]
    pub const fn pending_len(&self) -> usize {
        self.pending
    }

    /// Returns the highest queue depth observed since construction.
    #[must_use]
    pub const fn peak_pending_len(&self) -> usize {
        self.peak_pending
    }

    /// Returns whether no events are queued.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.pending == 0
    }
}

#[cfg(test)]
mod tests {
    use super::VirtualScheduler;

    #[test]
    fn default_scheduler_starts_empty_at_zero() {
        let scheduler = VirtualScheduler::<()>::default();

        assert_eq!(scheduler.now_ms(), 0);
        assert_eq!(scheduler.pending_len(), 0);
        assert_eq!(scheduler.peak_pending_len(), 0);
        assert!(scheduler.is_empty());
    }

    #[test]
    fn events_pop_in_timestamp_order_and_advance_time() {
        let mut scheduler = VirtualScheduler::new(10);
        scheduler.schedule_at(30, "third");
        scheduler.schedule_at(12, "first");
        scheduler.schedule_at(20, "second");

        assert_eq!(scheduler.pop_next(), Some("first"));
        assert_eq!(scheduler.now_ms(), 12);
        assert_eq!(scheduler.pop_next(), Some("second"));
        assert_eq!(scheduler.now_ms(), 20);
        assert_eq!(scheduler.pop_next(), Some("third"));
        assert_eq!(scheduler.now_ms(), 30);
        assert_eq!(scheduler.pop_next(), None);
        assert_eq!(scheduler.now_ms(), 30);
    }

    #[test]
    fn equal_timestamps_preserve_fifo_order() {
        let mut scheduler = VirtualScheduler::default();
        scheduler.schedule_at(5, 1);
        scheduler.schedule_at(5, 2);
        scheduler.schedule_at(5, 3);

        assert_eq!(scheduler.pop_next(), Some(1));
        assert_eq!(scheduler.pop_next(), Some(2));
        assert_eq!(scheduler.pop_next(), Some(3));
        assert_eq!(scheduler.now_ms(), 5);
    }

    #[test]
    fn schedule_after_is_relative_to_advanced_time() {
        let mut scheduler = VirtualScheduler::new(100);
        assert_eq!(scheduler.schedule_after(25, "first"), 125);
        assert_eq!(scheduler.pop_next(), Some("first"));

        assert_eq!(scheduler.schedule_after(10, "second"), 135);
        assert_eq!(scheduler.pop_next(), Some("second"));
        assert_eq!(scheduler.now_ms(), 135);
    }

    #[test]
    fn scheduling_in_the_past_never_rewinds_time() {
        let mut scheduler = VirtualScheduler::new(50);

        assert_eq!(scheduler.schedule_at(20, "late arrival"), 50);
        assert_eq!(scheduler.pop_next(), Some("late arrival"));
        assert_eq!(scheduler.now_ms(), 50);
    }

    #[test]
    fn relative_time_saturates_without_wrapping() {
        let mut scheduler = VirtualScheduler::new(u64::MAX - 2);

        assert_eq!(scheduler.schedule_after(10, "end"), u64::MAX);
        assert_eq!(scheduler.pop_next(), Some("end"));
        assert_eq!(scheduler.now_ms(), u64::MAX);
    }

    #[test]
    fn pending_and_peak_depth_track_queue_usage() {
        let mut scheduler = VirtualScheduler::default();
        scheduler.schedule_after(3, 'a');
        scheduler.schedule_after(1, 'b');
        scheduler.schedule_after(2, 'c');

        assert_eq!(scheduler.pending_len(), 3);
        assert_eq!(scheduler.peak_pending_len(), 3);
        assert_eq!(scheduler.pop_next(), Some('b'));
        assert_eq!(scheduler.pending_len(), 2);

        scheduler.schedule_after(1, 'd');
        assert_eq!(scheduler.pending_len(), 3);
        assert_eq!(scheduler.peak_pending_len(), 3);

        while scheduler.pop_next().is_some() {}
        assert!(scheduler.is_empty());
        assert_eq!(scheduler.peak_pending_len(), 3);
    }
}
