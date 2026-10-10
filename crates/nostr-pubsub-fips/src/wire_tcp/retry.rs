use super::{Duration, Instant, SERVICE_RETRY_INTERVAL};

const MAX_SERVICE_RETRY_INTERVAL: Duration = Duration::from_mins(1);
const STABLE_SERVICE_INTERVAL: Duration = Duration::from_secs(30);

#[derive(Default)]
pub(super) struct ServiceRetry {
    pub(super) next_attempt_at: Option<Instant>,
    failures: u32,
    retry_scheduled: bool,
    connected_since: Option<Instant>,
}

impl ServiceRetry {
    pub(super) fn is_waiting(&self, now: Instant) -> bool {
        self.next_attempt_at.is_some_and(|due| now < due)
    }

    pub(super) fn started(&mut self, now: Instant) {
        self.next_attempt_at = Some(now + SERVICE_RETRY_INTERVAL);
        self.retry_scheduled = false;
        self.connected_since = None;
    }

    pub(super) fn failed(&mut self, now: Instant) {
        self.connected_since = None;
        self.failures = self.failures.saturating_add(1).min(6);
        let delay = SERVICE_RETRY_INTERVAL
            .saturating_mul(1 << (self.failures - 1))
            .min(MAX_SERVICE_RETRY_INTERVAL);
        // A TCP handshake may have consumed the whole attempt's deadline.
        // Start the failure cooldown now, retaining any longer existing floor.
        let due = now + delay;
        self.next_attempt_at = Some(self.next_attempt_at.map_or(due, |old| old.max(due)));
        self.retry_scheduled = true;
    }

    pub(super) fn connected(&mut self, now: Instant) {
        // A successful TCP handshake alone does not prove a usable service.
        // Accept-then-close peers must not erase the accumulated failure delay.
        let since = *self.connected_since.get_or_insert(now);
        if now.duration_since(since) >= STABLE_SERVICE_INTERVAL {
            self.restore_fast_retry(now);
        }
        self.retry_scheduled = false;
    }

    pub(super) fn in_flight(&mut self) {
        self.retry_scheduled = false;
    }

    pub(super) fn next_retry_at(&self) -> Option<Instant> {
        self.retry_scheduled
            .then_some(self.next_attempt_at)
            .flatten()
    }

    pub(super) fn service_discovered(&mut self, now: Instant) {
        self.connected_since = None;
        self.restore_fast_retry(now);
    }

    fn restore_fast_retry(&mut self, now: Instant) {
        self.failures = 0;
        // A fresh service advertisement or stable connection can end a long
        // backoff, retaining the minimum spacing against repeated epoch changes.
        self.next_attempt_at = self
            .next_attempt_at
            .map(|due| due.min(now + SERVICE_RETRY_INTERVAL));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_short_connections_keep_failure_backoff() {
        let mut retry = ServiceRetry::default();
        let mut now = Instant::now();
        for seconds in [3, 6, 12, 24, 48, 60] {
            retry.started(now);
            retry.connected(now + Duration::from_millis(10));
            let closed = now + Duration::from_millis(20);
            retry.failed(closed);
            assert_eq!(
                retry.next_retry_at(),
                Some(closed + Duration::from_secs(seconds))
            );
            now = closed + Duration::from_secs(seconds);
        }
    }

    #[test]
    fn failures_back_off_from_completion_and_recovery_restores_fast_retries() {
        let mut retry = ServiceRetry::default();
        let mut now = Instant::now();
        for seconds in [3, 6, 12, 24, 48, 60, 60] {
            retry.started(now);
            now += Duration::from_mins(5);
            retry.failed(now);
            let due = now + Duration::from_secs(seconds);
            assert!(retry.is_waiting(due.checked_sub(Duration::from_millis(1)).unwrap()));
            assert!(!retry.is_waiting(due));
            now = due;
        }
        retry.connected(now);
        retry.connected(now + STABLE_SERVICE_INTERVAL);
        now += STABLE_SERVICE_INTERVAL;
        assert_eq!(retry.next_retry_at(), None);
        retry.started(now);
        assert_eq!(retry.next_retry_at(), None);
        retry.failed(now);
        assert_eq!(retry.next_retry_at(), Some(now + SERVICE_RETRY_INTERVAL));
        assert!(!retry.is_waiting(now + SERVICE_RETRY_INTERVAL));
        retry.in_flight();
        assert_eq!(retry.next_retry_at(), None);
        retry.failed(now + Duration::from_secs(10));
        assert_eq!(retry.next_retry_at(), Some(now + Duration::from_secs(16)));
    }

    #[test]
    fn discovered_service_shortens_only_a_long_backoff() {
        let now = Instant::now();
        let mut retry = ServiceRetry::default();
        retry.service_discovered(now);
        assert!(!retry.is_waiting(now), "a new service starts eagerly");
        for _ in 0..6 {
            retry.failed(now);
        }
        retry.service_discovered(now);
        assert!(retry.is_waiting(now));
        assert!(!retry.is_waiting(now + SERVICE_RETRY_INTERVAL));
        let almost_due = (now + SERVICE_RETRY_INTERVAL)
            .checked_sub(Duration::from_millis(1))
            .unwrap();
        retry.service_discovered(almost_due);
        assert!(!retry.is_waiting(now + SERVICE_RETRY_INTERVAL));
    }

    #[test]
    fn incoming_connection_is_usable_without_resetting_backoff_on_early_close() {
        let now = Instant::now();
        let mut retry = ServiceRetry::default();
        for _ in 0..6 {
            retry.failed(now);
        }
        let connected = now + Duration::from_secs(5);
        retry.connected(connected);
        assert_eq!(retry.next_retry_at(), None);
        let closed = connected + Duration::from_secs(1);
        retry.failed(closed);
        assert_eq!(
            retry.next_retry_at(),
            Some(closed + MAX_SERVICE_RETRY_INTERVAL)
        );
    }
}
