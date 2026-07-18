use std::sync::atomic::Ordering;

use nostr_pubsub::{PubsubPeerSubscriptionSnapshot, Result};

use super::{FipsPubsubClient, poisoned};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FipsPubsubDeliverySnapshot {
    pub inv_frames_received: u64,
    pub want_frames_sent: u64,
    pub subscription_events_received: u64,
}

impl FipsPubsubClient {
    #[must_use]
    pub fn delivery_snapshot(&self) -> FipsPubsubDeliverySnapshot {
        FipsPubsubDeliverySnapshot {
            inv_frames_received: self.inner.inv_frames_received.load(Ordering::Relaxed),
            want_frames_sent: self.inner.want_frames_sent.load(Ordering::Relaxed),
            subscription_events_received: self
                .inner
                .subscription_events_received
                .load(Ordering::Relaxed),
        }
    }

    /// Returns raw retained state for subscriptions accepted from FIPS peers.
    ///
    /// Encoded byte counts are canonical Nostr JSON sizes. They are useful for
    /// deterministic resource accounting but do not estimate allocator overhead.
    pub fn peer_subscription_snapshot(&self) -> Result<PubsubPeerSubscriptionSnapshot> {
        self.inner
            .peer_subscriptions
            .lock()
            .map_err(|_| poisoned("FIPS peer subscription state"))?
            .retained_snapshot()
    }
}
