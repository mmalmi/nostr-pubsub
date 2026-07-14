use nostr_pubsub::{PubsubPeerSubscriptionSnapshot, Result};

use super::{FipsPubsubClient, poisoned};

impl FipsPubsubClient {
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
