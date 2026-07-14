use std::collections::{BTreeMap, VecDeque};

use nostr::{ClientMessage, Event, Filter, JsonUtil, SubscriptionId};

use crate::{PubsubError, Result, SourceId, VerifiedEvent, subscription_filters_match};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubDeliveryStrategy {
    PushSubscribed,
    InventoryFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubPeerInterest {
    Subscribed,
    Unsubscribed,
    Unknown,
}

impl PubsubPeerInterest {
    #[must_use]
    pub fn from_filters(filters: &[Filter], event: &VerifiedEvent) -> Self {
        if subscription_filters_match(filters, event.as_event()) {
            Self::Subscribed
        } else {
            Self::Unsubscribed
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PubsubDeliveryAction {
    PushFrame,
    AnnounceInventory,
    Skip,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubsubDeliveryPolicy {
    pub strategy: PubsubDeliveryStrategy,
}

impl PubsubDeliveryPolicy {
    #[must_use]
    pub const fn push_subscribed() -> Self {
        Self {
            strategy: PubsubDeliveryStrategy::PushSubscribed,
        }
    }

    #[must_use]
    pub const fn inventory_to_subscribers() -> Self {
        Self {
            strategy: PubsubDeliveryStrategy::InventoryFirst,
        }
    }

    /// Inventory-first delivery to peers with matching subscriptions.
    ///
    /// This is kept as a mesh-oriented constructor, but inventory is still
    /// gated by Nostr subscription/filter interest.
    #[must_use]
    pub const fn inventory_to_peers() -> Self {
        Self {
            strategy: PubsubDeliveryStrategy::InventoryFirst,
        }
    }

    #[must_use]
    pub fn action_for_peer(self, interest: PubsubPeerInterest) -> PubsubDeliveryAction {
        match (self.strategy, interest) {
            (PubsubDeliveryStrategy::PushSubscribed, PubsubPeerInterest::Subscribed) => {
                PubsubDeliveryAction::PushFrame
            }
            (PubsubDeliveryStrategy::InventoryFirst, PubsubPeerInterest::Subscribed) => {
                PubsubDeliveryAction::AnnounceInventory
            }
            _ => PubsubDeliveryAction::Skip,
        }
    }

    #[must_use]
    pub fn action_for_event(
        self,
        subscriptions: &PubsubPeerSubscriptionStore,
        peer_id: &SourceId,
        event: &VerifiedEvent,
    ) -> PubsubDeliveryAction {
        self.action_for_peer(subscriptions.peer_interest(peer_id, event))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PubsubSubscriptionLimits {
    pub max_peers: usize,
    pub max_subscriptions_per_peer: usize,
    pub max_filters_per_subscription: usize,
}

impl Default for PubsubSubscriptionLimits {
    fn default() -> Self {
        Self {
            max_peers: 1024,
            max_subscriptions_per_peer: 64,
            max_filters_per_subscription: 16,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubsubPeerSubscription {
    pub subscription_id: String,
    pub filters: Vec<Filter>,
}

impl PubsubPeerSubscription {
    #[must_use]
    pub fn new(subscription_id: impl Into<String>, filters: Vec<Filter>) -> Self {
        Self {
            subscription_id: subscription_id.into(),
            filters,
        }
    }

    #[must_use]
    pub fn matches(&self, event: &VerifiedEvent) -> bool {
        self.matches_event(event.as_event())
    }

    #[must_use]
    pub fn matches_event(&self, event: &Event) -> bool {
        subscription_filters_match(&self.filters, event)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PubsubSubscriptionUpdate {
    Subscribed,
    Closed,
    Ignored,
}

#[derive(Debug, Clone, Default)]
struct PeerSubscriptionSet {
    subscriptions: BTreeMap<String, PubsubPeerSubscription>,
    order: VecDeque<String>,
}

impl PeerSubscriptionSet {
    fn upsert(
        &mut self,
        subscription: PubsubPeerSubscription,
        limits: PubsubSubscriptionLimits,
    ) -> (
        Option<PubsubPeerSubscription>,
        Option<PubsubPeerSubscription>,
    ) {
        let subscription_id = subscription.subscription_id.clone();
        self.order.retain(|id| id != &subscription_id);
        self.order.push_back(subscription_id.clone());
        let replaced = self
            .subscriptions
            .insert(subscription_id.clone(), subscription);
        let evicted = if replaced.is_none() {
            self.evict_oldest_over_limit(limits.max_subscriptions_per_peer)
        } else {
            None
        };
        (replaced, evicted)
    }

    fn remove(&mut self, subscription_id: &str) -> Option<PubsubPeerSubscription> {
        self.order.retain(|id| id != subscription_id);
        self.subscriptions.remove(subscription_id)
    }

    fn evict_oldest_over_limit(&mut self, limit: usize) -> Option<PubsubPeerSubscription> {
        while self.subscriptions.len() > limit {
            let Some(subscription_id) = self.order.pop_front() else {
                break;
            };
            if let Some(removed) = self.subscriptions.remove(&subscription_id) {
                return Some(removed);
            }
        }
        None
    }

    fn is_empty(&self) -> bool {
        self.subscriptions.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct PubsubPeerSubscriptionStore {
    limits: PubsubSubscriptionLimits,
    peers: BTreeMap<SourceId, PeerSubscriptionSet>,
    peer_order: VecDeque<SourceId>,
    retained: PubsubPeerSubscriptionSnapshot,
}

/// Raw retained-state measurements for peer subscriptions.
///
/// Encoded byte counts use canonical compact Nostr JSON. They measure the
/// wire-equivalent retained control state, not allocator or container overhead.
/// `encoded_filter_bytes` is a component view of `encoded_req_bytes`; do not
/// add the two when estimating retained state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PubsubPeerSubscriptionSnapshot {
    pub peer_count: usize,
    pub subscription_count: usize,
    pub filter_count: usize,
    /// Sum of canonical JSON bytes for every retained filter, including repeats.
    pub encoded_filter_bytes: usize,
    /// Sum of canonical complete `REQ` frame bytes for retained subscriptions.
    pub encoded_req_bytes: usize,
}

impl Default for PubsubPeerSubscriptionStore {
    fn default() -> Self {
        Self::new(PubsubSubscriptionLimits::default())
    }
}

impl PubsubPeerSubscriptionStore {
    #[must_use]
    pub fn new(limits: PubsubSubscriptionLimits) -> Self {
        Self {
            limits,
            peers: BTreeMap::new(),
            peer_order: VecDeque::new(),
            retained: PubsubPeerSubscriptionSnapshot::default(),
        }
    }

    #[must_use]
    pub fn limits(&self) -> PubsubSubscriptionLimits {
        self.limits
    }

    #[must_use]
    pub fn peer_count(&self) -> usize {
        self.peers.len()
    }

    #[must_use]
    pub fn subscription_count(&self) -> usize {
        self.peers
            .values()
            .map(|peer| peer.subscriptions.len())
            .sum()
    }

    #[must_use]
    pub fn filter_count(&self) -> usize {
        self.retained.filter_count
    }

    /// Number of retained filter candidates for one peer. Event matching can
    /// short-circuit before evaluating every candidate.
    #[must_use]
    pub fn peer_filter_count(&self, peer_id: &SourceId) -> usize {
        self.peers.get(peer_id).map_or(0, |peer| {
            peer.subscriptions
                .values()
                .map(|subscription| subscription.filters.len())
                .sum()
        })
    }

    /// Measures the exact canonical encoded size of the retained filters and
    /// their complete Nostr `REQ` control frames.
    pub fn retained_snapshot(&self) -> Result<PubsubPeerSubscriptionSnapshot> {
        Ok(self.retained)
    }

    #[must_use]
    pub fn peer_subscription_count(&self, peer_id: &SourceId) -> usize {
        self.peers
            .get(peer_id)
            .map_or(0, |peer| peer.subscriptions.len())
    }

    pub fn apply_client_message(
        &mut self,
        peer_id: SourceId,
        message: ClientMessage<'_>,
    ) -> Result<PubsubSubscriptionUpdate> {
        match message {
            ClientMessage::Req {
                subscription_id,
                filters,
            } => {
                let subscription_id = subscription_id.into_owned().to_string();
                let filters = filters
                    .into_iter()
                    .map(std::borrow::Cow::into_owned)
                    .collect::<Vec<_>>();
                self.upsert_filters(peer_id, subscription_id, filters)?;
                Ok(PubsubSubscriptionUpdate::Subscribed)
            }
            ClientMessage::Close(subscription_id) => {
                let subscription_id = subscription_id.into_owned().to_string();
                self.remove(&peer_id, &subscription_id);
                Ok(PubsubSubscriptionUpdate::Closed)
            }
            _ => Ok(PubsubSubscriptionUpdate::Ignored),
        }
    }

    pub fn upsert_filters(
        &mut self,
        peer_id: SourceId,
        subscription_id: impl Into<String>,
        filters: Vec<Filter>,
    ) -> Result<Option<PubsubPeerSubscription>> {
        let subscription = PubsubPeerSubscription::new(subscription_id, filters);
        self.upsert(peer_id, subscription)
    }

    pub fn upsert(
        &mut self,
        peer_id: SourceId,
        subscription: PubsubPeerSubscription,
    ) -> Result<Option<PubsubPeerSubscription>> {
        if self.limits.max_peers == 0 {
            return Err(PubsubError::Validation(
                "peer subscription store max_peers must be greater than zero".to_string(),
            ));
        }
        if self.limits.max_subscriptions_per_peer == 0 {
            return Err(PubsubError::Validation(
                "peer subscription store max_subscriptions_per_peer must be greater than zero"
                    .to_string(),
            ));
        }
        if subscription.filters.len() > self.limits.max_filters_per_subscription {
            return Err(PubsubError::Validation(format!(
                "subscription {} has {} filters, limit is {}",
                subscription.subscription_id,
                subscription.filters.len(),
                self.limits.max_filters_per_subscription
            )));
        }
        let added = subscription_contribution(&subscription)?;

        let is_new_peer = !self.peers.contains_key(&peer_id);
        self.touch_peer(peer_id.clone());
        if is_new_peer {
            for removed in self.evict_peers_over_limit() {
                self.subtract_retained(&removed);
            }
        }
        let peer = self.peers.entry(peer_id).or_default();
        let (replaced, evicted) = peer.upsert(subscription, self.limits);
        if let Some(replaced) = replaced {
            self.subtract_retained(&replaced);
        }
        if let Some(evicted) = evicted.as_ref() {
            self.subtract_retained(evicted);
        }
        self.add_retained(added);
        self.retained.peer_count = self.peers.len();
        Ok(evicted)
    }

    pub fn remove(
        &mut self,
        peer_id: &SourceId,
        subscription_id: &str,
    ) -> Option<PubsubPeerSubscription> {
        let removed = self
            .peers
            .get_mut(peer_id)
            .and_then(|peer| peer.remove(subscription_id));
        if let Some(removed) = removed.as_ref() {
            self.subtract_retained(removed);
        }
        if self
            .peers
            .get(peer_id)
            .is_some_and(PeerSubscriptionSet::is_empty)
        {
            self.remove_peer(peer_id);
        }
        removed
    }

    pub fn remove_peer(&mut self, peer_id: &SourceId) -> Vec<PubsubPeerSubscription> {
        self.peer_order.retain(|candidate| candidate != peer_id);
        let removed = self
            .peers
            .remove(peer_id)
            .map(|peer| peer.subscriptions.into_values().collect())
            .unwrap_or_default();
        for subscription in &removed {
            self.subtract_retained(subscription);
        }
        self.retained.peer_count = self.peers.len();
        removed
    }

    #[must_use]
    pub fn peer_interest(&self, peer_id: &SourceId, event: &VerifiedEvent) -> PubsubPeerInterest {
        let Some(peer) = self.peers.get(peer_id) else {
            return PubsubPeerInterest::Unknown;
        };
        if peer
            .subscriptions
            .values()
            .any(|subscription| subscription.matches(event))
        {
            PubsubPeerInterest::Subscribed
        } else {
            PubsubPeerInterest::Unsubscribed
        }
    }

    #[must_use]
    pub fn matching_subscriptions<'a>(
        &'a self,
        peer_id: &SourceId,
        event: &VerifiedEvent,
    ) -> Vec<&'a PubsubPeerSubscription> {
        self.peers
            .get(peer_id)
            .into_iter()
            .flat_map(|peer| peer.subscriptions.values())
            .filter(|subscription| subscription.matches(event))
            .collect()
    }

    /// Iterates every `(peer, subscription)` pair matching an event in one
    /// pass, without cloning peer IDs or subscription state.
    pub fn matching_peer_subscriptions<'a>(
        &'a self,
        event: &'a VerifiedEvent,
    ) -> impl Iterator<Item = (&'a SourceId, &'a PubsubPeerSubscription)> + 'a {
        self.peers.iter().flat_map(move |(peer_id, peer)| {
            peer.subscriptions
                .values()
                .filter(move |subscription| subscription.matches(event))
                .map(move |subscription| (peer_id, subscription))
        })
    }

    #[must_use]
    pub fn interested_peers(&self, event: &VerifiedEvent) -> Vec<SourceId> {
        self.peers
            .iter()
            .filter(|(_, peer)| {
                peer.subscriptions
                    .values()
                    .any(|subscription| subscription.matches(event))
            })
            .map(|(peer_id, _)| peer_id.clone())
            .collect()
    }

    fn touch_peer(&mut self, peer_id: SourceId) {
        self.peer_order.retain(|candidate| candidate != &peer_id);
        self.peer_order.push_back(peer_id);
    }

    fn evict_peers_over_limit(&mut self) -> Vec<PubsubPeerSubscription> {
        while self.peers.len() >= self.limits.max_peers {
            let Some(peer_id) = self.peer_order.pop_front() else {
                break;
            };
            if let Some(peer) = self.peers.remove(&peer_id) {
                return peer.subscriptions.into_values().collect();
            }
        }
        Vec::new()
    }

    fn add_retained(&mut self, added: SubscriptionContribution) {
        self.retained.subscription_count = self.retained.subscription_count.saturating_add(1);
        self.retained.filter_count = self
            .retained
            .filter_count
            .saturating_add(added.filter_count);
        self.retained.encoded_filter_bytes = self
            .retained
            .encoded_filter_bytes
            .saturating_add(added.encoded_filter_bytes);
        self.retained.encoded_req_bytes = self
            .retained
            .encoded_req_bytes
            .saturating_add(added.encoded_req_bytes);
    }

    fn subtract_retained(&mut self, removed: &PubsubPeerSubscription) {
        let removed = subscription_contribution_infallible(removed);
        self.retained.subscription_count = self.retained.subscription_count.saturating_sub(1);
        self.retained.filter_count = self
            .retained
            .filter_count
            .saturating_sub(removed.filter_count);
        self.retained.encoded_filter_bytes = self
            .retained
            .encoded_filter_bytes
            .saturating_sub(removed.encoded_filter_bytes);
        self.retained.encoded_req_bytes = self
            .retained
            .encoded_req_bytes
            .saturating_sub(removed.encoded_req_bytes);
    }
}

#[derive(Debug, Clone, Copy)]
struct SubscriptionContribution {
    filter_count: usize,
    encoded_filter_bytes: usize,
    encoded_req_bytes: usize,
}

fn subscription_contribution(
    subscription: &PubsubPeerSubscription,
) -> Result<SubscriptionContribution> {
    let encoded_filter_bytes = subscription
        .filters
        .iter()
        .try_fold(0_usize, |total, filter| {
            filter
                .try_as_json()
                .map(|encoded| total.saturating_add(encoded.len()))
                .map_err(|error| encoded_state_error("filter", error))
        })?;
    let encoded_req_bytes = ClientMessage::req(
        SubscriptionId::new(subscription.subscription_id.clone()),
        subscription.filters.clone(),
    )
    .try_as_json()
    .map_err(|error| encoded_state_error("REQ", error))?
    .len();
    Ok(SubscriptionContribution {
        filter_count: subscription.filters.len(),
        encoded_filter_bytes,
        encoded_req_bytes,
    })
}

fn subscription_contribution_infallible(
    subscription: &PubsubPeerSubscription,
) -> SubscriptionContribution {
    subscription_contribution(subscription)
        .expect("a retained subscription was encoded successfully when inserted")
}

fn encoded_state_error(kind: &str, error: impl std::fmt::Debug) -> PubsubError {
    PubsubError::Storage(format!(
        "failed to encode retained peer subscription {kind}: {error:?}"
    ))
}

#[cfg(test)]
mod tests {
    use nostr::{EventBuilder, JsonUtil, Keys, Kind};

    use super::*;

    #[test]
    fn retained_snapshot_tracks_spam_replacement_eviction_and_close() {
        let mut store = PubsubPeerSubscriptionStore::new(PubsubSubscriptionLimits {
            max_peers: 2,
            max_subscriptions_per_peer: 2,
            max_filters_per_subscription: 2,
        });
        let peer_a = SourceId::new("peer-a");
        let peer_b = SourceId::new("peer-b");
        let peer_c = SourceId::new("peer-c");
        let note = Filter::new().kind(Kind::TextNote);
        let metadata = Filter::new().kind(Kind::Metadata);
        let direct = Filter::new().kind(Kind::EncryptedDirectMessage);

        store
            .upsert_filters(
                peer_a.clone(),
                "first",
                vec![note.clone(), metadata.clone()],
            )
            .unwrap();
        store
            .upsert_filters(peer_a.clone(), "second", vec![direct.clone()])
            .unwrap();
        assert_eq!(
            store.retained_snapshot().unwrap(),
            expected_snapshot(
                1,
                [
                    ("first", vec![note.clone(), metadata.clone()]),
                    ("second", vec![direct.clone()]),
                ],
            )
        );

        let before_spam = store.retained_snapshot().unwrap();
        assert!(
            store
                .upsert_filters(
                    peer_a.clone(),
                    "oversized",
                    vec![note.clone(), metadata.clone(), direct.clone()],
                )
                .is_err()
        );
        assert_eq!(store.retained_snapshot().unwrap(), before_spam);

        store
            .upsert_filters(peer_a.clone(), "first", vec![note.clone()])
            .unwrap();
        assert_eq!(store.filter_count(), 2);
        assert_eq!(store.peer_filter_count(&peer_a), 2);
        let evicted = store
            .upsert_filters(peer_a.clone(), "third", vec![metadata.clone()])
            .unwrap()
            .unwrap();
        assert_eq!(evicted.subscription_id, "second");

        store
            .upsert_filters(peer_b.clone(), "peer-b", vec![note.clone()])
            .unwrap();
        store
            .upsert_filters(peer_c.clone(), "peer-c", vec![direct.clone()])
            .unwrap();
        assert_eq!(store.peer_filter_count(&peer_a), 0);
        assert_eq!(
            store.retained_snapshot().unwrap(),
            expected_snapshot(
                2,
                [("peer-b", vec![note]), ("peer-c", vec![direct.clone()])],
            )
        );

        store
            .apply_client_message(peer_b, ClientMessage::close(SubscriptionId::new("peer-b")))
            .unwrap();
        assert_eq!(
            store.retained_snapshot().unwrap(),
            expected_snapshot(1, [("peer-c", vec![direct])])
        );
    }

    #[test]
    fn matching_peer_subscriptions_yields_each_matching_subscription_once() {
        let keys = Keys::generate();
        let event = VerifiedEvent::try_from(
            EventBuilder::text_note("single-pass")
                .sign_with_keys(&keys)
                .unwrap(),
        )
        .unwrap();
        let peer_a = SourceId::new("peer-a");
        let peer_b = SourceId::new("peer-b");
        let mut store = PubsubPeerSubscriptionStore::default();
        store
            .upsert_filters(peer_a.clone(), "a-1", vec![Filter::new()])
            .unwrap();
        store
            .upsert_filters(
                peer_a.clone(),
                "a-2",
                vec![
                    Filter::new().kind(Kind::TextNote),
                    Filter::new().author(keys.public_key()),
                ],
            )
            .unwrap();
        store
            .upsert_filters(
                peer_a.clone(),
                "a-no-match",
                vec![Filter::new().kind(Kind::Metadata)],
            )
            .unwrap();
        store
            .upsert_filters(peer_b.clone(), "b-1", vec![Filter::new()])
            .unwrap();

        let matches = store
            .matching_peer_subscriptions(&event)
            .map(|(peer, subscription)| (peer.as_str(), subscription.subscription_id.as_str()))
            .collect::<Vec<_>>();

        assert_eq!(store.peer_filter_count(&peer_a), 4);
        assert_eq!(
            matches,
            [("peer-a", "a-1"), ("peer-a", "a-2"), ("peer-b", "b-1")]
        );
    }

    fn expected_snapshot<const N: usize>(
        peer_count: usize,
        subscriptions: [(&str, Vec<Filter>); N],
    ) -> PubsubPeerSubscriptionSnapshot {
        PubsubPeerSubscriptionSnapshot {
            peer_count,
            subscription_count: subscriptions.len(),
            filter_count: subscriptions.iter().map(|(_, filters)| filters.len()).sum(),
            encoded_filter_bytes: subscriptions
                .iter()
                .flat_map(|(_, filters)| filters)
                .map(|filter| filter.as_json().len())
                .sum(),
            encoded_req_bytes: subscriptions
                .into_iter()
                .map(|(id, filters)| {
                    ClientMessage::req(SubscriptionId::new(id), filters)
                        .as_json()
                        .len()
                })
                .sum(),
        }
    }
}
