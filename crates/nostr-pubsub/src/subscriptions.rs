use std::collections::{BTreeMap, VecDeque};

use nostr::{ClientMessage, Event, Filter};

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
    ) -> Option<PubsubPeerSubscription> {
        let subscription_id = subscription.subscription_id.clone();
        self.order.retain(|id| id != &subscription_id);
        self.order.push_back(subscription_id.clone());
        let replaced = self
            .subscriptions
            .insert(subscription_id.clone(), subscription);
        if replaced.is_none() {
            self.evict_oldest_over_limit(limits.max_subscriptions_per_peer)
        } else {
            None
        }
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

        let is_new_peer = !self.peers.contains_key(&peer_id);
        self.touch_peer(peer_id.clone());
        if is_new_peer {
            self.evict_peers_over_limit();
        }
        let peer = self.peers.entry(peer_id).or_default();
        Ok(peer.upsert(subscription, self.limits))
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
        self.peers
            .remove(peer_id)
            .map(|peer| peer.subscriptions.into_values().collect())
            .unwrap_or_default()
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

    fn evict_peers_over_limit(&mut self) {
        while self.peers.len() >= self.limits.max_peers {
            let Some(peer_id) = self.peer_order.pop_front() else {
                break;
            };
            if self.peers.remove(&peer_id).is_some() {
                break;
            }
        }
    }
}
