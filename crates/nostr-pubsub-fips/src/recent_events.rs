use std::collections::{HashSet, VecDeque};
use std::sync::atomic::Ordering;

use nostr::Filter;
use nostr_pubsub::{
    EventSource, PubsubPeerInterest, QueryEvent, SOURCE_PRIORITY_FIPS_ENDPOINT, VerifiedEvent,
};

use crate::client_inner::{ActiveSubscription, InventoryAdvertisement};
use crate::{FIPS_NOSTR_PUBSUB_MAX_SEEN_EVENT_IDS, PeerIdentity, event_payload_bytes};

impl crate::ClientInner {
    pub(super) async fn handle_inv(
        &self,
        source_peer: PeerIdentity,
        source_npub: &str,
        inventory: InventoryAdvertisement,
    ) {
        self.inv_frames_received.fetch_add(1, Ordering::Relaxed);
        if inventory.subscription_ids.len()
            > self.options.max_active_subscriptions.saturating_add(1)
        {
            return;
        }
        let event_id = inventory.event_id.to_hex();
        let cached = self
            .recent_events
            .lock()
            .ok()
            .and_then(|events| events.event(&event_id).cloned());
        if let Some(event) = cached {
            let source = EventSource::fips_endpoint(source_npub);
            if event.as_event().kind.as_u16() != inventory.event_kind
                || event_payload_bytes(&event).ok() != Some(inventory.payload_bytes)
                || !self.event_is_admitted(&event, &source).await
            {
                return;
            }
            // A peer has freshly advertised this authenticated body. Scope the
            // observation to its fresh subscriptions; replay and dedup on ordinary
            // subscriptions stay unchanged. WANT has no subscription ID, so a
            // redundant refetch could be answered under an older subscription.
            let Ok(mut subscriptions) = self.lock_subscriptions() else {
                return;
            };
            for id in inventory.subscription_ids {
                let Some(active) = subscriptions.get_mut(&id.to_string()) else {
                    continue;
                };
                if active.fresh
                    && active.peers.contains(source_npub)
                    && !active.recent_event_ids.contains(&event_id)
                    && PubsubPeerInterest::from_filters(&active.filters, &event)
                        == PubsubPeerInterest::Subscribed
                {
                    deliver_local(
                        active,
                        event.clone(),
                        source.clone(),
                        &event_id,
                        FIPS_NOSTR_PUBSUB_MAX_SEEN_EVENT_IDS,
                    );
                }
            }
            return;
        }
        let Ok(Some(frame)) = self.accept_inventory(source_npub, inventory) else {
            return;
        };
        if self.send_frame(source_peer, frame).is_ok() {
            self.want_frames_sent.fetch_add(1, Ordering::Relaxed);
        }
    }

    // WANT identifies a body, not a subscription. A peer can legitimately answer
    // using an older open subscription. Only an outstanding, matching fresh
    // request permits that response to cross the normal observation dedup fence.
    pub(super) fn fresh_response_subscription(
        &self,
        peer: &str,
        event: &VerifiedEvent,
    ) -> Option<nostr::SubscriptionId> {
        let id = event.as_event().id.to_hex();
        let pending = self.pending_wants.lock().ok()?;
        let request = pending.entries.get(&id)?;
        if request.selected.peer_npub != peer
            || request.event_kind != event.as_event().kind.as_u16()
            || request.payload_bytes != crate::event_payload_bytes(event).ok()?
        {
            return None;
        }
        let subscriptions = self.lock_subscriptions().ok()?;
        request
            .selected
            .subscription_ids
            .iter()
            .find_map(|subscription_id| {
                let active = subscriptions.get(subscription_id)?;
                (active.fresh
                    && active.peers.contains(peer)
                    && !active.recent_event_ids.contains(&id)
                    && PubsubPeerInterest::from_filters(&active.filters, event)
                        == PubsubPeerInterest::Subscribed)
                    .then(|| nostr::SubscriptionId::new(subscription_id.clone()))
            })
    }
}

#[derive(Clone)]
pub(super) struct CachedEvent {
    pub(super) event: VerifiedEvent,
    pub(super) source: EventSource,
    pub(super) hop_limit: u8,
}

pub(super) struct RecentEvents {
    pub(super) max_payload_events: usize,
    pub(super) max_seen_ids: usize,
    pub(super) event_ids: HashSet<String>,
    pub(super) event_id_order: VecDeque<String>,
    pub(super) entries: VecDeque<CachedEvent>,
}

impl RecentEvents {
    pub(super) fn new(max_payload_events: usize, max_seen_ids: usize) -> Self {
        Self {
            max_payload_events,
            max_seen_ids,
            event_ids: HashSet::new(),
            event_id_order: VecDeque::new(),
            entries: VecDeque::new(),
        }
    }

    pub(super) fn insert(
        &mut self,
        event: VerifiedEvent,
        source: EventSource,
        hop_limit: u8,
    ) -> bool {
        let event_id = event.as_event().id.to_string();
        if !self.event_ids.insert(event_id.clone()) {
            return false;
        }
        self.event_id_order.push_back(event_id);
        self.entries.push_back(CachedEvent {
            event,
            source,
            hop_limit,
        });
        while self.entries.len() > self.max_payload_events {
            self.entries.pop_front();
        }
        while self.event_ids.len() > self.max_seen_ids {
            let Some(removed) = self.event_id_order.pop_front() else {
                break;
            };
            self.event_ids.remove(&removed);
        }
        true
    }

    /// An application's durable outbox may retry an event whose payload has
    /// left the live window. Refresh that payload and its bounded seen-ID age;
    /// inbound gossip still uses `insert` and cannot undo deduplication.
    pub(super) fn insert_for_publish(
        &mut self,
        event: VerifiedEvent,
        source: EventSource,
        hop_limit: u8,
    ) -> bool {
        let id = event.as_event().id.to_string();
        if self.event(&id).is_none() && self.event_ids.remove(&id) {
            self.event_id_order.retain(|seen| seen != &id);
        }
        self.insert(event, source, hop_limit)
    }

    pub(super) fn contains(&self, event_id: &str) -> bool {
        self.event_ids.contains(event_id)
    }

    pub(super) fn event(&self, event_id: &str) -> Option<&VerifiedEvent> {
        self.entries
            .iter()
            .find(|cached| cached.event.as_event().id.to_string() == event_id)
            .map(|cached| &cached.event)
    }

    pub(super) fn matching(&self, filters: &[Filter]) -> Vec<CachedEvent> {
        self.entries
            .iter()
            .filter(|cached| {
                PubsubPeerInterest::from_filters(filters, &cached.event)
                    == PubsubPeerInterest::Subscribed
            })
            .cloned()
            .collect()
    }
}

pub(super) fn deliver_local(
    active: &mut ActiveSubscription,
    event: VerifiedEvent,
    source: EventSource,
    event_id: &str,
    max_replay_events: usize,
) {
    active.recent_event_ids.insert(event_id.to_string());
    active.recent_event_order.push_back(event_id.to_string());
    while active.recent_event_order.len() > max_replay_events {
        if let Some(oldest) = active.recent_event_order.pop_front() {
            active.recent_event_ids.remove(&oldest);
        }
    }
    let _ = active.sender.try_send(QueryEvent {
        event,
        source,
        priority: SOURCE_PRIORITY_FIPS_ENDPOINT,
    });
}
