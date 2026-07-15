use nostr::Event;

use super::InvWantMesh;

#[derive(Debug, Clone)]
pub(super) struct CachedEvent {
    pub(super) event: Event,
    pub(super) expires_at_ms: u64,
    payload_bytes: u64,
}

/// Raw retained-state units for deterministic memory accounting.
///
/// Counts intentionally remain unweighted. Consumers can calibrate them to
/// allocator/RSS measurements without changing the production state machine.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InvWantMeshRetainedState {
    pub cached_events: usize,
    pub cached_event_bytes: u64,
    pub seen_inventories: usize,
    pub delivered_events: usize,
    pub upstream_routes: usize,
    pub transport_disrupted_route_peers: usize,
    pub pending_events: usize,
    pub pending_peers: usize,
    pub forwarded_wants: usize,
    pub peer_behaviors: usize,
}

impl InvWantMesh {
    #[must_use]
    pub fn retained_state(&self) -> InvWantMeshRetainedState {
        InvWantMeshRetainedState {
            cached_events: self.cached_events.len(),
            cached_event_bytes: self.cached_event_bytes,
            seen_inventories: self.seen_inventories.len(),
            delivered_events: self.delivered_events.len(),
            upstream_routes: self.upstream_routes.len(),
            transport_disrupted_route_peers: self
                .upstream_routes
                .values()
                .map(|route| route.transport_disrupted_peers.len())
                .sum(),
            pending_events: self.pending_downstream.len(),
            pending_peers: self.pending_peer_count,
            forwarded_wants: self.want_forwarded.len(),
            peer_behaviors: self.peer_behaviors.len(),
        }
    }

    pub(super) fn store_event(&mut self, event: Event, payload_bytes: u32, now_ms: u64) {
        let event_id = event.id.to_hex();
        let expires_at_ms = now_ms.saturating_add(self.options.event_ttl_ms);
        let payload_bytes = u64::from(payload_bytes);
        if let Some(cached) = self.cached_events.get_mut(&event_id) {
            self.cached_event_bytes = self
                .cached_event_bytes
                .saturating_sub(cached.payload_bytes)
                .saturating_add(payload_bytes);
            *cached = CachedEvent {
                event,
                expires_at_ms,
                payload_bytes,
            };
            self.track_expiry(expires_at_ms);
            return;
        }
        while self.cached_events.len() >= self.options.max_cached_events
            || self.cached_event_bytes.saturating_add(payload_bytes)
                > self.options.max_cached_event_bytes as u64
        {
            if !self.evict_oldest_cached_event() {
                break;
            }
        }
        self.cache_order.push_back(event_id.clone());
        self.cached_event_bytes = self.cached_event_bytes.saturating_add(payload_bytes);
        self.cached_events.insert(
            event_id,
            CachedEvent {
                event,
                expires_at_ms,
                payload_bytes,
            },
        );
        self.track_expiry(expires_at_ms);
    }

    pub(super) fn prune_cached_events(&mut self, now_ms: u64) {
        self.cached_events
            .retain(|_, cached| cached.expires_at_ms > now_ms);
        self.cache_order
            .retain(|event_id| self.cached_events.contains_key(event_id));
        self.cached_event_bytes = self.cached_events.values().fold(0, |total, cached| {
            total.saturating_add(cached.payload_bytes)
        });
    }

    pub(super) fn remove_pending_event(&mut self, event_id: &str) -> Option<super::PendingPeers> {
        let removed = self.pending_downstream.remove(event_id);
        if let Some(pending) = removed.as_ref() {
            self.pending_peer_count = self.pending_peer_count.saturating_sub(pending.peers.len());
        }
        removed
    }

    pub(super) fn remember_delivered(&mut self, event_id: &str, now_ms: u64) -> bool {
        if self.delivered_events.contains_key(event_id) {
            return false;
        }
        while self.delivered_events.len() >= self.options.max_seen_events {
            let Some(oldest) = self.delivered_order.pop_front() else {
                break;
            };
            self.delivered_events.remove(&oldest);
        }
        let expires_at_ms = now_ms.saturating_add(self.options.event_ttl_ms);
        self.delivered_order.push_back(event_id.to_string());
        self.delivered_events
            .insert(event_id.to_string(), expires_at_ms);
        self.track_expiry(expires_at_ms);
        true
    }

    fn evict_oldest_cached_event(&mut self) -> bool {
        while let Some(oldest) = self.cache_order.pop_front() {
            if let Some(cached) = self.cached_events.remove(&oldest) {
                self.cached_event_bytes =
                    self.cached_event_bytes.saturating_sub(cached.payload_bytes);
                return true;
            }
        }
        false
    }
}
