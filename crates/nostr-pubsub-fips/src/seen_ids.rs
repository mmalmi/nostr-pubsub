use std::collections::{HashMap, VecDeque};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ObservationScope {
    peer_npub: String,
    subscription_id: String,
}

#[derive(Default)]
struct IdWindow {
    ids: HashMap<String, IdObservation>,
    order: VecDeque<(String, u64)>,
}

struct IdObservation {
    generation: u64,
    matched_subscription: bool,
}

pub(super) struct Observation {
    pub(super) first: bool,
    pub(super) previously_matched: bool,
}

/// Bounded event-ID observations scoped to one authenticated peer and
/// subscription epoch. An inventory from one peer therefore cannot suppress a
/// complete event later offered by another peer.
pub(super) struct ScopedSeenIds {
    max_per_scope: usize,
    max_total: usize,
    next_generation: u64,
    total: usize,
    scopes: HashMap<ObservationScope, IdWindow>,
    global_order: VecDeque<(ObservationScope, String, u64)>,
}

impl ScopedSeenIds {
    pub(super) fn new(max_per_scope: usize, max_total: usize) -> Self {
        debug_assert!(max_per_scope > 0);
        debug_assert!(max_total >= max_per_scope);
        Self {
            max_per_scope,
            max_total,
            next_generation: 1,
            total: 0,
            scopes: HashMap::new(),
            global_order: VecDeque::new(),
        }
    }

    /// Returns true only for the first observation still inside the bounded
    /// peer/subscription window.
    pub(super) fn observe(
        &mut self,
        peer_npub: &str,
        subscription_id: &str,
        event_id: &str,
    ) -> bool {
        self.observe_with_match(peer_npub, subscription_id, event_id, false)
            .first
    }

    pub(super) fn observe_response(
        &mut self,
        peer_npub: &str,
        wire_subscription: Option<&str>,
        response_subscription: &str,
        event_id: &str,
        matched_subscription: bool,
    ) -> Observation {
        // Fresh WANTs may be answered under an older open wire subscription.
        // Keep that identity for late duplicates without pre-observing the
        // current delivery when the two subscriptions are the same.
        if let Some(wire) = wire_subscription
            && wire != response_subscription
        {
            self.observe_with_match(peer_npub, wire, event_id, matched_subscription);
        }
        self.observe_with_match(
            peer_npub,
            response_subscription,
            event_id,
            matched_subscription,
        )
    }

    pub(super) fn observe_with_match(
        &mut self,
        peer_npub: &str,
        subscription_id: &str,
        event_id: &str,
        matched_subscription: bool,
    ) -> Observation {
        let scope = ObservationScope {
            peer_npub: peer_npub.to_string(),
            subscription_id: subscription_id.to_string(),
        };
        let window = self.scopes.entry(scope.clone()).or_default();
        if let Some(previous) = window.ids.get(event_id) {
            return Observation {
                first: false,
                previously_matched: previous.matched_subscription,
            };
        }

        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        window.ids.insert(
            event_id.to_string(),
            IdObservation {
                generation,
                matched_subscription,
            },
        );
        window.order.push_back((event_id.to_string(), generation));
        self.global_order
            .push_back((scope.clone(), event_id.to_string(), generation));
        self.total += 1;

        while window.ids.len() > self.max_per_scope {
            let Some((oldest, oldest_generation)) = window.order.pop_front() else {
                break;
            };
            if window.ids.get(&oldest).map(|id| id.generation) == Some(oldest_generation) {
                window.ids.remove(&oldest);
                self.total -= 1;
            }
        }
        self.evict_global();
        Observation {
            first: true,
            previously_matched: false,
        }
    }

    pub(super) fn clear_peer(&mut self, peer_npub: &str) {
        let removed = self
            .scopes
            .extract_if(|scope, _| scope.peer_npub == peer_npub)
            .map(|(_, window)| window.ids.len())
            .sum::<usize>();
        self.total = self.total.saturating_sub(removed);
        self.global_order
            .retain(|(scope, _, _)| scope.peer_npub != peer_npub);
    }

    pub(super) fn clear_subscription(&mut self, subscription_id: &str) {
        let removed = self
            .scopes
            .extract_if(|scope, _| scope.subscription_id == subscription_id)
            .map(|(_, window)| window.ids.len())
            .sum::<usize>();
        self.total = self.total.saturating_sub(removed);
        self.global_order
            .retain(|(scope, _, _)| scope.subscription_id != subscription_id);
    }

    fn evict_global(&mut self) {
        while self.total > self.max_total {
            let Some((scope, event_id, generation)) = self.global_order.pop_front() else {
                break;
            };
            let mut remove_scope = false;
            if let Some(window) = self.scopes.get_mut(&scope)
                && window.ids.get(&event_id).map(|id| id.generation) == Some(generation)
            {
                window.ids.remove(&event_id);
                self.total -= 1;
                while window.order.front().is_some_and(|(id, generation)| {
                    window.ids.get(id).map(|id| id.generation) != Some(*generation)
                }) {
                    window.order.pop_front();
                }
                remove_scope = window.ids.is_empty();
            }
            if remove_scope {
                self.scopes.remove(&scope);
            }
        }
        // Per-scope eviction leaves stale global records, including behind a
        // live oldest entry. Compact in batches while preserving FIFO order.
        if self.global_order.len() > self.max_total.saturating_mul(2) {
            self.global_order.retain(|(scope, id, generation)| {
                self.scopes
                    .get(scope)
                    .and_then(|window| window.ids.get(id))
                    .map(|id| id.generation)
                    == Some(*generation)
            });
        }
    }
}

#[cfg(test)]
#[path = "seen_ids/tests.rs"]
mod tests;
