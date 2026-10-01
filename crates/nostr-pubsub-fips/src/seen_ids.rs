use std::collections::{HashMap, VecDeque};

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ObservationScope {
    peer_npub: String,
    subscription_id: String,
}

#[derive(Default)]
struct IdWindow {
    ids: HashMap<String, u64>,
    order: VecDeque<(String, u64)>,
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
        let scope = ObservationScope {
            peer_npub: peer_npub.to_string(),
            subscription_id: subscription_id.to_string(),
        };
        let window = self.scopes.entry(scope.clone()).or_default();
        if window.ids.contains_key(event_id) {
            return false;
        }

        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        window.ids.insert(event_id.to_string(), generation);
        window.order.push_back((event_id.to_string(), generation));
        self.global_order
            .push_back((scope.clone(), event_id.to_string(), generation));
        self.total += 1;

        while window.ids.len() > self.max_per_scope {
            let Some((oldest, oldest_generation)) = window.order.pop_front() else {
                break;
            };
            if window.ids.get(&oldest) == Some(&oldest_generation) {
                window.ids.remove(&oldest);
                self.total -= 1;
            }
        }
        self.evict_global();
        true
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
                && window.ids.get(&event_id) == Some(&generation)
            {
                window.ids.remove(&event_id);
                self.total -= 1;
                while window
                    .order
                    .front()
                    .is_some_and(|(id, generation)| window.ids.get(id) != Some(generation))
                {
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
                self.scopes.get(scope).and_then(|window| window.ids.get(id)) == Some(generation)
            });
        }
    }
}

#[cfg(test)]
#[path = "seen_ids/tests.rs"]
mod tests;
