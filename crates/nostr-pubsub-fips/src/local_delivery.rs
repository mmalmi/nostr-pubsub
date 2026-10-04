use std::collections::{HashSet, VecDeque};
use std::sync::Mutex;

use nostr_pubsub::{QueryEvent, SubscriptionDeliveryStatus};
use tokio::sync::mpsc;

use crate::FIPS_NOSTR_PUBSUB_MAX_SEEN_EVENT_IDS;

/// Bodies awaiting application progress belong to the subscription, rather
/// than the replay cache. This state outlives removal of its wire subscription.
pub(super) struct LocalDelivery {
    state: Mutex<DeliveryState>,
}

struct DeliveryState {
    sender: Option<mpsc::Sender<QueryEvent>>,
    pending: VecDeque<QueryEvent>,
    pending_ids: HashSet<String>,
    delivered_ids: HashSet<String>,
    delivered_order: VecDeque<String>,
    maximum_pending: usize,
    status: SubscriptionDeliveryStatus,
}

impl LocalDelivery {
    pub(super) fn new(sender: mpsc::Sender<QueryEvent>, maximum_pending: usize) -> Self {
        Self {
            state: Mutex::new(DeliveryState {
                sender: Some(sender),
                pending: VecDeque::new(),
                pending_ids: HashSet::new(),
                delivered_ids: HashSet::new(),
                delivered_order: VecDeque::new(),
                maximum_pending,
                status: SubscriptionDeliveryStatus::Active,
            }),
        }
    }

    pub(super) fn status(&self) -> SubscriptionDeliveryStatus {
        self.state
            .lock()
            .map_or(SubscriptionDeliveryStatus::Closed, |state| state.status)
    }

    pub(super) fn contains(&self, id: &str) -> bool {
        self.state
            .lock()
            .is_ok_and(|state| state.delivered_ids.contains(id) || state.pending_ids.contains(id))
    }

    pub(super) fn enqueue(&self, event: QueryEvent) -> bool {
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        if state.status != SubscriptionDeliveryStatus::Active {
            return false;
        }
        let id = event.event.as_event().id.to_string();
        if state.delivered_ids.contains(&id) || state.pending_ids.contains(&id) {
            return false;
        }
        // Always admit older pending bodies before a new arrival, including
        // when a receiver has freed a slot but has not yet taken this mutex.
        state.refill();
        let Some(sender) = state.sender.as_ref() else {
            return false;
        };
        match sender.try_send(event) {
            Ok(()) => state.mark_delivered(id),
            Err(mpsc::error::TrySendError::Full(event)) => {
                if state.pending.len() == state.maximum_pending {
                    state.status = SubscriptionDeliveryStatus::Lagged;
                    return false;
                }
                state.pending.push_back(event);
                state.pending_ids.insert(id);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                state.close();
                return false;
            }
        }
        true
    }

    pub(super) fn refill(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.refill();
        }
    }

    pub(super) fn close(&self) {
        if let Ok(mut state) = self.state.lock() {
            state.close();
        }
    }
}

impl DeliveryState {
    fn mark_delivered(&mut self, id: String) {
        self.delivered_ids.insert(id.clone());
        self.delivered_order.push_back(id);
        while self.delivered_order.len() > FIPS_NOSTR_PUBSUB_MAX_SEEN_EVENT_IDS {
            if let Some(removed) = self.delivered_order.pop_front() {
                self.delivered_ids.remove(&removed);
            }
        }
    }

    fn refill(&mut self) {
        while let Some(event) = self.pending.pop_front() {
            let id = event.event.as_event().id.to_string();
            let Some(sender) = self.sender.as_ref() else {
                self.pending.push_front(event);
                break;
            };
            match sender.try_send(event) {
                Ok(()) => {
                    self.pending_ids.remove(&id);
                    self.mark_delivered(id);
                }
                Err(mpsc::error::TrySendError::Full(event)) => {
                    self.pending.push_front(event);
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    // The application dropped its receiver; there is no
                    // remaining consumer to which these bodies can be admitted.
                    self.pending.clear();
                    self.pending_ids.clear();
                    self.close();
                    break;
                }
            }
        }
        if self.status != SubscriptionDeliveryStatus::Active && self.pending.is_empty() {
            self.sender.take();
        }
    }

    fn close(&mut self) {
        if self.status == SubscriptionDeliveryStatus::Active {
            self.status = SubscriptionDeliveryStatus::Closed;
        }
        if self.pending.is_empty() {
            self.sender.take();
        }
    }
}
