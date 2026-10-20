use super::{
    ClientInner, EventSource, Ordering, PeerIdentity, SourceId, SubscriptionId, VerifiedEvent,
};
use crate::provider_behavior::ProviderViolation;

impl ClientInner {
    pub(super) async fn handle_received_event(
        &self,
        source_peer: PeerIdentity,
        subscription_id: Option<SubscriptionId>,
        event: VerifiedEvent,
    ) {
        let source_npub = source_peer.npub();
        let source_id = SourceId::new(source_npub.clone());
        self.event_frames_received.fetch_add(1, Ordering::Relaxed);
        if subscription_id.is_none() && self.options.accept_anonymous_publications {
            self.accept_publication(source_peer, event).await;
            return;
        }
        let is_subscribed =
            self.event_matches_subscription(&source_npub, subscription_id.as_ref(), &event);
        let fresh_id = if is_subscribed && subscription_id.is_some() {
            self.fresh_response_subscription(&source_npub, &event)
        } else {
            None
        };
        let response_id = fresh_id.as_ref().or(subscription_id.as_ref());
        let subscription_key = response_id.map(ToString::to_string).unwrap_or_default();
        let event_id = event.as_event().id.to_string();
        let observation = self.observed_full_events.lock().ok().map(|mut seen| {
            seen.observe_response(
                &source_npub,
                subscription_id.as_ref().map(SubscriptionId::as_str),
                &subscription_key,
                &event_id,
                is_subscribed,
            )
        });
        let Some(observation) = observation else {
            return;
        };
        if !is_subscribed {
            // A response to a retried WANT may already be in flight at
            // local CLOSE. The exact previously matched observation is
            // harmless; new unsolicited IDs still count as violations.
            if !observation.previously_matched {
                self.record_provider_violation(
                    source_peer,
                    ProviderViolation::OutOfFilterEvent {
                        repeated: !observation.first,
                    },
                );
            }
            return;
        }
        if !observation.first {
            return;
        }
        let source = EventSource::fips_endpoint(&source_npub);
        if !self.event_is_admitted(&event, &source).await {
            return;
        }
        self.handle_event(&source_npub, &source_id, response_id, &event);
    }
}
