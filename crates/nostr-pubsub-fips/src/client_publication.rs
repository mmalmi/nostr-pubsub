use super::{
    EventPolicyContext, EventSource, PolicyDecision, PublishReport, Result,
    SOURCE_PRIORITY_FIPS_ENDPOINT, VerifiedEvent, poisoned, publish_report,
};
use crate::client_inner::ClientInner;

impl ClientInner {
    pub(super) async fn publish(
        &self,
        event: VerifiedEvent,
        source: EventSource,
    ) -> Result<PublishReport> {
        self.role.validate_publication(&event)?;
        drop(self.admit()?);
        let decision = self.event_decision(&event, &source).await?;
        drop(self.admit()?);
        let (accepted, priority, reason) = decision_report(&decision);
        if !accepted {
            return Ok(PublishReport {
                accepted,
                priority,
                reason,
            });
        }

        let subscribed = self.peer_delivery_targets(&event, None)?;
        let _admission = self.admit()?;
        let is_new = self
            .recent_events
            .lock()
            .map_err(|_| poisoned("FIPS recent event cache"))?
            .insert_for_publish(event.clone(), source, self.options.max_hops);
        if !is_new {
            return Ok(PublishReport {
                accepted: true,
                priority,
                reason: Some("event was already published".to_string()),
            });
        }

        if !subscribed.is_empty() {
            let peer_count = subscribed.len();
            let sent = self.send_inventories(subscribed, &event, self.options.max_hops);
            let mut report = publish_report(sent, peer_count, "subscribed FIPS peers")?;
            report.priority = priority;
            if report.reason.is_none() {
                report.reason = reason;
            }
            return Ok(report);
        }

        Ok(PublishReport {
            accepted: true,
            priority,
            reason: Some("cached for live FIPS subscriptions".to_string()),
        })
    }

    pub(super) async fn event_is_admitted(
        &self,
        event: &VerifiedEvent,
        source: &EventSource,
    ) -> bool {
        self.event_decision(event, source)
            .await
            .is_ok_and(|decision| !matches!(decision, PolicyDecision::Drop { .. }))
    }

    pub(super) async fn event_decision(
        &self,
        event: &VerifiedEvent,
        source: &EventSource,
    ) -> Result<PolicyDecision> {
        match &self.event_policy {
            Some(policy) => {
                policy
                    .check_event(EventPolicyContext { event, source })
                    .await
            }
            None => Ok(PolicyDecision::allow_with_priority(
                SOURCE_PRIORITY_FIPS_ENDPOINT,
            )),
        }
    }
}

fn decision_report(decision: &PolicyDecision) -> (bool, i32, Option<String>) {
    match decision {
        PolicyDecision::Allow { priority } => (true, *priority, None),
        PolicyDecision::Throttle { priority, reason } => (true, *priority, Some(reason.clone())),
        PolicyDecision::Drop { reason } => (false, 0, Some(reason.clone())),
    }
}
