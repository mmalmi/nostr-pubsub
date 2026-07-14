use nostr_pubsub::FipsPubsubWireMessage;

use crate::topology::NodeRole;

use super::{
    Result, Simulation, SubscriptionPurpose, SubscriptionStore, TrafficProvenance,
    profile_subscription_id,
};

impl Simulation {
    pub(super) fn exercise_subscription_lifecycle(&mut self) -> Result<()> {
        let probes = (self.config.attacker_count..self.config.node_count)
            .filter(|node| self.topology.roles[*node] == NodeRole::Peer)
            .take(24)
            .collect::<Vec<_>>();
        for subscriber in probes {
            let Some(provider) = self.topology.neighbors[subscriber]
                .iter()
                .copied()
                .find(|peer| *peer >= self.config.attacker_count)
            else {
                continue;
            };
            self.schedule_subscription_message(
                subscriber,
                provider,
                &FipsPubsubWireMessage::close(profile_subscription_id(subscriber)),
                SubscriptionStore::Ordinary,
                SubscriptionPurpose::LifecycleClose,
                TrafficProvenance::Legitimate,
            )?;
        }
        Ok(())
    }

    pub(in crate::simulation) fn schedule_lifecycle_reopen(
        &mut self,
        provider: usize,
        subscriber: usize,
        observed_close: bool,
    ) -> Result<()> {
        let filters = self.nodes[subscriber].filters.clone();
        self.schedule_subscription_message(
            subscriber,
            provider,
            &FipsPubsubWireMessage::req(profile_subscription_id(subscriber), filters),
            SubscriptionStore::Ordinary,
            SubscriptionPurpose::LifecycleReopen { observed_close },
            TrafficProvenance::Legitimate,
        )
    }
}
