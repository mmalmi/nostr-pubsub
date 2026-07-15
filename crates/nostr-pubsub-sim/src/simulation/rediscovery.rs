use std::collections::BTreeSet;

use super::{
    NodeRole, PubsubPeerInterest, Result, ScheduledAction, Simulation, SourceId, TopologyStrategy,
    mix64,
};

const CANDIDATE_ORDER_DOMAIN: u64 = 0x5254_4449_5343_4f56;
const MAX_BOOTSTRAP_REDISCOVERY_SWEEPS: usize = 12;
pub(super) const MAX_REPLACEMENTS_PER_SWEEP: usize = 2;
pub(super) const MAX_CANDIDATE_ATTEMPTS_PER_SWEEP: usize = 64;

#[derive(Debug, Default)]
pub(super) struct RediscoveryState {
    target_links: usize,
    next_candidate: usize,
    retired_candidates: BTreeSet<usize>,
}

impl RediscoveryState {
    pub(super) fn new(target_links: usize) -> Self {
        Self {
            target_links,
            ..Self::default()
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RemovalReason {
    Unavailable,
    MachineRejected,
    NegativeBehavior(i32),
    NoSubscriptionOverlap,
}

impl RemovalReason {
    const fn order_key(self) -> (u8, i32) {
        match self {
            Self::Unavailable => (0, i32::MIN),
            Self::MachineRejected => (1, i32::MIN),
            Self::NegativeBehavior(score) => (2, score),
            Self::NoSubscriptionOverlap => (3, i32::MIN),
        }
    }
}

impl Simulation {
    pub(super) fn run_bootstrap_rediscovery(&mut self) -> Result<()> {
        if self.config.topology != TopologyStrategy::HybridSupernodes {
            return Ok(());
        }
        for _ in 0..MAX_BOOTSTRAP_REDISCOVERY_SWEEPS {
            let removed_before = self.report.rediscovery_links_removed;
            self.scheduler
                .schedule_after(0, ScheduledAction::RediscoverySweep);
            self.drain_scheduler()?;
            if self.report.rediscovery_links_removed == removed_before {
                break;
            }
        }
        Ok(())
    }

    pub(super) fn schedule_rediscovery_sweep(&mut self, phase_start_ms: u64, delay_ms: u64) {
        if self.config.topology == TopologyStrategy::HybridSupernodes {
            self.scheduler.schedule_at(
                phase_start_ms.saturating_add(delay_ms),
                ScheduledAction::RediscoverySweep,
            );
        }
    }

    pub(super) fn run_rediscovery_sweep(&mut self) -> Result<()> {
        self.report.rediscovery_sweeps = self.report.rediscovery_sweeps.saturating_add(1);
        let now_ms = self.scheduler.now_ms();
        for source in self.config.attacker_count..self.config.node_count {
            if self.rediscovery[source].target_links == 0 {
                continue;
            }
            self.nodes[source].mesh.maintain(now_ms);
            self.observe_core_resource_state(source);
            let bad_links = self.bad_outbound_discovery_links(source)?;
            for (subject, reason) in bad_links.into_iter().take(MAX_REPLACEMENTS_PER_SWEEP) {
                self.remove_outbound_discovery_link(source, subject, reason)?;
            }
            self.refill_outbound_discovery_links(source)?;
        }
        self.flush_rediscovery_subscriptions()?;
        self.report.rediscovery_state_entries = self.rediscovery_state_entries();
        Ok(())
    }

    fn bad_outbound_discovery_links(
        &mut self,
        source: usize,
    ) -> Result<Vec<(usize, RemovalReason)>> {
        let mut bad = Vec::new();
        for subject in self.topology.outbound_discovery_neighbors[source].clone() {
            let reason = if !self.link_is_active(source, subject) {
                Some(RemovalReason::Unavailable)
            } else if self.candidate_peer(source, subject)?.is_none() {
                Some(RemovalReason::MachineRejected)
            } else {
                let behavior = self.nodes[source]
                    .mesh
                    .peer_behavior_observation(&self.peer_ids[subject])
                    .filter(|observation| {
                        observation.invalid_messages >= 3
                            || (observation.unserved_inventories >= 6
                                && observation.valid_frames == 0)
                    })
                    .map(|observation| RemovalReason::NegativeBehavior(observation.score));
                behavior.or_else(|| {
                    (!self.has_subscription_overlap(source, subject))
                        .then_some(RemovalReason::NoSubscriptionOverlap)
                })
            };
            if let Some(reason) = reason {
                bad.push((subject, reason));
            }
        }
        bad.sort_by_key(|(subject, reason)| (reason.order_key(), *subject));
        Ok(bad)
    }

    fn has_subscription_overlap(&self, source: usize, subject: usize) -> bool {
        let subject = SourceId::new(&self.peer_ids[subject]);
        let Some(primary_filter) = self.nodes[source].filters.first() else {
            return false;
        };
        self.routing_probes.iter().any(|event| {
            PubsubPeerInterest::from_filters(std::slice::from_ref(primary_filter), event)
                == PubsubPeerInterest::Subscribed
                && self.nodes[source]
                    .wire
                    .subscriptions()
                    .peer_interest(&subject, event)
                    == PubsubPeerInterest::Subscribed
        })
    }

    fn remove_outbound_discovery_link(
        &mut self,
        source: usize,
        subject: usize,
        reason: RemovalReason,
    ) -> Result<()> {
        if !remove_sorted(
            &mut self.topology.outbound_discovery_neighbors[source],
            subject,
        ) {
            return Ok(());
        }
        remove_sorted(&mut self.topology.neighbors[source], subject);
        remove_sorted(&mut self.topology.neighbors[subject], source);
        self.rediscovery[source].retired_candidates.insert(subject);
        let subject_id = SourceId::new(&self.peer_ids[subject]);
        let source_id = SourceId::new(&self.peer_ids[source]);
        self.nodes[source].wire.disconnect_peer(&subject_id);
        self.nodes[subject].wire.disconnect_peer(&source_id);
        self.mark_rating_subscription_dirty(source);
        self.mark_rating_subscription_dirty(subject);
        self.observe_subscription_resource_state(source)?;
        self.observe_subscription_resource_state(subject)?;
        self.retry_counts.retain(|(left, right, _), _| {
            !((*left == source && *right == subject) || (*left == subject && *right == source))
        });
        self.report.rediscovery_links_removed =
            self.report.rediscovery_links_removed.saturating_add(1);
        if reason == RemovalReason::Unavailable {
            self.report.rediscovery_unavailable_links_removed = self
                .report
                .rediscovery_unavailable_links_removed
                .saturating_add(1);
        }
        if self.topology.roles[subject] == NodeRole::Attacker {
            self.report.rediscovery_adversarial_links_removed = self
                .report
                .rediscovery_adversarial_links_removed
                .saturating_add(1);
        }
        Ok(())
    }

    fn refill_outbound_discovery_links(&mut self, source: usize) -> Result<()> {
        let target = self.rediscovery[source].target_links;
        let missing =
            target.saturating_sub(self.topology.outbound_discovery_neighbors[source].len());
        let mut remaining = missing.min(MAX_REPLACEMENTS_PER_SWEEP);
        let mut attempts = 0usize;
        while remaining > 0
            && attempts < MAX_CANDIDATE_ATTEMPTS_PER_SWEEP
            && self.rediscovery[source].next_candidate < self.config.node_count
        {
            let ordinal = self.rediscovery[source].next_candidate;
            self.rediscovery[source].next_candidate = ordinal.saturating_add(1);
            attempts = attempts.saturating_add(1);
            self.report.rediscovery_candidate_attempts =
                self.report.rediscovery_candidate_attempts.saturating_add(1);
            let candidate = candidate_at(self.config.seed, source, ordinal, self.config.node_count);
            if self.rediscovery[source]
                .retired_candidates
                .contains(&candidate)
                || !self.try_add_outbound_discovery_link(source, candidate)?
            {
                continue;
            }
            remaining -= 1;
        }
        Ok(())
    }

    fn try_add_outbound_discovery_link(&mut self, source: usize, candidate: usize) -> Result<bool> {
        if source == candidate
            || self.topology.neighbors[source]
                .binary_search(&candidate)
                .is_ok()
            || self.topology.neighbors[source].len() >= self.endpoint_connection_limits[source]
            || self.topology.neighbors[candidate].len()
                >= self.endpoint_connection_limits[candidate]
            || self.candidate_peer(source, candidate)?.is_none()
        {
            return Ok(false);
        }
        insert_sorted(&mut self.topology.neighbors[source], candidate);
        insert_sorted(&mut self.topology.neighbors[candidate], source);
        insert_sorted(
            &mut self.topology.outbound_discovery_neighbors[source],
            candidate,
        );
        self.mark_rating_subscription_dirty(source);
        self.mark_rating_subscription_dirty(candidate);
        self.rediscovery_new_links
            .insert(super::rating_subscriptions::ordered_link(source, candidate));
        self.report.rediscovery_links_added = self.report.rediscovery_links_added.saturating_add(1);
        if self.topology.roles[candidate] == NodeRole::Supernode {
            self.report.rediscovery_high_capacity_links_added = self
                .report
                .rediscovery_high_capacity_links_added
                .saturating_add(1);
        }
        Ok(true)
    }

    fn rediscovery_state_entries(&self) -> usize {
        self.rediscovery
            .iter()
            .enumerate()
            .filter(|(_, state)| state.target_links > 0)
            .map(|(source, state)| {
                2usize
                    .saturating_add(state.retired_candidates.len())
                    .saturating_add(self.topology.outbound_discovery_neighbors[source].len())
            })
            .sum()
    }
}

fn candidate_at(seed: u64, source: usize, ordinal: usize, node_count: usize) -> usize {
    debug_assert!(node_count > 0);
    if node_count == 1 {
        return 0;
    }
    let source = u64::try_from(source).unwrap_or(u64::MAX);
    let count = u64::try_from(node_count).unwrap_or(u64::MAX);
    let start = mix64(seed ^ CANDIDATE_ORDER_DOMAIN ^ source.rotate_left(17)) % count;
    let mut step =
        1 + mix64(seed ^ CANDIDATE_ORDER_DOMAIN.rotate_left(29) ^ source.rotate_left(41))
            % count.saturating_sub(1);
    while greatest_common_divisor(step, count) != 1 {
        step = step % count.saturating_sub(1) + 1;
    }
    let offset = u64::try_from(ordinal).unwrap_or(u64::MAX) % count;
    usize::try_from((start + offset.saturating_mul(step) % count) % count).unwrap_or(0)
}

const fn greatest_common_divisor(mut left: u64, mut right: u64) -> u64 {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn insert_sorted(values: &mut Vec<usize>, value: usize) {
    if let Err(index) = values.binary_search(&value) {
        values.insert(index, value);
    }
}

fn remove_sorted(values: &mut Vec<usize>, value: usize) -> bool {
    let Ok(index) = values.binary_search(&value) else {
        return false;
    };
    values.remove(index);
    true
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nostr::{JsonUtil, Keys};
    use nostr_pubsub::VerifiedEvent;

    use super::*;
    use crate::{PeerSelectionMode, SimulationConfig, SupernodeDiscoveryStrategy};

    #[test]
    fn candidate_cursor_is_a_role_independent_permutation() {
        let count = 120;
        let candidates = (0..count)
            .map(|ordinal| candidate_at(7, 41, ordinal, count))
            .collect::<BTreeSet<_>>();
        assert_eq!(candidates, (0..count).collect());
    }

    #[test]
    fn non_shared_modes_install_new_links_without_rating_refreshes() {
        for mode in [PeerSelectionMode::Neutral, PeerSelectionMode::LocalBehavior] {
            let report = super::super::run_simulation(
                SimulationConfig {
                    node_count: 64,
                    attacker_count: 12,
                    topology: TopologyStrategy::HybridSupernodes,
                    supernode_count: 4,
                    adversarial_discovery_candidate_count: 3,
                    loss_basis_points: 0,
                    churn_basis_points: 0,
                    signed_spam_rounds: 1,
                    ..SimulationConfig::default()
                },
                mode,
            )
            .unwrap();
            assert!(report.rediscovery_links_added > 0, "{mode:?}");
            assert_eq!(report.rediscovery_subscription_refresh_nodes, 0, "{mode:?}");
            assert_eq!(
                report.rediscovery_subscription_refresh_targets, 0,
                "{mode:?}"
            );
            assert!(
                report.rediscovery_subscription_messages
                    >= report.rediscovery_links_added.saturating_mul(2),
                "{mode:?}"
            );
        }
    }

    #[test]
    fn observed_decoys_are_replaced_with_bounded_state_and_traffic() {
        let config = SimulationConfig {
            node_count: 120,
            attacker_count: 24,
            topology: TopologyStrategy::HybridSupernodes,
            supernode_discovery: SupernodeDiscoveryStrategy::Bootstrap,
            supernode_count: 8,
            adversarial_discovery_candidate_count: 6,
            loss_basis_points: 0,
            churn_basis_points: 0,
            signed_spam_rounds: 2,
            ..SimulationConfig::default()
        };
        let report = super::super::run_simulation(config, PeerSelectionMode::SharedReputation)
            .expect("rediscovery simulation");
        let client_count = report
            .honest_node_count
            .saturating_sub(report.supernode_count);

        assert!(report.rediscovery_sweeps >= 4);
        assert!(report.rediscovery_sweeps <= MAX_BOOTSTRAP_REDISCOVERY_SWEEPS.saturating_add(4));
        assert!(report.rediscovery_adversarial_links_removed > 0);
        assert!(report.rediscovery_links_added > 0);
        assert!(report.rediscovery_subscription_refresh_nodes > 0);
        assert!(report.rediscovery_subscription_refresh_targets > 0);
        let peer_degree_cap = report
            .config
            .supernode_links_per_peer
            .saturating_mul(4)
            .saturating_add(4)
            .max(8);
        assert!(
            report.rediscovery_subscription_refresh_nodes
                <= client_count.saturating_mul(report.rediscovery_sweeps)
        );
        assert!(
            report.rediscovery_subscription_refresh_targets
                <= report
                    .rediscovery_subscription_refresh_nodes
                    .saturating_mul(peer_degree_cap)
        );
        assert!(report.rediscovery_subscription_messages > 0);
        assert!(report.rediscovery_control_plane_wire_bytes > 0);
        assert!(
            report.rediscovery_candidate_attempts
                <= client_count
                    .saturating_mul(report.rediscovery_sweeps)
                    .saturating_mul(MAX_CANDIDATE_ATTEMPTS_PER_SWEEP)
        );
        assert!(
            report.rediscovery_state_entries
                <= client_count.saturating_mul(
                    2 + report.config.supernode_links_per_peer
                        + report.rediscovery_sweeps * MAX_REPLACEMENTS_PER_SWEEP,
                )
        );
        assert!(
            report.rediscovery_subscription_messages
                <= report
                    .rediscovery_subscription_refresh_targets
                    .saturating_add(report.rediscovery_links_added.saturating_mul(2))
                    .saturating_mul(usize::from(report.config.max_retries) + 1)
        );
        assert!(
            report.rediscovery_subscription_messages
                >= report
                    .rediscovery_subscription_refresh_targets
                    .saturating_add(report.rediscovery_links_added.saturating_mul(2))
        );
    }

    #[test]
    fn active_churn_link_is_not_resurrected_after_replacement() {
        let mut simulation = Simulation::new(
            SimulationConfig {
                node_count: 96,
                attacker_count: 0,
                topology: TopologyStrategy::HybridSupernodes,
                supernode_discovery: SupernodeDiscoveryStrategy::Bootstrap,
                supernode_count: 6,
                adversarial_discovery_candidate_count: 0,
                loss_basis_points: 0,
                churn_basis_points: 0,
                ..SimulationConfig::default()
            },
            PeerSelectionMode::SharedReputation,
        )
        .expect("churn rediscovery simulation");
        simulation.install_subscriptions().unwrap();
        simulation.drain_scheduler().unwrap();
        assert_eq!(
            retained_subscriptions(&simulation),
            simulation.topology.edge_count() * 2
        );
        let source = (simulation.config.attacker_count..simulation.config.node_count)
            .find(|source| {
                simulation.topology.roles[*source] == NodeRole::Peer
                    && !simulation.topology.outbound_discovery_neighbors[*source].is_empty()
            })
            .unwrap();
        let subject = simulation.topology.outbound_discovery_neighbors[source][0];
        let old_neighbors = simulation.topology.neighbors[source].clone();
        let old_rating_filters = simulation.nodes[source].rating_filters.clone();
        let outage =
            super::super::LinkOutage::new(source, subject, super::super::OutageCause::Stochastic);
        simulation.begin_link_outage(outage);

        simulation.run_rediscovery_sweep().unwrap();
        assert!(!simulation.topology.neighbors[source].contains(&subject));
        let replacement = simulation.topology.neighbors[source]
            .iter()
            .copied()
            .find(|peer| !old_neighbors.contains(peer))
            .expect("removed link was refilled");
        assert_ne!(simulation.nodes[source].rating_filters, old_rating_filters);
        simulation
            .scheduler
            .schedule_after(1, ScheduledAction::LinkUp(outage));
        simulation.drain_scheduler().unwrap();

        assert!(!simulation.topology.neighbors[source].contains(&subject));
        assert_eq!(simulation.report.rediscovery_unavailable_links_removed, 1);
        assert!(simulation.report.rediscovery_links_added > 0);
        assert_eq!(
            simulation.report.rediscovery_subscription_messages,
            simulation
                .report
                .rediscovery_subscription_refresh_targets
                .saturating_add(simulation.report.rediscovery_links_added.saturating_mul(2))
        );
        assert_refreshed_rating_subscriptions(&simulation, source, subject, replacement);
    }

    fn assert_refreshed_rating_subscriptions(
        simulation: &Simulation,
        source: usize,
        retired: usize,
        replacement: usize,
    ) {
        let signer = Keys::generate();
        let rater = signer.public_key().to_hex();
        let replacement_rating = VerifiedEvent::try_from(
            super::super::peer_rating_event(
                &signer,
                &rater,
                &simulation.peer_ids[replacement],
                100,
                super::super::SIM_UNIX_BASE,
            )
            .unwrap(),
        )
        .unwrap();
        let retired_rating = VerifiedEvent::try_from(
            super::super::peer_rating_event(
                &signer,
                &rater,
                &simulation.peer_ids[retired],
                100,
                super::super::SIM_UNIX_BASE,
            )
            .unwrap(),
        )
        .unwrap();
        assert_filter_interest(
            &simulation.nodes[source].rating_filters,
            &replacement_rating,
            &retired_rating,
        );
        let source_id = SourceId::new(&simulation.peer_ids[source]);
        for provider in &simulation.topology.neighbors[source] {
            let subscriptions = simulation.nodes[*provider].wire.subscriptions();
            assert_eq!(subscriptions.peer_subscription_count(&source_id), 1);
            assert_eq!(
                subscriptions.peer_interest(&source_id, &replacement_rating),
                PubsubPeerInterest::Subscribed
            );
            assert_eq!(
                subscriptions.peer_interest(&source_id, &retired_rating),
                PubsubPeerInterest::Unsubscribed
            );
        }
        let expected_filter_bytes = simulation.nodes[source]
            .filters
            .iter()
            .chain(simulation.nodes[source].rating_filters.iter())
            .map(|filter| u64::try_from(filter.try_as_json().unwrap().len()).unwrap())
            .sum::<u64>();
        assert_eq!(
            simulation.node_resources[source].current.local_filter_bytes,
            expected_filter_bytes
        );
        assert_eq!(
            retained_subscriptions(simulation),
            simulation.topology.edge_count() * 2,
            "missing={:?} rejections={} evictions={}",
            missing_profile_links(simulation),
            simulation.report.subscription_rejections,
            simulation.report.subscription_evictions,
        );
        assert_eq!(
            simulation.nodes[source]
                .wire
                .subscriptions()
                .peer_subscription_count(&SourceId::new(&simulation.peer_ids[retired])),
            0
        );
        assert_eq!(
            simulation.nodes[retired]
                .wire
                .subscriptions()
                .peer_subscription_count(&source_id),
            0
        );
    }

    fn retained_subscriptions(simulation: &Simulation) -> usize {
        simulation
            .nodes
            .iter()
            .map(|node| node.wire.subscriptions().subscription_count())
            .sum()
    }

    fn missing_profile_links(simulation: &Simulation) -> Vec<(usize, usize)> {
        simulation
            .topology
            .neighbors
            .iter()
            .enumerate()
            .flat_map(|(provider, subscribers)| {
                subscribers.iter().copied().filter_map(move |subscriber| {
                    (simulation.nodes[provider]
                        .wire
                        .subscriptions()
                        .peer_subscription_count(&SourceId::new(&simulation.peer_ids[subscriber]))
                        == 0)
                        .then_some((subscriber, provider))
                })
            })
            .collect()
    }

    fn assert_filter_interest(
        filters: &[nostr::Filter],
        replacement: &VerifiedEvent,
        retired: &VerifiedEvent,
    ) {
        assert_eq!(
            PubsubPeerInterest::from_filters(filters, replacement),
            PubsubPeerInterest::Subscribed
        );
        assert_eq!(
            PubsubPeerInterest::from_filters(filters, retired),
            PubsubPeerInterest::Unsubscribed
        );
    }
}
