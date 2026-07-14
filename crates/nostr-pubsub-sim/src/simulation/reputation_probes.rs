use nostr_pubsub::SourceId;

use crate::topology::NodeRole;

use super::reputation_flow::virtual_unix_secs;
use super::{ReputationEventOrigin, Result, Simulation, is_quiet_attacker, peer_rating_event};

impl Simulation {
    pub(super) fn publish_forged_probe(&mut self) -> Result<()> {
        if self.forged_rating_published || self.config.attacker_count == 0 {
            return Ok(());
        }
        let Some(publisher) = (0..self.config.attacker_count).find(|node| {
            is_quiet_attacker(*node)
                && self.topology.neighbors[*node]
                    .iter()
                    .any(|peer| *peer >= self.config.attacker_count)
        }) else {
            return Ok(());
        };
        let Some(rater) = self.topology.neighbors[publisher]
            .iter()
            .copied()
            .find(|peer| *peer >= self.config.attacker_count)
        else {
            return Ok(());
        };
        let subject = (self.config.attacker_count..self.config.node_count)
            .find(|peer| *peer != rater)
            .unwrap_or(rater);
        let event = peer_rating_event(
            &self.keys[publisher],
            &self.peer_ids[rater],
            &self.peer_ids[subject],
            100,
            virtual_unix_secs(self.scheduler.now_ms()),
        )?;
        self.forged_rating_published = true;
        self.report.forged_machine_ratings_published = self
            .report
            .forged_machine_ratings_published
            .saturating_add(1);
        self.publish_reputation_event(
            publisher,
            subject,
            self.scheduler.now_ms(),
            &event,
            ReputationEventOrigin::ForgedProbe,
        )
    }

    pub(super) fn publish_poisoned_probe(&mut self) -> Result<()> {
        if self.poisoned_rating_published {
            return Ok(());
        }
        let Some((publisher, _receiver, subject)) = self.poisoned_probe_plan() else {
            return Ok(());
        };
        let event = peer_rating_event(
            &self.keys[publisher],
            &self.peer_ids[publisher],
            &self.peer_ids[subject],
            0,
            virtual_unix_secs(self.scheduler.now_ms()).saturating_add(1),
        )?;
        self.poisoned_rating_published = true;
        self.report.poisoned_machine_ratings_published = self
            .report
            .poisoned_machine_ratings_published
            .saturating_add(1);
        self.publish_reputation_event(
            publisher,
            subject,
            self.scheduler.now_ms(),
            &event,
            ReputationEventOrigin::PoisonedProbe,
        )
    }

    pub(super) fn poisoned_probe_plan(&self) -> Option<(usize, usize, usize)> {
        let mut best = None;
        for publisher in self.config.attacker_count..self.config.node_count {
            let trusting = self.active_rating_receivers(publisher, true);
            let untrusted = self.active_rating_receivers(publisher, false);
            if trusting.is_empty() || untrusted.is_empty() {
                continue;
            }
            for receiver in trusting.iter().copied() {
                let Some(subject) = self.poisoned_subject(
                    publisher,
                    receiver,
                    trusting.as_slice(),
                    untrusted.as_slice(),
                ) else {
                    continue;
                };
                let interested = trusting.len().saturating_add(
                    untrusted
                        .iter()
                        .filter(|peer| self.topology.neighbors[**peer].contains(&subject))
                        .count(),
                );
                if best
                    .as_ref()
                    .is_none_or(|(score, _, _, _)| interested > *score)
                {
                    best = Some((interested, publisher, receiver, subject));
                }
            }
        }
        best.map(|(_, publisher, receiver, subject)| (publisher, receiver, subject))
    }

    fn active_rating_receivers(&self, publisher: usize, trusted: bool) -> Vec<usize> {
        self.topology.neighbors[publisher]
            .iter()
            .copied()
            .filter(|peer| *peer >= self.config.attacker_count)
            .filter(|peer| {
                self.nodes[*peer]
                    .machine_trusted_raters
                    .contains(&self.peer_ids[publisher])
                    == trusted
            })
            .filter(|peer| {
                self.nodes[publisher]
                    .wire
                    .subscriptions()
                    .peer_subscription_count(&SourceId::new(&self.peer_ids[*peer]))
                    > 0
            })
            .collect()
    }

    fn poisoned_subject(
        &self,
        publisher: usize,
        receiver: usize,
        trusting: &[usize],
        untrusted: &[usize],
    ) -> Option<usize> {
        (self.config.attacker_count..self.config.node_count)
            .filter(|subject| {
                self.topology.roles[*subject] == NodeRole::Peer
                    && *subject != publisher
                    && !trusting.contains(subject)
                    && !self.topology.neighbors[receiver].contains(subject)
                    && untrusted
                        .iter()
                        .any(|peer| self.topology.neighbors[*peer].contains(subject))
            })
            .min_by_key(|subject| {
                (
                    trusting
                        .iter()
                        .filter(|peer| self.topology.neighbors[**peer].contains(subject))
                        .count(),
                    *subject,
                )
            })
    }
}
