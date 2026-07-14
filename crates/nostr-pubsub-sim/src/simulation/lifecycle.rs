use std::sync::{Arc, RwLock};

use nostr::{Event, EventBuilder, Keys, Kind, PublicKey, Tag, Timestamp};
use nostr_pubsub::{EventSource, FipsPubsubWireMessage, VerifiedEvent};
use nostr_social_graph::{NostrEvent, SocialGraph};

use crate::topology::NodeRole;
use crate::workload::SubscriptionClass;

use super::{
    PeerSelectionMode, Result, SIM_UNIX_BASE, Simulation, SimulationError, SubscriptionPurpose,
    SubscriptionStore, TrafficProvenance, policy_drops, profile_subscription_id, pubsub_error,
};

#[derive(Debug, Clone, Copy)]
enum HumanLifecycleTransition {
    FollowAdmission,
    FollowRemoval,
    StaleUpdateRejection,
    FollowReadmission,
    MuteRemoval,
}

struct HumanLifecycleFixture {
    good_author: String,
    good_event: VerifiedEvent,
    bad_author: String,
    bad_event: VerifiedEvent,
}

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

    pub(super) fn exercise_human_lifecycle(&mut self) -> Result<()> {
        if self.mode != PeerSelectionMode::SharedReputation {
            return Ok(());
        }
        let Some(fixture) = self.human_lifecycle_fixture() else {
            return Ok(());
        };
        let probe_nodes = (self.config.attacker_count..self.config.node_count)
            .filter(|node| {
                self.peer_ids[*node] != fixture.good_author
                    && self.peer_ids[*node] != fixture.bad_author
            })
            .take(16)
            .collect::<Vec<_>>();
        for node in probe_nodes {
            self.exercise_human_probe(node, &fixture)?;
        }
        Ok(())
    }

    fn human_lifecycle_fixture(&self) -> Option<HumanLifecycleFixture> {
        let pair = self
            .workload_pairs
            .iter()
            .find(|pair| pair.class == SubscriptionClass::AuthorFeed)?;
        let good_metadata = self
            .events
            .get(&pair.legitimate_event_id)
            .expect("workload pair legitimate event must exist");
        let bad_metadata = pair
            .spam_event_id
            .as_ref()
            .and_then(|event_id| self.events.get(event_id))
            .or_else(|| {
                self.workload_pairs
                    .iter()
                    .filter_map(|candidate| self.events.get(&candidate.legitimate_event_id))
                    .find(|metadata| metadata.event.pubkey != good_metadata.event.pubkey)
            })?;
        Some(HumanLifecycleFixture {
            good_author: good_metadata.event.pubkey.to_hex(),
            good_event: good_metadata.verified.clone(),
            bad_author: bad_metadata.event.pubkey.to_hex(),
            bad_event: bad_metadata.verified.clone(),
        })
    }

    fn exercise_human_probe(&mut self, node: usize, fixture: &HumanLifecycleFixture) -> Result<()> {
        let root = self.peer_ids[node].clone();
        let graph = self.nodes[node].human_policy.graph();
        let original = clone_graph(&graph)?;
        replace_graph(&graph, SocialGraph::new(&root))?;

        self.ingest_signed_graph_update(
            node,
            &graph,
            Kind::ContactList,
            SIM_UNIX_BASE,
            [fixture.good_author.as_str()],
        )?;
        self.check_human_policy(
            node,
            &root,
            &fixture.good_event,
            false,
            HumanLifecycleTransition::FollowAdmission,
        )?;
        self.ingest_signed_graph_update(
            node,
            &graph,
            Kind::ContactList,
            SIM_UNIX_BASE.saturating_add(2),
            std::iter::empty::<&str>(),
        )?;
        self.check_human_policy(
            node,
            &root,
            &fixture.good_event,
            true,
            HumanLifecycleTransition::FollowRemoval,
        )?;
        self.ingest_signed_graph_update(
            node,
            &graph,
            Kind::ContactList,
            SIM_UNIX_BASE.saturating_add(1),
            [fixture.good_author.as_str()],
        )?;
        self.check_human_policy(
            node,
            &root,
            &fixture.good_event,
            true,
            HumanLifecycleTransition::StaleUpdateRejection,
        )?;
        self.ingest_signed_graph_update(
            node,
            &graph,
            Kind::ContactList,
            SIM_UNIX_BASE.saturating_add(3),
            [fixture.good_author.as_str()],
        )?;
        self.check_human_policy(
            node,
            &root,
            &fixture.good_event,
            false,
            HumanLifecycleTransition::FollowReadmission,
        )?;
        self.ingest_signed_graph_update(
            node,
            &graph,
            Kind::MuteList,
            SIM_UNIX_BASE.saturating_add(4),
            [fixture.bad_author.as_str()],
        )?;
        self.check_human_policy(
            node,
            &root,
            &fixture.bad_event,
            true,
            HumanLifecycleTransition::MuteRemoval,
        )?;
        replace_graph(&graph, original)
    }

    fn ingest_signed_graph_update<'a>(
        &mut self,
        node: usize,
        graph: &Arc<RwLock<SocialGraph>>,
        kind: Kind,
        created_at: u64,
        tagged: impl IntoIterator<Item = &'a str>,
    ) -> Result<()> {
        let event = signed_graph_event(&self.keys[node], kind, created_at, tagged)?;
        update_graph(graph, &graph_event_from_nostr(&event))?;
        self.report.human_signed_graph_updates_ingested = self
            .report
            .human_signed_graph_updates_ingested
            .saturating_add(1);
        Ok(())
    }

    fn check_human_policy(
        &mut self,
        node: usize,
        root: &str,
        event: &VerifiedEvent,
        expected_drop: bool,
        transition: HumanLifecycleTransition,
    ) -> Result<()> {
        let dropped = policy_drops(
            &self.nodes[node].human_policy,
            event,
            &EventSource::peer(root),
        )?;
        self.record_human_check(transition, dropped == expected_drop);
        Ok(())
    }

    fn record_human_check(&mut self, transition: HumanLifecycleTransition, success: bool) {
        self.report.human_lifecycle_checks = self.report.human_lifecycle_checks.saturating_add(1);
        self.report.human_lifecycle_successes = self
            .report
            .human_lifecycle_successes
            .saturating_add(usize::from(success));
        if !success {
            return;
        }
        let counter = match transition {
            HumanLifecycleTransition::FollowAdmission => &mut self.report.human_follow_admissions,
            HumanLifecycleTransition::FollowRemoval => &mut self.report.human_follow_removals,
            HumanLifecycleTransition::StaleUpdateRejection => {
                &mut self.report.human_stale_update_rejections
            }
            HumanLifecycleTransition::FollowReadmission => {
                &mut self.report.human_follow_readmissions
            }
            HumanLifecycleTransition::MuteRemoval => &mut self.report.human_mute_removals,
        };
        *counter = counter.saturating_add(1);
    }
}

fn signed_graph_event<'a>(
    keys: &Keys,
    kind: Kind,
    created_at: u64,
    tagged: impl IntoIterator<Item = &'a str>,
) -> Result<Event> {
    let tags = tagged
        .into_iter()
        .map(|pubkey| {
            PublicKey::parse(pubkey)
                .map(Tag::public_key)
                .map_err(pubsub_error)
        })
        .collect::<Result<Vec<_>>>()?;
    let event = EventBuilder::new(kind, "")
        .tags(tags)
        .custom_created_at(Timestamp::from(created_at))
        .sign_with_keys(keys)
        .map_err(pubsub_error)?;
    event.verify().map_err(pubsub_error)?;
    Ok(event)
}

fn graph_event_from_nostr(event: &Event) -> NostrEvent {
    NostrEvent {
        created_at: event.created_at.as_secs(),
        content: event.content.clone(),
        tags: event
            .tags
            .iter()
            .map(|tag| tag.as_slice().to_vec())
            .collect(),
        kind: u32::from(event.kind.as_u16()),
        pubkey: event.pubkey.to_hex(),
        id: event.id.to_hex(),
        sig: event.sig.to_string(),
    }
}

fn clone_graph(graph: &Arc<RwLock<SocialGraph>>) -> Result<SocialGraph> {
    graph.read().map(|graph| graph.clone()).map_err(graph_error)
}

fn replace_graph(graph: &Arc<RwLock<SocialGraph>>, replacement: SocialGraph) -> Result<()> {
    graph
        .write()
        .map(|mut graph| *graph = replacement)
        .map_err(graph_error)
}

fn update_graph(graph: &Arc<RwLock<SocialGraph>>, event: &NostrEvent) -> Result<()> {
    graph
        .write()
        .map(|mut graph| graph.handle_event(event, true, 1.0))
        .map_err(graph_error)
}

fn graph_error<T>(_: T) -> SimulationError {
    SimulationError::Pubsub("social graph lock poisoned".to_string())
}
