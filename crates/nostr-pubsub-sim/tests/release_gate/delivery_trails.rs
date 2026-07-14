use std::collections::{BTreeMap, BTreeSet};

use nostr_pubsub_sim::{
    DirectedServiceLink, PeerSelectionMode, SimulationConfig, VerifiedDeliveryRecord,
    run_simulation,
};

#[test]
fn useful_delivery_trails_are_exact_dissemination_trees() {
    let report = run_simulation(
        SimulationConfig {
            node_count: 96,
            attacker_count: 16,
            fake_inventories_per_attack_link: 3,
            signed_spam_rounds: 3,
            supernode_count: 8,
            false_supernode_count: 4,
            loss_basis_points: 0,
            churn_basis_points: 0,
            ..SimulationConfig::default()
        },
        PeerSelectionMode::Neutral,
    )
    .expect("focused trail simulation must complete");

    assert_eq!(report.delivery_basis_points, 10_000, "{report:?}");
    assert_eq!(
        report.delivery_path_samples,
        report
            .delivered_legitimate
            .saturating_sub(report.local_legitimate_deliveries)
    );
    assert!(report.multihop_interested_deliveries > 0, "{report:?}");
    assert!(report.delivery_path_hops_max > 1, "{report:?}");
    assert!(
        report.spam_delivered > 0,
        "spam exclusion was not exercised"
    );
    assert!(report.rejected_malformed_messages > 0, "{report:?}");
    assert!(report.inventory_messages > 0 && report.want_messages > 0);
    assert!(
        report.frame_messages > report.verified_delivery_records.len(),
        "not every frame may become useful service"
    );

    assert_aggregates_match_records(&report.verified_delivery_records, &report);
    let mut records_by_event = BTreeMap::<&str, Vec<&VerifiedDeliveryRecord>>::new();
    for record in &report.verified_delivery_records {
        records_by_event
            .entry(&record.event_id)
            .or_default()
            .push(record);
    }
    assert_eq!(
        records_by_event.len(),
        report.legitimate_events,
        "100% legitimate delivery plus delivered spam proves spam frames were excluded"
    );
    for (event_id, records) in records_by_event {
        assert_event_tree(event_id, &records, report.node_count);
    }
}

fn assert_aggregates_match_records(
    records: &[VerifiedDeliveryRecord],
    report: &nostr_pubsub_sim::SimulationReport,
) {
    let mut credits = BTreeMap::<DirectedServiceLink, usize>::new();
    let mut bytes = BTreeMap::<DirectedServiceLink, u64>::new();
    let mut interested = BTreeMap::<DirectedServiceLink, usize>::new();
    let mut interested_bytes = BTreeMap::<DirectedServiceLink, u64>::new();
    for record in records {
        *credits.entry(record.link()).or_default() += 1;
        *bytes.entry(record.link()).or_default() += record.payload_bytes;
        if record.final_interested_delivery {
            *interested.entry(record.link()).or_default() += 1;
            *interested_bytes.entry(record.link()).or_default() += record.payload_bytes;
        }
    }
    assert_eq!(credits, report.verified_delivery_credit_by_link);
    assert_eq!(bytes, report.verified_delivery_bytes_by_link);
    assert_eq!(interested, report.interested_delivery_credit_by_link);
    assert_eq!(interested_bytes, report.interested_delivery_bytes_by_link);
}

fn assert_event_tree(event_id: &str, records: &[&VerifiedDeliveryRecord], node_count: usize) {
    let mut parents = BTreeMap::new();
    let mut receivers = BTreeSet::new();
    let mut payload_bytes = None;
    let mut last_accepted_at_ms = 0;
    for record in records {
        assert!(record.provider < node_count && record.receiver < node_count);
        assert_ne!(record.provider, record.receiver);
        assert!(record.payload_bytes > 0);
        assert!(record.accepted_at_ms >= last_accepted_at_ms);
        last_accepted_at_ms = record.accepted_at_ms;
        assert!(
            parents.insert(record.receiver, record.provider).is_none(),
            "duplicate first acceptance for event {event_id} receiver {}",
            record.receiver
        );
        receivers.insert(record.receiver);
        assert_eq!(
            *payload_bytes.get_or_insert(record.payload_bytes),
            record.payload_bytes,
            "payload size changed within event {event_id}"
        );
    }

    let roots = records
        .iter()
        .map(|record| record.provider)
        .filter(|provider| !receivers.contains(provider))
        .collect::<BTreeSet<_>>();
    assert_eq!(
        roots.len(),
        1,
        "event {event_id} must have one publisher root"
    );
    let root = *roots.first().expect("root exists");
    for receiver in receivers {
        let mut cursor = receiver;
        let mut path = BTreeSet::new();
        while let Some(parent) = parents.get(&cursor).copied() {
            assert!(path.insert(cursor), "cycle in event {event_id}");
            cursor = parent;
        }
        assert_eq!(cursor, root, "disconnected event trail for {event_id}");
    }
}
