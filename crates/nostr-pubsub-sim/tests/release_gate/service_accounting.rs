use nostr_pubsub_sim::{NodeRole, SimulationReport};

use super::{report_context, role_service_bytes};

pub(super) fn assert_service_accounting_is_populated(report: &SimulationReport, case: &str) {
    let context = report_context(report, case);
    assert_eq!(report.node_roles.len(), report.node_count, "{context}");
    assert_protocol_accounting(report, &context);
    assert_final_delivery_accounting(report, &context);
    assert_verified_delivery_accounting(report, &context);
    assert!(role_service_bytes(report, NodeRole::Peer) > 0, "{context}");
}

fn assert_protocol_accounting(report: &SimulationReport, context: &str) {
    assert!(report.total_protocol_bytes > 0, "{context}");
    assert!(
        report.protocol_bytes_per_interested_delivery > 0,
        "{context}"
    );
    assert!(report.protocol_accounting_is_conserved(), "{context}");
    assert_eq!(
        report
            .data_plane_wire_bytes
            .saturating_add(report.control_plane_wire_bytes),
        report.total_protocol_bytes,
        "data and control planes must conserve bytes: {context}"
    );
    assert_eq!(
        report
            .legitimate_protocol_bytes
            .saturating_add(report.adversarial_protocol_bytes),
        report.total_protocol_bytes,
        "workload provenance must conserve bytes: {context}"
    );
    assert_eq!(report.sent_link_protocol_bytes, report.total_protocol_bytes);
    assert_eq!(report.sent_role_protocol_bytes, report.total_protocol_bytes);
    assert!(!report.protocol_service_by_link.is_empty(), "{context}");
}

fn assert_final_delivery_accounting(report: &SimulationReport, context: &str) {
    let link_credits = report
        .interested_delivery_credit_by_link
        .values()
        .sum::<usize>();
    assert!(link_credits > 0, "{context}");
    assert_eq!(
        link_credits,
        report
            .delivered_legitimate
            .saturating_sub(report.local_legitimate_deliveries),
        "final-hop credit must equal remote interested delivery: {context}"
    );
    assert_eq!(
        report.delivery_path_samples, link_credits,
        "every remote interested delivery must have a path: {context}"
    );
    assert!(
        report.multihop_interested_deliveries > 0,
        "scenario must exercise useful multihop propagation: {context}"
    );
    assert!(report.delivery_path_hops_max > 1, "{context}");
    assert_eq!(
        report
            .interested_delivery_credit_by_source_role
            .values()
            .sum::<usize>(),
        link_credits,
        "role and link delivery credits must conserve: {context}"
    );
    let link_bytes = report
        .interested_delivery_bytes_by_link
        .values()
        .sum::<u64>();
    assert!(link_bytes > 0, "{context}");
    assert_eq!(
        report
            .interested_delivery_bytes_by_source_role
            .values()
            .sum::<u64>(),
        link_bytes,
        "role and link useful-byte credits must conserve: {context}"
    );
    assert!(
        report
            .interested_delivery_bytes_by_link
            .values()
            .all(|bytes| *bytes > 0),
        "every credited interested delivery link must carry useful bytes: {context}"
    );
}

fn assert_verified_delivery_accounting(report: &SimulationReport, context: &str) {
    let verified_credits = report
        .verified_delivery_credit_by_link
        .values()
        .sum::<usize>();
    let final_credits = report
        .interested_delivery_credit_by_link
        .values()
        .sum::<usize>();
    assert!(
        verified_credits >= final_credits,
        "verified hop deliveries must include final interested deliveries: {context}"
    );
    let verified_bytes = report.verified_delivery_bytes_by_link.values().sum::<u64>();
    let final_bytes = report
        .interested_delivery_bytes_by_link
        .values()
        .sum::<u64>();
    assert!(
        verified_bytes >= final_bytes,
        "verified hop bytes must include final interested delivery bytes: {context}"
    );
    assert_eq!(
        report
            .verified_delivery_bytes_by_source_role
            .values()
            .sum::<u64>(),
        verified_bytes,
        "role and link verified-hop bytes must conserve: {context}"
    );
    assert_eq!(
        report.verified_delivery_records.len(),
        verified_credits,
        "per-event records and verified-hop credits must conserve: {context}"
    );
    assert_eq!(
        report
            .verified_delivery_records
            .iter()
            .map(|record| record.payload_bytes)
            .sum::<u64>(),
        verified_bytes,
        "per-event records and verified-hop bytes must conserve: {context}"
    );
    assert_eq!(
        report
            .verified_delivery_records
            .iter()
            .filter(|record| record.final_interested_delivery)
            .count(),
        final_credits,
        "recorded final-requester flags and final credits must conserve: {context}"
    );
}
