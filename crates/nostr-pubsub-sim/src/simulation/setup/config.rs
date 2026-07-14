use super::super::{
    Result, SimulationConfig, SimulationError, SubscriptionClass, TopologyStrategy,
};

pub(super) fn validate_config(config: &SimulationConfig) -> Result<()> {
    if config.node_count < SubscriptionClass::ALL.len() + config.attacker_count {
        return Err(SimulationError::InvalidConfig(format!(
            "at least {} honest nodes are required for the subscription cohorts",
            SubscriptionClass::ALL.len()
        )));
    }
    if config.attacker_count >= config.node_count {
        return Err(SimulationError::InvalidConfig(
            "attacker_count must leave honest nodes".to_string(),
        ));
    }
    if config.fanout == 0
        || config.supernode_fanout == 0
        || config.max_hops == 0
        || config.max_processed_actions == 0
        || config.retry_delay_ms == 0
    {
        return Err(SimulationError::InvalidConfig(
            "fanout, supernode_fanout, max_hops, retry_delay_ms and action budget must be non-zero"
                .to_string(),
        ));
    }
    if config.unknown_peer_reserve > config.fanout {
        return Err(SimulationError::InvalidConfig(
            "unknown peer reserve cannot exceed fanout".to_string(),
        ));
    }
    if config.loss_basis_points > 10_000 || config.churn_basis_points > 10_000 {
        return Err(SimulationError::InvalidConfig(
            "loss and churn basis points cannot exceed 10000".to_string(),
        ));
    }
    if config.topology == TopologyStrategy::HybridSupernodes
        && (config.supernode_count == 0
            || config.supernode_count >= config.node_count - config.attacker_count)
    {
        return Err(SimulationError::InvalidConfig(
            "hybrid topology requires some, but not all, honest nodes to be supernodes".to_string(),
        ));
    }
    Ok(())
}
