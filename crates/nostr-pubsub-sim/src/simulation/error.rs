#[derive(Debug, thiserror::Error)]
pub enum SimulationError {
    #[error("invalid simulation configuration: {0}")]
    InvalidConfig(String),
    #[error("pubsub simulation failed: {0}")]
    Pubsub(String),
    #[error("simulation exceeded its {0} scheduled-action processing budget")]
    ActionBudgetExceeded(usize),
}
