#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerSelectionMode {
    Neutral,
    LocalBehavior,
    SharedReputation,
}

impl PeerSelectionMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Neutral => "neutral",
            Self::LocalBehavior => "local-behavior",
            Self::SharedReputation => "shared-reputation",
        }
    }
}
