mod clock;
mod metrics;
mod simulation;
mod topology;
mod workload;

pub use clock::VirtualScheduler;
pub use metrics::{
    DistributionSummary, LatencySummary, LoadSummary, NodeTrafficLedger, TrafficCounter,
    TrafficDirection, TrafficProvenance, TrafficScope, basis_points, gini_basis_points,
    summarize_distribution, summarize_latencies, summarize_load,
};
pub use simulation::{
    CpuWorkDistribution, DirectedServiceLink, NodeCpuWork, NodeRetainedUsage, PeerSelectionMode,
    ResourceCohortReport, Result, RetainedUsageDistribution, SimulationConfig, SimulationError,
    SimulationReport, SimulationResourceReport, run_simulation,
};
pub use topology::{
    DiscoverySelectionCounts, HybridSupernodeConfig, NodeRole, PeerMeshConfig,
    SupernodeDiscoveryStrategy, TopologyConfig, TopologyError, TopologyResult, TopologyStrategy,
    build_topology,
};
pub use workload::{
    SubscriptionClass, SubscriptionWorkload, WorkloadResult, build_author_feed, build_fips_advert,
    build_fips_paid_offer, build_git_repo_announcement, build_hashtag_topic, build_hashtree_update,
    build_iris_drive_broad_root, build_targeted_approval_rating,
};
