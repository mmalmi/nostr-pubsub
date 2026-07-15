use super::Simulation;

pub(super) fn trusted_transport_triangle(simulation: &Simulation) -> Option<(usize, usize, usize)> {
    for publisher in simulation.config.attacker_count..simulation.config.node_count {
        for receiver in simulation.topology.neighbors[publisher]
            .iter()
            .copied()
            .filter(|peer| *peer >= simulation.config.attacker_count)
        {
            if let Some(subject) = simulation.topology.neighbors[receiver]
                .iter()
                .copied()
                .find(|peer| *peer < simulation.config.attacker_count)
            {
                return Some((publisher, receiver, subject));
            }
        }
    }
    None
}
