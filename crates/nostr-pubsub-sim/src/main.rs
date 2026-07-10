use nostr_pubsub_sim::{PeerSelectionMode, SimulationConfig, run_simulation};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_config(std::env::args().skip(1))?;
    println!(
        "mode,nodes,attackers,honest_delivered,delivery_bps,processed,inventory,want,frame,wire_bytes,dropped_at_attackers,rejected_malformed,unknown_sends"
    );
    for mode in [PeerSelectionMode::Neutral, PeerSelectionMode::LocalBehavior] {
        let report = run_simulation(config.clone(), mode)?;
        println!(
            "{},{},{},{},{},{},{},{},{},{},{},{},{}",
            report.mode.as_str(),
            report.node_count,
            report.attacker_count,
            report.delivered_honest_nodes,
            report.delivery_basis_points,
            report.processed_messages,
            report.inventory_messages,
            report.want_messages,
            report.frame_messages,
            report.wire_bytes,
            report.dropped_at_attackers,
            report.rejected_malformed_messages,
            report.sends_to_unknown_peers,
        );
    }
    Ok(())
}

fn parse_config(args: impl Iterator<Item = String>) -> Result<SimulationConfig, String> {
    let mut config = SimulationConfig::default();
    let mut args = args;
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .ok_or_else(|| format!("missing value after {flag}"))?;
        match flag.as_str() {
            "--nodes" => config.node_count = parse_number(&flag, &value)?,
            "--attackers" => config.attacker_count = parse_number(&flag, &value)?,
            "--fanout" => config.fanout = parse_number(&flag, &value)?,
            "--unknown-reserve" => {
                config.unknown_peer_reserve = parse_number(&flag, &value)?;
            }
            "--max-hops" => config.max_hops = parse_number(&flag, &value)?,
            "--spam-per-honest" => {
                config.attack_inventories_per_honest_node = parse_number(&flag, &value)?;
            }
            "--message-budget" => {
                config.max_processed_messages = parse_number(&flag, &value)?;
            }
            _ => return Err(format!("unknown argument {flag}")),
        }
    }
    Ok(config)
}

fn parse_number<T>(flag: &str, value: &str) -> Result<T, String>
where
    T: std::str::FromStr,
{
    value
        .parse()
        .map_err(|_| format!("invalid numeric value {value:?} for {flag}"))
}
