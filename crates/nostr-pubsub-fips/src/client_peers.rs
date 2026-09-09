use super::{ConnectedPeerLink, PeerIdentity, Result, invalid_option, poisoned, storage_error};
use crate::client_inner::ClientInner;

pub(super) fn validate_routed_peers(
    mut peers: Vec<String>,
    capacity: usize,
    local_npub: &str,
    transport_restricted: bool,
) -> Result<Vec<String>> {
    if transport_restricted && !peers.is_empty() {
        return Err(invalid_option(
            "routed_peers require an unrestricted FIPS client",
        ));
    }
    if peers.len() > capacity {
        return Err(invalid_option(
            "routed_peers cannot exceed max_connected_peers",
        ));
    }
    for npub in &mut peers {
        *npub = PeerIdentity::from_npub(npub)
            .map(|peer| peer.npub())
            .map_err(|error| invalid_option(format!("invalid routed peer: {error}")))?;
    }
    peers.retain(|npub| npub != local_npub);
    peers.sort_unstable();
    peers.dedup();
    Ok(peers)
}

impl ClientInner {
    pub(super) async fn connected_peer_links(&self) -> Result<Vec<ConnectedPeerLink>> {
        let snapshot = self
            .endpoint
            .peers()
            .await
            .map_err(|error| storage_error("snapshot FIPS peers", error))?;
        let routed = self
            .routed_peers
            .lock()
            .map_err(|_| poisoned("FIPS routed peers"))?;
        let mut peers = routed
            .iter()
            .map(|npub| ConnectedPeerLink {
                npub: npub.clone(),
                // A routed service session belongs to the destination identity,
                // independent of changes to its intermediate physical links.
                link_id: 0,
            })
            .collect::<Vec<_>>();
        let mut direct = snapshot
            .into_iter()
            .filter(|peer| {
                peer.connected
                    && self
                        .peer_transport
                        .is_none_or(|transport| peer.transport_type.as_deref() == Some(transport))
                    && peer
                        .transport_type
                        .as_deref()
                        .is_none_or(|transport| !self.excluded_peer_transports.contains(transport))
                    && routed.binary_search(&peer.npub).is_err()
            })
            .map(|peer| ConnectedPeerLink {
                npub: peer.npub,
                link_id: peer.link_id,
            })
            .collect::<Vec<_>>();
        direct.sort_unstable_by(|left, right| left.npub.cmp(&right.npub));
        direct.dedup_by(|left, right| left.npub == right.npub);
        peers.extend(direct);
        // Never change application-owned endpoint links to enforce pubsub bounds.
        peers.truncate(self.options.max_connected_peers);
        Ok(peers)
    }
}
