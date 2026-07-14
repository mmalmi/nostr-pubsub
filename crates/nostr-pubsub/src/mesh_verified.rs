use crate::{Result, VerifiedEvent};

use super::{InvWantAction, InvWantMesh, InvWantWireMessage, MeshPeer};

impl InvWantMesh {
    /// Publish an event whose signature was already checked at the trust boundary.
    pub fn publish_verified(
        &mut self,
        verified: VerifiedEvent,
        peers: &[MeshPeer],
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        self.prune(now_ms);
        let event = verified.into_event();
        let (event_id, payload_bytes) = self.validate_verified_event(&event)?;
        let event_kind = u16::from(event.kind);
        self.store_event(event, payload_bytes, now_ms);
        if !self.remember_inventory(&event_id, now_ms) {
            return Ok(Vec::new());
        }
        Ok(self.send_to_selected_peers(
            peers,
            None,
            &InvWantWireMessage::Inventory {
                event_id,
                event_kind,
                payload_bytes,
                hop_limit: self.options.max_hops,
            },
        ))
    }

    /// Replay a verified cached event without repeating signature verification.
    pub fn replay_verified_to_peer(
        &mut self,
        verified: VerifiedEvent,
        peer_id: &str,
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        self.prune(now_ms);
        let event = verified.into_event();
        let (event_id, payload_bytes) = self.validate_verified_event(&event)?;
        let event_kind = u16::from(event.kind);
        self.store_event(event, payload_bytes, now_ms);
        Ok(vec![InvWantAction::Send {
            peer_id: peer_id.to_string(),
            message: InvWantWireMessage::Inventory {
                event_id,
                event_kind,
                payload_bytes,
                hop_limit: self.options.max_hops,
            },
        }])
    }

    /// Admit a frame already verified by event policy without checking its
    /// signature again inside the mesh.
    pub fn receive_verified_frame(
        &mut self,
        source_peer: &str,
        event_id: &str,
        verified: VerifiedEvent,
        peers: &[MeshPeer],
        now_ms: u64,
    ) -> Result<Vec<InvWantAction>> {
        self.prune(now_ms);
        let event = verified.into_event();
        let result = self.receive_frame(source_peer, event_id, &event, peers, now_ms, false);
        if result.is_err() {
            self.record_invalid_message(source_peer);
        }
        result
    }
}
