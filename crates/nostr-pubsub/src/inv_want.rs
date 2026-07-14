use async_trait::async_trait;

use crate::{EventSource, PublishReport, Result, SourceId};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PubsubStreamId(pub String);

impl PubsubStreamId {
    #[must_use]
    pub fn new(stream_id: impl Into<String>) -> Self {
        Self(stream_id.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PubsubContentKey {
    pub stream_id: PubsubStreamId,
    pub origin: SourceId,
    pub seq: u64,
}

impl PubsubContentKey {
    #[must_use]
    pub fn new(stream_id: impl Into<String>, origin: impl Into<String>, seq: u64) -> Self {
        Self {
            stream_id: PubsubStreamId::new(stream_id),
            origin: SourceId::new(origin),
            seq,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubsubInventory {
    pub key: PubsubContentKey,
    pub payload_bytes: u64,
    pub hop_limit: u8,
}

impl PubsubInventory {
    #[must_use]
    pub fn new(key: PubsubContentKey, payload_bytes: u64, hop_limit: u8) -> Self {
        Self {
            key,
            payload_bytes,
            hop_limit,
        }
    }

    #[must_use]
    pub fn want(&self) -> PubsubWant {
        PubsubWant::new(self.key.clone())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubsubWant {
    pub key: PubsubContentKey,
}

impl PubsubWant {
    #[must_use]
    pub fn new(key: PubsubContentKey) -> Self {
        Self { key }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubsubFrame {
    pub key: PubsubContentKey,
    pub payload: Vec<u8>,
    pub hop_limit: u8,
}

impl PubsubFrame {
    #[must_use]
    pub fn new(key: PubsubContentKey, payload: impl Into<Vec<u8>>, hop_limit: u8) -> Self {
        Self {
            key,
            payload: payload.into(),
            hop_limit,
        }
    }

    #[must_use]
    pub fn inventory(&self) -> PubsubInventory {
        PubsubInventory::new(
            self.key.clone(),
            u64::try_from(self.payload.len()).unwrap_or(u64::MAX),
            self.hop_limit,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvWantMessage {
    Inventory(PubsubInventory),
    Want(PubsubWant),
    Frame(PubsubFrame),
}

impl InvWantMessage {
    #[must_use]
    pub fn key(&self) -> &PubsubContentKey {
        match self {
            Self::Inventory(inventory) => &inventory.key,
            Self::Want(want) => &want.key,
            Self::Frame(frame) => &frame.key,
        }
    }

    #[must_use]
    pub fn stream_id(&self) -> &PubsubStreamId {
        match self {
            Self::Inventory(inventory) => &inventory.key.stream_id,
            Self::Want(want) => &want.key.stream_id,
            Self::Frame(frame) => &frame.key.stream_id,
        }
    }
}

#[async_trait]
pub trait InvWantBus: Send + Sync {
    async fn announce_inventory(
        &self,
        inventory: PubsubInventory,
        source: EventSource,
    ) -> Result<PublishReport>;

    async fn request_want(&self, want: PubsubWant, source: EventSource) -> Result<()>;

    async fn publish_frame(&self, frame: PubsubFrame, source: EventSource)
    -> Result<PublishReport>;
}
