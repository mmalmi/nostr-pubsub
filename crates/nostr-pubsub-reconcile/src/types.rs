// Copyright (c) 2023 Doug Hoyte; 2023 Yuki Kishimoto. MIT licensed.
// Wire constants and value types adapted from negentropy 0.5.2, kept local so
// the engine, storage and types share one audited copy.
use core::array::TryFromSliceError;
use core::cmp::Ordering;
use core::fmt;
use core::ops::Deref;

/// Implemented protocol version (version 1).
pub const PROTOCOL_VERSION: u64 = 0x61;
pub const ID_SIZE: usize = 32;
pub const FINGERPRINT_SIZE: usize = 16;

#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    IdTooBig,
    FrameSizeLimitTooSmall,
    AlreadyBuiltInitialMessage,
    Initiator,
    NonInitiator,
    UnexpectedMode(u64),
    ParseEndsPrematurely,
    ProtocolVersionNotFound,
    InvalidProtocolVersion,
    UnsupportedProtocolVersion,
    TryFromSlice,
    BadRange,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::IdTooBig => write!(f, "ID too big"),
            Self::FrameSizeLimitTooSmall => write!(f, "Frame size limit too small"),
            Self::AlreadyBuiltInitialMessage => write!(f, "Already built initial message"),
            Self::Initiator => write!(f, "initiator not asking for have/need IDs"),
            Self::NonInitiator => write!(f, "non-initiator asking for have/need IDs"),
            Self::UnexpectedMode(m) => write!(f, "Unexpected mode: {m}"),
            Self::ParseEndsPrematurely => write!(f, "parse ends prematurely"),
            Self::ProtocolVersionNotFound => write!(f, "protocol version not found"),
            Self::InvalidProtocolVersion => write!(f, "invalid negentropy protocol version byte"),
            Self::UnsupportedProtocolVersion => {
                write!(f, "server does not support our negentropy protocol version")
            }
            Self::TryFromSlice => write!(f, "could not convert slice to array"),
            Self::BadRange => write!(f, "bad range"),
        }
    }
}

impl From<TryFromSliceError> for Error {
    fn from(_: TryFromSliceError) -> Self {
        Self::TryFromSlice
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; ID_SIZE]);

impl Deref for Id {
    type Target = [u8; ID_SIZE];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Id {
    pub const fn from_byte_array(bytes: [u8; ID_SIZE]) -> Self {
        Self(bytes)
    }

    pub const fn as_bytes(&self) -> &[u8; ID_SIZE] {
        &self.0
    }
}

/// Ordered by timestamp, then ID.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Item {
    pub timestamp: u64,
    pub id: Id,
}

impl Item {
    pub fn with_timestamp(timestamp: u64) -> Self {
        Self {
            timestamp,
            id: Id::default(),
        }
    }

    pub const fn with_timestamp_and_id(timestamp: u64, id: Id) -> Self {
        Self { timestamp, id }
    }
}

/// A range boundary: an item whose ID may be shortened to `id_len` bytes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Bound {
    pub item: Item,
    pub id_len: usize,
}

impl Bound {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn from_item(item: &Item) -> Self {
        Self {
            item: *item,
            id_len: ID_SIZE,
        }
    }

    pub fn with_timestamp(timestamp: u64) -> Self {
        Self {
            item: Item::with_timestamp(timestamp),
            id_len: 0,
        }
    }

    pub fn with_timestamp_and_id(timestamp: u64, id: &[u8]) -> Result<Self, Error> {
        if id.len() > ID_SIZE {
            return Err(Error::IdTooBig);
        }
        let mut bytes = [0_u8; ID_SIZE];
        bytes[..id.len()].copy_from_slice(id);
        Ok(Self {
            item: Item::with_timestamp_and_id(timestamp, Id::from_byte_array(bytes)),
            id_len: id.len(),
        })
    }
}

impl PartialOrd for Bound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Bound {
    fn cmp(&self, other: &Self) -> Ordering {
        self.item.cmp(&other.item)
    }
}
