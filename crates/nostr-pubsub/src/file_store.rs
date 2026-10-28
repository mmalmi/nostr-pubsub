//! Optional native adapter for bounded signed events. One writer owns a path;
//! callers select the owner/scope and decide which public events peers may read.
use crate::{
    EventBus, EventRetentionPolicy, EventSource, Filter, MAX_RETAINED_SNAPSHOT_BYTES,
    PublishReport, PubsubError, QueryEvent, QueryOptions, QueryReport, Result, RetainedEvents,
    SOURCE_PRIORITY_LOCAL_INDEX, VerifiedEvent,
};
use async_trait::async_trait;
use nostr::{Event, Timestamp};
use serde::{Deserialize, Serialize};
use std::{
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct FileEventBus {
    state: Arc<Mutex<Option<State>>>,
}
struct State {
    path: PathBuf,
    events: RetainedEvents,
    _writer: File,
}
#[derive(Serialize, Deserialize)]
struct Snapshot<E = Event> {
    version: u8,
    events: Vec<E>,
}

impl FileEventBus {
    /// Open a private local snapshot. The lock also excludes another process.
    /// Do not share this path across users, discovery scopes or privacy lanes.
    pub async fn open(path: impl AsRef<Path>, retention: EventRetentionPolicy) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let state = tokio::task::spawn_blocking(move || {
            let mut events = RetainedEvents::new(retention)?;
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                std::fs::create_dir_all(parent).map_err(storage)?;
            }
            let mut lock_path = path.as_os_str().to_os_string();
            lock_path.push(".lock");
            let mut options = OpenOptions::new();
            options.read(true).write(true).create(true).truncate(false);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let writer = options.open(PathBuf::from(lock_path)).map_err(storage)?;
            writer.try_lock().map_err(storage)?;
            match File::open(&path) {
                Ok(file) => {
                    let mut bytes = Vec::new();
                    file.take(MAX_RETAINED_SNAPSHOT_BYTES as u64 + 1)
                        .read_to_end(&mut bytes)
                        .map_err(storage)?;
                    if bytes.len() > MAX_RETAINED_SNAPSHOT_BYTES {
                        return Err(storage("event snapshot exceeds byte limit"));
                    }
                    let snapshot: Snapshot<serde_json::Value> =
                        serde_json::from_slice(&bytes).map_err(storage)?;
                    if snapshot.version != 1 {
                        return Err(storage("unsupported event snapshot version"));
                    }
                    events.restore(
                        snapshot
                            .events
                            .into_iter()
                            .filter_map(|event| serde_json::from_value(event).ok()),
                        Timestamp::now().as_secs(),
                    );
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(storage(error)),
            }
            Ok(State {
                path,
                events,
                _writer: writer,
            })
        })
        .await
        .map_err(storage)??;
        Ok(Self {
            state: Arc::new(Mutex::new(Some(state))),
        })
    }

    /// Finish an already-owned write and release the writer lease, even when
    /// stopped consumers still hold clones. Later reads/writes return closed.
    pub async fn close(&self) {
        self.state.lock().await.take();
    }
}

#[async_trait]
impl EventBus for FileEventBus {
    async fn publish(&self, event: VerifiedEvent, _source: EventSource) -> Result<PublishReport> {
        // Retain the owned guard through the blocking write even if cancelled.
        let mut slot = self.state.clone().lock_owned().await;
        tokio::task::spawn_blocking(move || {
            let state = slot
                .as_mut()
                .ok_or_else(|| storage("event store is closed"))?;
            let now = Timestamp::now().as_secs();
            let mut candidate = state.events.clone();
            let accepted = candidate.insert(event, now);
            if accepted {
                let snapshot = Snapshot {
                    version: 1,
                    events: candidate.snapshot(now),
                };
                let bytes = serde_json::to_vec(&snapshot).map_err(storage)?;
                if bytes.len() > MAX_RETAINED_SNAPSHOT_BYTES {
                    return Err(storage("event snapshot exceeds byte limit"));
                }
                let parent = state
                    .path
                    .parent()
                    .filter(|p| !p.as_os_str().is_empty())
                    .unwrap_or(Path::new("."))
                    .to_path_buf();
                let mut temporary = tempfile::NamedTempFile::new_in(&parent).map_err(storage)?;
                temporary.write_all(&bytes).map_err(storage)?;
                temporary.as_file().sync_all().map_err(storage)?;
                temporary.persist(&state.path).map_err(storage)?;
                state.events = candidate;
                #[cfg(unix)]
                File::open(&parent)
                    .and_then(|directory| directory.sync_all())
                    .map_err(storage)?;
            }
            Ok(PublishReport {
                accepted,
                priority: SOURCE_PRIORITY_LOCAL_INDEX,
                reason: (!accepted).then(|| "event unchanged or outside retention".into()),
            })
        })
        .await
        .map_err(storage)?
    }

    async fn query(&self, filters: Vec<Filter>, options: QueryOptions) -> Result<QueryReport> {
        let slot = self.state.lock().await;
        let state = slot
            .as_ref()
            .ok_or_else(|| storage("event store is closed"))?;
        Ok(QueryReport {
            events: state
                .events
                .query(&filters, options, Timestamp::now().as_secs())
                .into_iter()
                .map(|event| QueryEvent {
                    event,
                    source: EventSource::local_index("retained-events"),
                    priority: SOURCE_PRIORITY_LOCAL_INDEX,
                })
                .collect(),
        })
    }
}
fn storage(error: impl std::fmt::Display) -> PubsubError {
    PubsubError::Storage(error.to_string())
}
