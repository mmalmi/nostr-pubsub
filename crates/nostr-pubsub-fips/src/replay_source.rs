use std::sync::{Arc, Weak};

use nostr_pubsub::{EventBus, QueryOptions, QueryReport};
use tokio::sync::mpsc;

use crate::{
    ClientInner, EventId, EventSource, Filter, FipsPubsubClient, FipsPubsubWireMessage,
    PeerIdentity, PubsubPeerInterest, Result, SourceId, SubscriptionId, VerifiedEvent, poisoned,
};

pub(super) enum ReplayQuery {
    Subscription {
        id: SubscriptionId,
        filters: Vec<Filter>,
        excluded: Vec<EventId>,
        limit: usize,
    },
    Event(EventId),
}

pub(super) struct ReplayRequest {
    peer: PeerIdentity,
    source: Arc<dyn EventBus>,
    query: ReplayQuery,
}

impl FipsPubsubClient {
    /// Serve peer history requests from an application-owned local event store.
    ///
    /// The source must expose only events this application permits peers to read.
    /// It is queried off the transport loop with bounded work. The application
    /// owns ingestion and persistence; live delivery and fresh-read provenance
    /// are unchanged. Do not attach a network router or this client itself.
    /// Detaching with `None` remains valid after shutdown.
    pub fn set_replay_source(&self, source: Option<Arc<dyn EventBus>>) -> Result<()> {
        let admission = source.as_ref().map(|_| self.inner.admit()).transpose()?;
        let previous = std::mem::replace(
            &mut *self
                .inner
                .replay_source
                .lock()
                .map_err(|_| poisoned("FIPS replay source"))?,
            source,
        );
        drop(admission);
        drop(previous);
        Ok(())
    }
}

impl ClientInner {
    pub(super) fn queue_replay(&self, peer: PeerIdentity, query: ReplayQuery) -> bool {
        let source_id = SourceId::new(peer.npub());
        if !self
            .peer_subscriptions
            .lock()
            .is_ok_and(|subscriptions| subscriptions.peer_filter_count(&source_id) > 0)
        {
            return false;
        }
        let source = self
            .replay_source
            .lock()
            .ok()
            .and_then(|source| source.clone());
        source.is_some_and(|source| {
            self.replay_tx
                .try_send(ReplayRequest {
                    peer,
                    source,
                    query,
                })
                .is_ok()
        })
    }
}

pub(super) async fn run(inner: Weak<ClientInner>, requests: mpsc::Receiver<ReplayRequest>) {
    let requests = tokio::sync::Mutex::new(requests);
    // These futures belong to the single owned task: shutdown drops all four
    // queries, with no detached child tasks or blocking the transport driver.
    tokio::join!(
        worker(&inner, &requests),
        worker(&inner, &requests),
        worker(&inner, &requests),
        worker(&inner, &requests),
    );
}

async fn worker(
    inner: &Weak<ClientInner>,
    requests: &tokio::sync::Mutex<mpsc::Receiver<ReplayRequest>>,
) {
    loop {
        let request = requests.lock().await.recv().await;
        let (Some(request), Some(inner)) = (request, inner.upgrade()) else {
            break;
        };
        let _ = tokio::time::timeout(inner.options.query_timeout, serve(inner, request)).await;
    }
}

async fn serve(inner: Arc<ClientInner>, request: ReplayRequest) {
    let limit = match &request.query {
        ReplayQuery::Subscription { limit, .. } => *limit,
        ReplayQuery::Event(_) => 1,
    };
    if limit == 0 {
        return;
    }
    let filters = match &request.query {
        ReplayQuery::Subscription { filters, .. } => filters.clone(),
        ReplayQuery::Event(id) => vec![Filter::new().id(*id).limit(1)],
    };
    let report = request
        .source
        .query(filters.clone(), QueryOptions { limit: Some(limit) })
        .await
        .unwrap_or_default();
    let mut events = bounded_events(report, &filters, &request.query, limit);
    let source_id = SourceId::new(request.peer.npub());
    for (event, source) in events.drain(..) {
        if !inner.event_is_admitted(&event, &source).await {
            continue;
        }
        let Ok(_admission) = inner.admit() else {
            return;
        };
        let Ok(current_source) = inner.replay_source.lock() else {
            return;
        };
        if !current_source
            .as_ref()
            .is_some_and(|source| Arc::ptr_eq(source, &request.source))
        {
            return;
        }
        let Ok(subscriptions) = inner.peer_subscriptions.lock() else {
            return;
        };
        let matches = subscriptions.matching_subscriptions(&source_id, &event);
        let frame = match &request.query {
            ReplayQuery::Subscription { id, .. } => {
                if !matches
                    .iter()
                    .any(|subscription| subscription.subscription_id == id.as_str())
                {
                    continue;
                }
                inner.inventory_frame(vec![id.clone()], &event, inner.options.max_hops)
            }
            ReplayQuery::Event(id) => {
                let Some(subscription) = matches.first() else {
                    continue;
                };
                if event.as_event().id != *id {
                    continue;
                }
                inner.codec.encode_frame(&FipsPubsubWireMessage::deliver(
                    SubscriptionId::new(subscription.subscription_id.clone()),
                    event,
                ))
            }
        };
        if let Ok(frame) = frame {
            let _ = inner.send_frame(request.peer, frame);
        }
    }
}

fn bounded_events(
    report: QueryReport,
    filters: &[Filter],
    query: &ReplayQuery,
    limit: usize,
) -> Vec<(VerifiedEvent, EventSource)> {
    let excluded = match query {
        ReplayQuery::Subscription { excluded, .. } => excluded.as_slice(),
        ReplayQuery::Event(_) => &[],
    };
    let mut events = report
        .events
        .into_iter()
        .take(limit)
        .filter(|event| {
            !excluded.contains(&event.event.as_event().id)
                && PubsubPeerInterest::from_filters(filters, &event.event)
                    == PubsubPeerInterest::Subscribed
        })
        .map(|event| (event.event, event.source))
        .collect::<Vec<_>>();
    events.sort_by(|a, b| {
        b.0.as_event()
            .created_at
            .cmp(&a.0.as_event().created_at)
            .then_with(|| a.0.as_event().id.cmp(&b.0.as_event().id))
    });
    events.dedup_by_key(|event| event.0.as_event().id);
    events.truncate(limit.saturating_sub(excluded.len()));
    events
}

pub(super) fn query_limit(filters: &[Filter], maximum: usize) -> usize {
    if filters.is_empty() {
        return maximum;
    }
    filters
        .iter()
        .fold(0_usize, |total, filter| {
            total.saturating_add(filter.limit.unwrap_or(maximum))
        })
        .min(maximum)
}
