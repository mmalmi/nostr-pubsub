# nostr-pubsub-social-graph

Social-graph policy adapter for `nostr-pubsub`.

This crate connects `nostr-social-graph` backends to `nostr_pubsub::PubsubPolicy`
so applications can prioritize, throttle, or drop events and sources according
to graph distance, mute state, overmute heuristics, and optional service
reputation records.

The crate implements `nostr_pubsub::MeshPeerPolicy` for the same graph: known
good peers carry a score, unknown peers remain eligible for exploration, and
dropped peers are omitted.

`PeerReputation` maintains the newest valid signed rating per rater, subject,
and scope. The local node is always a trust root; callers may also configure up
to 1,024 explicit `trusted_raters`, using public keys in hex or npub form. The
default set is empty. A rating affects the projection only when its Nostr
signer is its declared rater and is either the local root or an explicitly
configured rater. Positive peer ratings improve peer selection but do not
authorize their subjects to rate other peers. Root negative ratings can revoke
a configured rater's influence; removing it from configuration and replaying
also removes its influence. Signature binding does not establish whether an
authorized rater is honest.

`trusted_raters` supplies entrypoints for one general peer trust prior. It
consumes signed kind-7368 ratings in the configured scope (`fips.peer` by
default); it does not automatically import a person's kind-3 follow list or
map their social identity to separate transport keys. Applications can choose
their own public entrypoints or leave the set empty. The exported legacy
`DEFAULT_SOCIAL_GRAPH_ENTRYPOINT_NPUB` example is never selected automatically.

`PeerRatingPublisher` coalesces local machine ratings before publication.
`from_events_at` reconstructs its retention and cadence state against a
caller-supplied timestamp for deterministic virtual-clock replay, while
`from_events` retains wall-clock compatibility. Reputation ingestion and replay
likewise provide `ingest_event_at` and `replay_at`, and pruning always takes an
explicit timestamp.
`PeerRatingPublisherConfig::min_negative_samples` independently gates negative
ratings (three samples by default), so callers can demand evidence before
sharing a removal signal; `min_non_negative_samples` performs the corresponding
gate for neutral and positive ratings. Its shared policy bundle uses the same
projection for mesh peers and signed event authors, so known-bad authors can be
rejected before cache/fanout regardless of whether their event arrived through
FIPS, a relay, or another transport. Unknown authors remain admitted by
default. Ratings expire after 30 days, timestamps more than ten minutes in the
future are rejected, and the projection retains at most 4,096 rating keys total
and 1,024 per rater. On global capacity pressure, non-anchor raters are evicted
before ratings authored by the local root or explicit trusted raters; if only
anchor ratings remain, the oldest are evicted deterministically. Publisher
cadence state uses the same age window and per-rater bound.
