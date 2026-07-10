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
and scope. By default it uses the local node as the trust root and accepts a
rating only when its Nostr signer is also its declared rater, preventing an
unknown publisher from claiming somebody else's trust. `PeerRatingPublisher`
coalesces local machine ratings before publication. Its shared policy bundle
uses the same projection for mesh peers and signed event authors, so known-bad
authors can be rejected before cache/fanout regardless of whether their event
arrived through FIPS, a relay, or another transport. Unknown authors remain
admitted by default.
