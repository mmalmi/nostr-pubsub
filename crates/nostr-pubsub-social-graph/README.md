# nostr-pubsub-social-graph

Social-graph policy adapter for `nostr-pubsub`.

This crate connects `nostr-social-graph` backends to `nostr_pubsub::PubsubPolicy`
so applications can prioritize, throttle, or drop events and sources according
to graph distance, mute state, overmute heuristics, and optional service
reputation records.

`MeshPeerPolicy` applies the same local policy directly to inv/want fanout:
known peers carry a score, unknown peers remain eligible for exploration, and
dropped peers are omitted.
