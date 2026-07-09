# nostr-pubsub

Neutral Nostr event and source routing primitives for relay, mesh, local-index,
and test backends.

This crate re-exports standard `nostr` protocol message types for normal
`REQ`/`CLOSE` subscriptions and `EVENT` delivery. It adds transport-neutral
building blocks for:

- bounded event retention policies based on Nostr filters
- source routes and priority-aware routed queries
- inv/want content keys, inventory announcements, wants, and frames
- peer subscription tracking so inventory is sent only after a matching filter
- bounded FIPS payload-frame codecs for standard Nostr `REQ`/`CLOSE`/`EVENT`

`VerifiedEvent` verifies both the event ID and Schnorr signature. The FIPS wire
adapter updates bounded peer subscriptions and never returns an unverified
event. FIPS transports remain responsible for stream framing, admission,
liveness, and backpressure.

Product crates keep their own app-specific event meanings. This crate provides
the shared pubsub vocabulary.

Consumers select exactly one `PubsubProvider`: `local-only` for a local peer
provider or `direct-relay` for direct relay sockets. Provider construction is
explicit and the core never falls back between the two modes at runtime.
