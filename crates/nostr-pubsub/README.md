# nostr-pubsub

Neutral Nostr event and source routing primitives for relay, mesh, local-index,
and test backends.

This crate re-exports standard `nostr` protocol message types for normal
`REQ`/`CLOSE` subscriptions and `EVENT` delivery. It adds transport-neutral
building blocks for:

- bounded event retention policies based on Nostr filters
- source routes and priority-aware routed queries
- inv/want content keys, inventory announcements, wants, and frames
- a bounded signed-event `InvWantMesh` state machine with reverse wants,
  exactly-once local delivery, and configurable protocol envelopes
- targeted cached-event replay for late peers through the same
  inventory/WANT/frame proof
- priority-aware peer fanout with explicit unknown-peer exploration capacity
- peer subscription tracking so inventory is sent only after a matching filter
- bounded FIPS payload-frame codecs for standard Nostr `REQ`/`CLOSE`/`EVENT`

`VerifiedEvent` verifies both the event ID and Schnorr signature. The FIPS wire
adapter updates bounded peer subscriptions and never returns an unverified
event. FIPS transports remain responsible for stream framing, admission,
liveness, and backpressure.

Product crates keep their own app-specific event meanings. This crate provides
the shared pubsub vocabulary. Peer-quality inputs are local transport or pubsub
observations; the mesh does not assume that a transport peer has a social
identity and does not trust shared ratings by default.

The production mesh's local score is provider-specific. It rewards a peer only
after an accepted inventory is wanted and the peer returns the matching valid
signed frame. Inventories outside the configured kind subscription, invalid or
mismatched frames, and accepted inventories left unserved through route expiry
are negative evidence. Silent peers remain unknown. Applications should run
author/social admission before delivering a frame to the mesh; a locally
rejected valid frame can be dismissed without provider credit or penalty.

Consumers select exactly one `PubsubProvider`: `local-only` for a local peer
provider or `direct-relay` for direct relay sockets. Provider construction is
explicit and the core never falls back between the two modes at runtime.
