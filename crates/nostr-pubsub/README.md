# nostr-pubsub

Neutral Nostr event and source routing primitives for relay, mesh, local-index,
and test backends.

This crate re-exports standard `nostr` protocol message types for normal
`REQ`/`CLOSE` subscriptions and `EVENT` delivery. It adds transport-neutral
building blocks for:

- bounded event retention policies based on Nostr filters
- source routes plus policy-aware historical and live routing across additive
  Hashtree/local-index, FIPS, peer, and traditional relay sources
- inv/want content keys, inventory announcements, wants, and frames
- a bounded signed-event `InvWantMesh` state machine with reverse wants,
  exactly-once local delivery, and configurable protocol envelopes
- strict inventory, `WANT`, and frame admission with canonical event IDs
- bounded recovery through at most three matching inventory providers
- targeted cached-event replay for late peers through the same
  inventory/WANT/frame proof
- count- and byte-bounded cached payloads, with a 16 MiB aggregate default
- TTL- and count-bounded seen-inventory and delivered-event deduplication
- raw cache-byte and state-count snapshots through `retained_state()`
- bounded route-local attribution for confirmed transport disruptions
- priority-aware peer fanout with explicit unknown-peer exploration capacity
- peer subscription tracking so inventory is sent only after a matching filter
- bounded FIPS-TCP record codecs for standard Nostr `REQ`/`CLOSE`/`EVENT` plus
  grouped subscription-scoped `INV` and one-event `WANT`

`VerifiedEvent` verifies both the event ID and Schnorr signature. The FIPS wire
adapter updates bounded peer subscriptions and never returns an unverified
event. FIPS transports remain responsible for stream framing, admission,
liveness, and backpressure.

Callers that already hold a `VerifiedEvent` from their trust boundary can use
`publish_verified`, `replay_verified_to_peer`, or `receive_verified_frame` to
avoid checking the same signature again. Untrusted events and decoded wire
input must use the normal verifying methods; a fast path is not a replacement
for boundary verification.

`InvWantMesh` accepts only canonical lowercase 64-hex event IDs. An inventory
must fit the local kind, payload-size, and hop bounds; another provider's copy
must repeat the original kind and size, but may carry a different remaining hop
budget after traversing a different path. Every budget is capped locally. The
mesh sends `WANT` to at most three such providers, rejects frames from every
other source, and accepts a requested frame only when its verified signed event
has the announced ID, kind, and exact serialized size. Fulfilled route state
retains the requested-provider set until its bounded expiry, so a delayed valid
answer is verified and ignored rather than scored as malicious. A `WANT` with
neither a cached event nor a live route is not retained. Route expiry or
seen-inventory eviction removes its reverse route, pending peers, and
forwarded-want marker together, and forwarding always decrements the hop limit.
Cached event payloads are limited by both `max_cached_events` and
`max_cached_event_bytes`; the latter defaults to 16 MiB. Delivered-event and
seen-inventory deduplication expire by TTL and retain hard count bounds.

When a local transport confirms that the stream or link carrying one active
request failed, it may call `record_transport_disruption` for that exact peer
and event. The matching attempt then expires without falsely recording an
unserved provider. Never derive this signal from peer-supplied data: false
marking would suppress useful local misbehavior evidence. The bounded mark
clears on fulfillment, expiry, or a new `WANT` attempt to that peer.

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
rejected valid frame can be dismissed without provider credit or penalty while
retaining bounded provenance for already-requested late answers.

After the mesh's confidence floor is met, `PeerBehaviorObservation` exposes the
score together with its sample count and separate valid-frame,
invalid-message, and unserved-inventory counts. Consumers can therefore require
the evidence appropriate to a machine-rating policy instead of treating every
negative score as the same failure mode. The TypeScript mesh mirrors these
admission, recovery, maintenance, and evidence semantics.

Consumers expose exactly one `PubsubProvider`: `local-only` for a local peer
provider, `direct-relay` for direct relay sockets, or `router` for an explicitly
composed `NostrPubsubRouter`. Provider construction is application-owned; the
core never opens or falls back to an unconfigured backend.
