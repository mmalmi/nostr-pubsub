# nostr-pubsub

Neutral Nostr event/source routing primitives for relay, mesh, local-index, and
test backends.

This repo intentionally keeps product policy out of the core crate. Product and
capability repos can share the same event bus and policy shapes while deciding
their own meanings for peer adverts, paid route offers, rating facts, or file
delivery sources.

## Shape

Use the standard `nostr` crate message types for normal Nostr protocol traffic:
`REQ`/`CLOSE` subscriptions, `EVENT` publish/delivery, and relay responses. The
core crate re-exports those types so transports do not invent parallel
subscription messages.

`nostr-pubsub` only adds the missing transport-neutral extension for
inventory-first delivery: content keys, inventory announcements, wants, and
frames. A backend can push full events only to subscribed peers, use inv/want for
larger or costed payloads, or combine both per stream. Inventory is still gated
by matching Nostr subscriptions before any inv/want announcement is sent.

The Rust core also contains `InvWantMesh`, the bounded production state machine
for signed Nostr-event inventory/want/frame propagation. Inventories require a
canonical lowercase event ID and locally allowed kind, size, and hop limit;
copies must repeat the signed event's kind and serialized size, while their
remaining hop budgets may differ by path and remain locally capped. The mesh
can request the same event from at most three providers for bounded blackhole
recovery. It accepts a frame only from a provider sent a `WANT`, then verifies
the event signature and exact announced ID, kind, and serialized size. After
one provider answers, bounded fulfilled-route provenance makes delayed answers
from the other requested providers harmless; every unrequested source remains
invalid. A `WANT` without a cached event or live route is ignored, while
seen-inventory, reverse-route, pending-peer, and forwarded-want state expires
or is evicted together. Forwarding decrements the locally bounded hop budget.
Fanout, event and wire size, caches, route lifetime, pending peers, and delivery
deduplication are bounded. The event cache has both count and aggregate payload
limits, with a 16 MiB default byte cap. Seen-inventory and delivered-event
deduplication have TTL and count bounds, and `retained_state()` exposes raw
cache bytes and state counts for memory accounting.

`publish_verified`, `replay_verified_to_peer`, and `receive_verified_frame`
avoid repeating signature checks when the caller already holds a
`VerifiedEvent` produced at its trust boundary. Untrusted events and wire input
must use the normal verifying paths; the fast paths do not move the boundary.

Mesh peer selection accepts optional locally observed quality scores. Unknown
peers are represented separately from low-quality peers, and fanout reserves
configurable exploration capacity for them. The core does not infer social
identity, ingest third-party ratings, or make an admission decision from these
scores. Once the local confidence floor is met, `PeerBehaviorObservation`
exposes the score and its valid-frame, invalid-message, unserved-inventory, and
total sample counts so a policy can distinguish evidence types.

The core crate also defines the retention contract for bounded event caches:
which Nostr filters a local store should retain and how many matching events it
may keep. Durable implementations belong in adapter repos. For example, a
hashtree-backed local index can implement the bounded cache while FIPS, Nostr
VPN, and Iris Drive choose the small event sets they need.

FIPS connections can carry pubsub beside VPN tunnel traffic and hashtree traffic
as a multiplexed transport. FIPS should own connection liveness, peer admission,
link backpressure, and framing. Pubsub should own subscription/filter,
bounded peer subscription records, inventory/want, source selection, and
policy/scoring semantics.

The Rust and TypeScript cores expose matching `FipsPubsubWireCodec` and
`FipsPubsubWireAdapter` boundaries. Each codec consumes one payload frame
provided by FIPS, enforces a configurable byte limit, and carries ordinary
Nostr `REQ`, `CLOSE`, client `EVENT`, and subscription `EVENT` JSON arrays.
Decoded events are accepted only after their IDs and Schnorr signatures verify.
The codec does not define stream framing or peer admission.

Source routing defaults prefer local indexes, then FIPS-carried endpoint routes,
then generic peer routes, with actual Nostr relay routes last. Product code can
override route priority per source, but relay fallback should be explicit rather
than the first peerfinding path.

## Crates

- `nostr-pubsub`: core `EventBus`, route, source, query, policy, retention, and
  inv/want types, plus the transport-blind provider interface and explicit
  `local-only` / `direct-relay` modes.
- `nostr-pubsub-fips`: local-only `FipsEndpoint` provider over authenticated
  connected peers and FIPS service port 7368, with bounded replay for late
  subscribers.
- `nostr-pubsub-relay`: optional `nostr-sdk` backend for actual Nostr relays.
- `nostr-pubsub-sim`: deterministic adversarial simulations that execute the
  production `InvWantMesh`, FIPS subscriptions, social and machine admission,
  and forged versus authorized-poisoned rating paths across up to thousands of
  virtual peers.
- `nostr-pubsub-social-graph`: social-graph policy adapter for filtering and
  prioritizing event authors or sources.

Hashtree mesh/local-relay adapters should live in the hashtree repo because
they depend on hashtree internals. FIPS, Nostr VPN, and Iris Drive should keep
their product-specific event interpretation in their own repos.

## TypeScript

The browser-ready TypeScript core lives in `ts/packages/nostr-pubsub` and builds
as an ESM package named `nostr-pubsub`. It mirrors the Rust core primitives that
browser apps need before wiring real transports: source route defaults, Nostr
filter retention, bounded peer subscriptions, delivery policy, the verified
bounded `InvWantMesh` and byte-compatible `InvWantCodec`, in-memory event buses,
routed queries with source policy, and the bounded FIPS wire boundary. Its mesh
uses the same strict inventory/frame admission, three-provider recovery,
transient-state eviction, aggregate cache-byte cap, retained-state snapshot,
TTL-plus-count deduplication, verified-event fast paths, hop enforcement,
maintenance scoring, and separate valid-frame, invalid-message, and
unserved-inventory evidence counters as Rust.

Iris browser apps can later consume it with a local dependency such as:

```json
"nostr-pubsub": "link:../nostr-pubsub/ts/packages/nostr-pubsub"
```

Interop vectors live in `crates/nostr-pubsub/tests/data/interop-vectors.json`
and are read by both Vitest and Rust integration tests. Update the shared vector
file when changing behavior that must remain compatible across Rust/FIPS peers
and browser callers. The inv/want contract is documented in
`docs/inv-want-wire.md`.

## Checks

Reviewability is enforced: every Rust source file is at most 1,000 lines, and
every TypeScript package source file is at most 500 lines.

```sh
cargo test --workspace
cargo test -p nostr-pubsub-sim --test source_size
cargo clippy --workspace --all-targets -- -D warnings
pnpm --dir ts --filter nostr-pubsub build
pnpm --dir ts --filter nostr-pubsub check
pnpm --dir ts --filter nostr-pubsub test
cargo test -p nostr-pubsub@0.1.8 --test typescript_interop
cargo run -p nostr-pubsub-sim -- --nodes 1000 --attackers 200
cargo test --release -p nostr-pubsub-sim --test release_gate -- \
  --ignored --nocapture --test-threads=1
```
