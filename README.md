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
for signed Nostr-event inventory/want/frame propagation. It verifies complete
frames, deduplicates inventories and deliveries, caches events for downstream
wants, expires reverse routes, and limits fanout, hop count, event size, cache
size, and pending peers. The transport supplies its protocol namespace and
connected peer set; applications remain responsible for choosing which peer
subscriptions or coarse streams should see an inventory.

Mesh peer selection accepts optional locally observed quality scores. Unknown
peers are represented separately from low-quality peers, and fanout reserves
configurable exploration capacity for them. The core does not infer social
identity, ingest third-party ratings, or make an admission decision from these
scores.

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
  Ethernet peers and FIPS service port 7368.
- `nostr-pubsub-relay`: optional `nostr-sdk` backend for actual Nostr relays.
- `nostr-pubsub-sim`: deterministic adversarial simulations that execute the
  production `InvWantMesh` and codec across up to thousands of virtual peers.
- `nostr-pubsub-social-graph`: social-graph policy adapter for filtering and
  prioritizing event authors or sources.

Hashtree mesh/local-relay adapters should live in the hashtree repo because
they depend on hashtree internals. FIPS, Nostr VPN, and Iris Drive should keep
their product-specific event interpretation in their own repos.

## TypeScript

The browser-ready TypeScript core lives in `ts/packages/nostr-pubsub` and builds
as an ESM package named `nostr-pubsub`. It mirrors the Rust core primitives that
browser apps need before wiring real transports: source route defaults, Nostr
filter retention, bounded peer subscriptions, delivery policy, inv/want frames,
in-memory event buses, routed queries with source policy, and the bounded FIPS
wire boundary.

Iris browser apps can later consume it with a local dependency such as:

```json
"nostr-pubsub": "link:../nostr-pubsub/ts/packages/nostr-pubsub"
```

Interop vectors live in `ts/packages/nostr-pubsub/test-data/interop-vectors.json`
and are read by both Vitest and Rust integration tests. Update the shared vector
file when changing behavior that must remain compatible across Rust/FIPS peers
and browser callers.

## Checks

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
pnpm --dir ts --filter nostr-pubsub build
pnpm --dir ts --filter nostr-pubsub test
cargo test -p nostr-pubsub --test typescript_interop
cargo run -p nostr-pubsub-sim -- --nodes 1000 --attackers 200
```
