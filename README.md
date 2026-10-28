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
FIPS subscription extension adds only `INV` and one-event `WANT`: historical
catch-up and new live events use the same inventory-first transfer, while the
requested signed payload remains an ordinary addressed `EVENT`.

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
may keep. The optional native 'file-store' feature provides FileEventBus with
an atomic snapshot and exclusive writer lock (Rust 1.89+); browsers can use
IndexedDbEventBus with atomic transactions across tabs. Both revalidate signed
events on reload, retain NIP-01 coordinate winners, reject ephemeral/future
events, honor expiration on reads, and bound event, snapshot and entry sizes.
Use an explicit owner/scope/privacy-lane path or database name. Existing durable
application adapters remain supported; no filesystem or browser store is opened
implicitly. RetainedEvents exposes the same rules to custom adapters. For example, a
hashtree-backed local index can implement the bounded cache while FIPS, Nostr
VPN, and Iris Drive choose the small event sets they need.

FIPS connections can carry pubsub beside VPN tunnel traffic and hashtree traffic
as a multiplexed transport. FIPS should own connection liveness, peer admission,
link backpressure, and framing. Pubsub should own subscription/filter,
bounded peer subscription records, inventory/want, source selection, and
policy/scoring semantics.

The Rust and TypeScript cores expose matching `FipsPubsubWireCodec` and
`FipsPubsubWireAdapter` boundaries. Each codec consumes one length-delimited
record over reliable `fips-tcp`, enforces a configurable byte limit, and carries
ordinary Nostr `REQ`, `CLOSE`, client `EVENT`, and subscription `EVENT` JSON
arrays plus grouped `INV` and one-event `WANT`. Decoded events are accepted only
after their IDs and Schnorr signatures verify. FIPS owns stream admission,
liveness, and backpressure; there is no raw-datagram compatibility path.

Source routing defaults prefer local indexes, then FIPS-carried endpoint routes,
then generic peer routes, with actual Nostr relay routes last. The matching Rust
and TypeScript routers support both bounded historical queries and noisy live
subscriptions: same-dataset routes are ordered replicas, different dataset IDs
are additive, and signed event IDs are globally deduplicated across Hashtree
indexes, FIPS peers, and traditional relays. Product code can override route
priority per source, but relay fallback should be explicit rather than the first
peerfinding path.

## Crates

- `nostr-pubsub`: core `EventBus`, route, source, query, policy, retention, and
  inv/want types, plus the transport-blind provider interface and explicit
  `local-only` / `direct-relay` / composed `router` modes.
- `nostr-pubsub-fips`: local-only `FipsEndpoint` provider over authenticated
  connected peers and FIPS service port 7368, with bounded replay for late
  subscribers.
- `nostr-pubsub-relay`: optional `nostr-sdk` backend for actual Nostr relays.
- `nostr-pubsub-sim`: deterministic adversarial simulations that execute the
  production `InvWantMesh`, FIPS subscriptions, application filters, machine
  admission/removal,
  and forged versus authorized-poisoned rating paths across up to thousands of
  virtual peers on a virtual clock.
- `nostr-pubsub-social-graph`: social-graph policy adapter for filtering and
  prioritizing event authors or sources.

Hashtree mesh/local-relay adapters should live in the hashtree repo because
they depend on hashtree internals. FIPS, Nostr VPN, and Iris Drive should keep
their product-specific event interpretation in their own repos.

The simulator publishes real signed events through eight production-shaped
subscriptions: author feed, hashtag, Hashtree update, targeted machine rating,
Iris Drive root, FIPS advert, FIPS paid offer, and git repository announcement.
Repeated publication rounds traverse the real mesh, filter, verification,
loss/churn, retry, and first-acceptance paths; payment experiments consume those
verified delivery edges rather than fabricated traffic.

The scale incentive planner is explicitly a model, not a second Cashu
implementation. It compares direct Cashu, bounded offline peer credit, fixed
prepayment, incremental Spilman, and accepted-mint batching with bilateral
useful-service setoff, bounded residual peer credit, private on-demand mint discovery, outages,
payment refusal, and fake service claims. Reports include delivery paths, spam
suppression, honest CPU/memory/bandwidth proxies, settled value, fees,
unsecured exposure, locked liquidity, payment traffic, and mint acceptance
usefulness.

An optional integration gate feeds a production-path verified delivery into
bounded `cashu-credit`, real loopback CDK mints, and simulated Lightning. It
checks route-bound withdrawable settlement, closed-loop non-cashout, forged and
replayed proof rejection, payout binding, backing uniqueness, and reserve
conservation without putting wallet, mint, or economic policy code into the
pubsub protocol crates.

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

For relay-only browser bundles, import these APIs from
`nostr-pubsub/private-relays` to avoid loading optional FIPS adapters.

For private messaging, use `createPrivateRelayPublisher` for recipient-specific
encrypted kinds 1059, 1060 (v2), and 21111. Each event and retry opens independent
relay connections with a fresh anonymous NIP-42 key. The publisher exposes no
query or subscription API, and additional sources retain only their publication
capability. Supply a separate durable pending store; the caller owns its lifetime.
Wrap the account receiver's pending store with `publicationLaneStore(store,
'public')` before constructing its runtime, and route every private kind using
`isPrivateRelayEvent`, including malformed events, so rejection cannot fall back
to the account connection. Move valid old pending private rows to the private
store before removing their original pending entries. Invalid shared-group and
roster entries remain quarantined locally.

Use `createAnonymousRelayDiscovery` for targeted profile (0), relay-list
(10002/10050), public device-list (37368) and invite (30078) lookups.
`splitRelayDiscoveryFilters` partitions mixed kinds and author batches into
ordinary receiver interests and anonymous single-key scopes. Broad unscoped
public profile/relay-list scans remain ordinary. Each discovery operation
accepts exactly one author, or one reverse
device `#p` selector for kind 37368. Split different keys into separate operations;
unscoped, mixed-author and combined author/device selectors are rejected. Active
requests for the same key may share a scope; idle scopes close, and later scopes
use fresh authentication. Discovery never retries pending publications and never
closes a supplied local cache. `isRelayDiscoveryFilter` also classifies malformed
or unscoped protocol filters so hosts can fail closed. These boundaries isolate
protocol identities; servers can still observe network addresses and timing.

Interop vectors live in `crates/nostr-pubsub/tests/data/interop-vectors.json`
and are read by both Vitest and Rust integration tests. Update the shared vector
file when changing behavior that must remain compatible across Rust/FIPS peers
and browser callers. The inv/want contract is documented in
`docs/inv-want-wire.md`.

## Checks

Reviewability is enforced: every Rust source file is at most 1,000 lines, and
every TypeScript package source file is at most 500 lines.

```sh
cargo test --locked --workspace
cargo test --locked -p nostr-pubsub-sim --test source_size
cargo test --locked -p nostr-pubsub-sim --features cashu-integration \
  --test cashu_integration -- --nocapture
cargo clippy --locked --workspace --all-targets -- -D warnings
pnpm --dir ts --filter nostr-pubsub build
pnpm --dir ts --filter nostr-pubsub check
pnpm --dir ts --filter nostr-pubsub test
cargo test --locked -p nostr-pubsub@0.1.19 --test typescript_interop
cargo run --locked -p nostr-pubsub-sim -- --nodes 1000 --attackers 200
cargo test --locked --release -p nostr-pubsub-sim --test release_gate -- \
  --ignored --nocapture --test-threads=1
cargo test --locked --release -p nostr-pubsub-sim --test incentives \
  thousand_node_adversarial_incentive_gate -- --ignored --nocapture
```

## Library releases

Commit the manifests, root `Cargo.lock`, source and required scripts before
qualification. Use a full commit and a clean checkout. The focused social-graph
qualification uses preinstalled Rust/Cargo 1.99.0, the default features, and
`aarch64-apple-darwin` on an Apple silicon Mac with Xcode's macOS 27 SDK and
Clang, targeting macOS 14. It requires no additional system libraries or
privileged prerequisite installation. Reuse a compatible Cargo target directory
with one writer; record any explicit compiler/linker flags with the result.

```sh
cargo +1.99.0 test --frozen --release --target aarch64-apple-darwin \
  -p nostr-pubsub-social-graph --lib -- --test-threads=1
cargo +1.99.0 test --frozen --release --target aarch64-apple-darwin \
  -p nostr-pubsub-fips --test inv_want_stream -- --test-threads=1
```

`scripts/release-library.py` performs source, Cargo package, and release-ref
publication as separate steps. It uses Python 3.11+ and Git. Its preinstalled
Hashtree helper is pinned by executable and archive SHA-256 in the script and
checked before execution. It downloads no helper or toolchain. Cargo uses the
committed lock; registry archives are verified against its checksums.

Supply a private JSON plan and a fresh evidence directory for each step:

```sh
python3 scripts/release-library.py source "$RELEASE_PLAN" "$SOURCE_EVIDENCE"
python3 scripts/release-library.py package "$RELEASE_PLAN" "$PACKAGE_EVIDENCE"
python3 scripts/release-library.py refs "$RELEASE_PLAN" "$REF_EVIDENCE"
```

The plan contains `root`, `workspace` (relative to root), `target`, `head` (full
commit), `helper_directory`, and `source` with the established `remote`,
`existing_url` and explicit `public_url`. Set `entry_bytes`, `floor_bytes`,
`growth_bytes`, `seconds` and `phase_seconds` to the admitted resource budget.
Package/ref steps additionally need `crate`, `crate_path` (relative to root),
`version`, `source_receipt`, `package_receipt`, `tag`, and `push_master`.
`dependency_requirements` pins normalized manifest requirements;
`registry_checksums` maps `name@version` to expected archive checksums, including
workspace libraries resolved from the registry during packaging.

The source step preserves existing remote refs, publishes an immutable commit
ref, then fetches full history into an empty cache with local daemon use disabled.
It verifies the revision, reachable objects and strict Git integrity. The package
step checks clean source provenance, source bytes and executable metadata,
locked dependency versions, and downloaded registry bytes. Packaging uses
`--no-verify` because focused qualification is a separate required gate; it must
not be substituted for those tests. Inspect a failed or uncertain publication
and verify the registry before retrying; never upload changed bytes at an
existing version. Retain receipts privately and publish refs only after package
verification. These checks establish source availability and archive identity;
they are not a full dependency source audit or proof of native binary reproduction.

### Durable peer announcements

Select only public discovery filters in the local event store. A FipsPeerfinder
can use with_store followed by restore before network discovery; accepted
incoming and locally published announcements then reach the same store.
Use FipsPubsubClient::set_replay_source with that local store, or a permission-
filtered composition of existing local indexes, to answer ordinary Nostr REQ
messages. Keep anonymous per-author discovery caches separate from identified
replay services. Never attach a network router as a replay source.

Announcements are discovery candidates: FIPS still checks namespace, schema and
freshness, and authenticates a connection. Retain successful address memory
separately for outages longer than an announcement's signed lifetime.

Native applications can use `FileEventBus` with the `file-store` feature;
browsers can use `IndexedDbEventBus` and `RetainedPeerDiscovery`. Choose an
explicit owner/scope database or file path, and close the store after stopping
its consumers. Native `close().await` drains an already-owned write before
releasing its writer lease. No adapter opens a relay or changes peer authorization.
