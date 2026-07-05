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

Source routing defaults prefer local indexes, then FIPS-carried endpoint routes,
then generic peer routes, with actual Nostr relay routes last. Product code can
override route priority per source, but relay fallback should be explicit rather
than the first peerfinding path.

## Crates

- `nostr-pubsub`: core `EventBus`, route, source, query, policy, retention, and
  inv/want types.
- `nostr-pubsub-relay`: optional `nostr-sdk` backend for actual Nostr relays.
- `nostr-pubsub-social-graph`: social-graph policy adapter for filtering and
  prioritizing event authors or sources.

Hashtree mesh/local-relay adapters should live in the hashtree repo because
they depend on hashtree internals. FIPS, Nostr VPN, and Iris Drive should keep
their product-specific event interpretation in their own repos.

## Checks

```sh
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
```
