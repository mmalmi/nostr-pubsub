# nostr-pubsub

Neutral Nostr event/source routing primitives for relay, mesh, local-index, and
test backends.

This repo intentionally keeps product policy out of the core crate. Product and
capability repos can share the same event bus and policy shapes while deciding
their own meanings for peer adverts, paid route offers, rating facts, or file
delivery sources.

## Crates

- `nostr-pubsub`: core `EventBus`, route, source, query, and policy types.
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
