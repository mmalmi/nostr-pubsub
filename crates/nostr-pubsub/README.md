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

Product crates keep their own app-specific event meanings. This crate provides
the shared pubsub vocabulary.
