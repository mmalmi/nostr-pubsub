# Changelog

## 0.1.11 - 2026-07-18

- Implement the shared live-source contract over ordinary relay
  `REQ`/`EVENT`/`CLOSE`, with owned-subscription filtering, signature
  verification, relay provenance, and explicit close fanout.
- Align historical relay reads with NIP-01 OR-filter limits, empty match-all
  queries, global event-ID deduplication, deterministic ordering, and the
  router-wide result limit.

## 0.1.10 - 2026-07-18

- Queue published events to every configured relay without waiting for every
  relay to acknowledge them, preventing an unavailable relay from blocking
  otherwise healthy pubsub delivery.
