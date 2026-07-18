# Changelog

## 0.1.10 - 2026-07-18

- Queue published events to every configured relay without waiting for every
  relay to acknowledge them, preventing an unavailable relay from blocking
  otherwise healthy pubsub delivery.
