# Changelog

## 0.3.1 - 2026-07-16

- Retry a local subscription's `REQ` after an initially unavailable FIPS route
  later reconnects, while keeping replay delivery valid during the pending send.
- Close a late successful `REQ` when its local subscription or peer admission was
  removed before the send completed.

## 0.3.0 - 2026-07-16

- Add `FipsNostrPubsubClient`, the shared browser `nostr.pubsub/1`
  `REQ`/`EVENT`/`CLOSE` carrier for signed Nostr events.
- Keep peer admission application-owned and explicit; connected peers are never
  inferred, and subscriptions refresh when admitted standalone links reconnect.
- Bound replay, subscriptions, filters, peers, frames, and pending work; failed
  publications remain retryable and invalid or non-admitted traffic is dropped.
- Match the native FSP datagram maximum exactly: accept 65,525 bytes and reject
  65,526, with a real Rust process roundtrip gate.
- Include TypeScript sources referenced by the published declaration and source
  maps so clean-installed artifacts are self-contained.

## 0.2.0 - 2026-07-16

- Add the bounded `FipsInvWantStream` and `FipsInvWantTcpDriver` over the
  shared TCP/FIPS v1 stack.
- Cover partial and coalesced records, queue and peer bounds, close/reset
  lifecycle, reconnects, and real Rust-to-TypeScript process interoperability.
- Match Rust simultaneous-connect ordering by normalizing compressed and
  x-only transport identities to their canonical npub ordering key.
- Keep authenticated capability discovery explicit; no fallback provider
  namespace or unauthenticated peer inference is introduced.

## 0.1.5 - 2026-07-13

- Add the authenticated FIPS Nostr relay adapter and shared Rust/TypeScript
  interoperability vectors.
