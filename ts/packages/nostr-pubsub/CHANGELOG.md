# Changelog

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
