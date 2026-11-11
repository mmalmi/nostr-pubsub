# nostr-pubsub-reconcile

Transport-independent Negentropy v1 set reconciliation. The companion npm package
`nostr-pubsub-reconcile` implements the same wire protocol in TypeScript, including
browser workers, using Web Crypto and no WebAssembly.

```rust
use nostr_pubsub_reconcile::{Filter, Limits, Session};
# fn example() -> Result<(), String> {
let mut initiator = Session::new([], Filter { since: 100, until: 200 }, Limits::default())?;
let mut responder = Session::new([], Filter { since: 100, until: 200 }, Limits::default())?;
let mut query = initiator.initiate()?;
loop {
    let response = responder.respond(&query)?;
    let step = initiator.reconcile(&response)?;
    // Fetch step.need and offer step.have subject to the receiver's policy.
    match step.next { Some(next) => query = next, None => break }
}
# Ok(()) }
```

Records are immutable `(timestamp, 32-byte ID)` pairs. IDs must be cryptographic
hashes, such as Nostr event IDs; additive fingerprints are unsuitable for padded
counters. Both peers must agree on the inclusive `since`/`until` window and the
meaning of a record. Native timestamps are `u64`; TypeScript uses `bigint`.

Limits bound records, frame bytes and rounds. Exceeding a limit is an explicit
error, never a successful partial comparison. Defaults are 100,000 records,
16 KiB per frame and 256 rounds. Split larger histories into agreed windows.
Each session compares one immutable snapshot; reconnects, policy changes and
concurrent new records require another session. Discard idle sessions yourself.

The caller owns authentication, authorization, retention, record validation,
transport and durable storage. Negentropy completion is **not** an acknowledgement
that records were transferred or saved. A receiver may decline history or pause
when storage is full. Intentional local deletions need durable suppression records
or an excluded history range so they do not become repair requests forever.

The engine retains the MIT reference layout and upstream Rust wire types, with
correct tail fingerprints at frame boundaries. `tests/wire.rs` independently checks
every emitted range; `tests/reconcile.rs` checks actual differences. The TypeScript
suite exchanges real frames with the Rust `reconcile-stdio` fixture in both roles.

Version 0.1.1 uses patched `negentropy` 0.5.2 and also validates ID-list lengths
before allocating in the adapted engine (RUSTSEC-2026-0365). The public session's
existing whole-frame validation remains in place; the wire format is unchanged.

Version 0.1.2 keeps running ID sums for the snapshot, so a range fingerprint is
one subtraction and one hash instead of a pass over the range, and builds the
snapshot by sorting rather than through an ordered map. A million-record
snapshot measured about 3.5 times faster to build and a 100-difference sync
about five times faster. Frames are byte-identical to 0.1.1. While reading
records the session may briefly hold up to twice `max_records` before
collapsing duplicates; the limit on the resulting snapshot is unchanged.
