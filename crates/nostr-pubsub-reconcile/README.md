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

The engine retains the MIT reference layout and upstream Rust storage types, with
correct tail fingerprints at frame boundaries. `tests/wire.rs` independently checks
every emitted range; `tests/reconcile.rs` checks actual differences. The TypeScript
suite exchanges real frames with the Rust `reconcile-stdio` fixture in both roles.
