# nostr-pubsub-sim

Deterministic, bounded adversarial simulations for the production
`nostr_pubsub::InvWantMesh` state machine and wire codec.

The default scenario creates 1,000 peers: 800 honest nodes in a small-world
overlay and 200 Sybil/blackhole nodes. Sybils can grind deterministic neutral
fanout order, send malformed payloads, and advertise syntactically valid event
IDs without answering wants. The simulator compares:

- `neutral`: no local peer observations
- `local-behavior`: successful peers have positive local scores, observed
  blackholes have negative scores, and one fanout slot remains available to an
  unknown peer

It reports honest delivery, inventory/want/frame counts, bytes, malformed
rejections, blackhole drops, and sends using unknown-peer exploration slots.
Shared ratings and social identities are intentionally outside this model.

Run the default comparison:

```sh
cargo run -p nostr-pubsub-sim
```

Configure the deterministic scenario:

```sh
cargo run -p nostr-pubsub-sim -- \
  --nodes 1000 \
  --attackers 200 \
  --fanout 4 \
  --unknown-reserve 1 \
  --max-hops 12 \
  --spam-per-honest 1
```

The simulation has an explicit message budget and fails rather than running
without a bound. Valid fake inventories remain an intentionally visible source
of amplification: quality-aware fanout improves eclipse resistance, but this
first slice does not convert reputation into an inbound trust gate.
