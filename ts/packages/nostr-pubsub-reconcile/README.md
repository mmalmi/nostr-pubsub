# nostr-pubsub-reconcile

Negentropy v1 reconciliation for browsers, workers and Node. Pure TypeScript,
Web Crypto SHA-256, no runtime package dependencies and no WebAssembly.
Wire-compatible with the Rust crate of the same name.

```ts
import { Reconciliation } from 'nostr-pubsub-reconcile';

const filter = { since: 100n, until: 200n };
const initiator = new Reconciliation([], filter);
const responder = new Reconciliation([], filter);
let query = await initiator.initiate();
for (;;) {
  const response = await responder.respond(query);
  const step = await initiator.reconcile(response);
  // Fetch step.need and offer step.have subject to the receiver's policy.
  if (!step.next) break;
  query = step.next;
}
```

Pass `{ timestamp: bigint, id: Uint8Array }` records. IDs must be 32-byte
cryptographic hashes, such as Nostr event IDs, never padded counters. Both peers
must agree on the inclusive time filter and record semantics. Snapshots are copied
and immutable. Timestamps support the full u64 range except the reserved infinity
value. Calls on one session must be sequential.

Optional limits: `maxRecords` (100,000), `maxFrameBytes` (16,384), `maxRounds`
(256). Exceeding a limit throws; it never claims partial history is complete.
Split larger histories into agreed windows and discard expired sessions.

The application owns authenticated transport, history permissions, validation and
storage. Completion compares a snapshot; it does not acknowledge durable delivery.
A receiver can narrow its time window, decline data or pause for storage pressure.
Persist local deletion suppression so removed records are not repeatedly requested.
Restart with a fresh session after reconnects, filter changes or new records.

`pnpm test` runs interoperability tests against the Rust fixture (Cargo required).
