# nostr-pubsub for TypeScript

Browser-ready Nostr source routing, bounded peer subscriptions, FIPS Nostr
frames, and signed-event inventory/want propagation. The TypeScript package
lives beside the canonical Rust crates and shares their interoperability
vectors.

```ts
import { InvWantCodec, InvWantMesh, meshPeer } from 'nostr-pubsub';

const codec = new InvWantCodec('iris.fips.pubsub', 1, 64 * 1024);
const mesh = new InvWantMesh({ allowedKinds: new Set([37_195]) });

for (const action of mesh.publish(signedEvent, [meshPeer(remoteFipsPubkey)], Date.now())) {
  if (action.type === 'send') {
    await fips.send(action.peerId, codec.encode(action.message));
  }
}
```

The application owns Nostr relay connections, peer-advert meaning, FIPS peer
admission, and transport framing. This package does not contain a default relay
or gateway. See the repository `docs/inv-want-wire.md` for compatibility and
security boundaries.

`InvWantMesh` matches the Rust production state machine: inventories use
canonical lowercase event IDs and local kind, size, and hop bounds; repeated
inventories must keep identical event kind and size, while remaining hop
budgets may differ by path under the local cap; and recovery uses no more than
three providers. Frames are accepted only from a provider sent a `WANT` and
only when the verified signature, ID, kind, and serialized size match the
inventory. Bounded fulfilled-route provenance absorbs delayed valid answers
from requested alternatives without scoring them, while unrequested sources
remain invalid. A want with neither a cached event nor a live route is ignored,
and related transient state expires or is evicted together. Cached payloads are
bounded by both count and `maxCachedEventBytes`, whose aggregate default is
16 MiB. Seen-inventory and delivered-event deduplication have both TTL and count
bounds. `retainedState()` reports raw cache bytes and state counts, while
`maintain()` and `peerBehaviorObservation()` expose the same maintenance and
evidence semantics as Rust.

If a local transport confirms that the stream or link carrying one active
request failed, `recordTransportDisruption()` marks only that peer/event
attempt so expiry does not falsely blame the provider. Do not derive this
signal from peer-supplied data. Its bounded mark clears on completion, expiry,
or a new `WANT` attempt to that peer.

`publishVerified()`, `replayVerifiedToPeer()`, and `receiveVerifiedFrame()`
avoid a repeated signature check only for immutable events returned by
`verifyNostrEvent()`. Use `publish()`, `replayToPeer()`, or `receive()` for
untrusted input. Type assertions are not a trust boundary and are rejected by
the verified fast paths.
