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
admission, and outbound connection policy. This package does not contain a
default relay or gateway. See the repository `docs/inv-want-wire.md` for
compatibility and security boundaries.

`FipsNostrPubsubClient` is the browser peer carrier matching Rust
`FipsPubsubClient` on authenticated FSP service `nostr.pubsub/1` (port 7368).
It carries only verified Nostr `REQ`, `EVENT`, and `CLOSE` frames. The
application supplies the admitted peer identities explicitly, so arbitrary
connected FIPS peers are never treated as pubsub providers:

```ts
import { FipsNostrPubsubClient } from 'nostr-pubsub';

const pubsub = new FipsNostrPubsubClient({
  node: fipsNode,
  peers: () => appOwnedStandaloneLinks.map((link) => link.remotePubkey),
  allowedKinds: [1059, 1060, 30078, 37368],
}).start();

const subscription = pubsub.subscribe([{ kinds: [1060] }], (event) => {
  receiveSignedChatEvent(event);
});
await pubsub.publish(signedChatEvent);
subscription.close();
```

Frames are bounded to the native FSP service-body maximum of 65,525 bytes.
Peer refresh events can add or restore explicitly admitted standalone links;
they do not create links or infer admission policy.

For reliable authenticated carriage, `FipsInvWantStream` applies the same
four-byte big-endian record framing and bounds as Rust. `FipsInvWantTcpDriver`
binds that stream to `@fips/tcp`, accepts only the peer identity supplied by the
authenticated FSP service context, converges simultaneous connects on one
stream, and owns bounded partial-read/write queues. Applications explicitly
choose peers and reconnect timing:

```ts
import { FipsInvWantStream, FipsInvWantTcpDriver } from 'nostr-pubsub';

const stream = new FipsInvWantStream();
const driver = FipsInvWantTcpDriver.bind(
  fipsNode,
  localFipsPubkey,
  stream,
  {
    serviceNamespace: 'nostr.pubsub',
    serviceVersion: 1,
    servicePort: 39_121,
    maxPeers: 64,
    maxQueuedRecordsPerPeer: 64,
    maxQueuedBytesPerPeer: 2 * 1024 * 1024,
    maxIoBytesPerDrive: 256 * 1024,
  },
);
await driver.connectPeer(remoteFipsPubkey);
const report = await driver.poll();
```

`fipsInvWantTcpCapabilityName()` returns the authenticated capability name for
the configured namespace and version. Capability roster registration remains
an FSP concern; the TypeScript FIPS API does not yet expose the Rust endpoint's
lifecycle-bound capability registration. The driver does not advertise through
plaintext discovery or add a product-local fallback. The existing
`FipsNostrRelayService` datagram adapter remains available for its separate
`REQ`/`EVENT`/`CLOSE` contract.

The simultaneous-connect tie-break normalizes FIPS's compressed hex peer keys
to NIP-19 `npub` strings before ordering them. This deliberately matches the
Rust driver's ordering; comparing the raw compressed hex would sometimes make
the two runtimes select and reset opposite streams.

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
