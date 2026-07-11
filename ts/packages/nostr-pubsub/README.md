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
