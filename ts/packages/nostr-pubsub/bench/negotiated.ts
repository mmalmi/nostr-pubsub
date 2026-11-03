// Exercise production subscriptions, codec and TCP over an RTC data channel.
// Each end has a single known peer, so only the service ports need framing here.
import * as current from 'pubsub-source/index';
import * as comparison from 'comparison-pubsub';
import type {FipsPubsubClientNode} from 'pubsub-source/fips-pubsub-client-types';
import type {NostrVerifiedEvent} from 'pubsub-source/types';
import type {FipsPubsubWireMessage} from 'pubsub-source/wire';

type Carrier = { client: RTCDataChannel; server: RTCDataChannel; close(): void };
type Handler = Parameters<FipsPubsubClientNode['registerService']>[1];

function endpoint(peer: string) {
  const services = new Map<number, Handler>();
  const listeners = new Set<(value: unknown) => void>();
  let channel: RTCDataChannel | undefined;
  let traffic = 0;
  const node: FipsPubsubClientNode = {
    registerService(port, handler) { services.set(port, handler); return () => services.delete(port); },
    on(kind, listener) {
      if (kind === 'peer') listeners.add(listener);
      return () => listeners.delete(listener);
    },
    async sendDatagram({dst, srcPort = 0, dstPort, payload}) {
      if (dst !== peer || channel?.readyState !== 'open') throw Error('Peer offline');
      const packet = new Uint8Array(4 + payload.byteLength);
      const view = new DataView(packet.buffer);
      view.setUint16(0, srcPort); view.setUint16(2, dstPort);
      packet.set(payload, 4); channel.send(packet); traffic += packet.length;
    },
  };
  return {node, bytes: () => traffic, peers: () => channel ? [peer] : [], attach(next?: RTCDataChannel) {
    if (channel) channel.onmessage = null;
    channel = next;
    if (channel) channel.onmessage = message => {
      const view = new DataView(message.data);
      void services.get(view.getUint16(2))?.({src: peer, dstPort: view.getUint16(2),
        srcPort: view.getUint16(0), payload: new Uint8Array(message.data, 4)});
    };
    for (const notify of listeners) notify({remotePubkey: peer, state: channel ? 'connected' : 'disconnected'});
  }};
}
async function until(predicate: () => boolean | Promise<boolean>) {
  const deadline = performance.now() + 12_000;
  while (!await predicate()) {
    if (performance.now() >= deadline) throw Error('Feed delivery or protocol exchange timed out');
    await new Promise(resolve => setTimeout(resolve, 10));
  }
}
export async function verifyNegotiation(connect: () => Promise<Carrier>, posts: NostrVerifiedEvent[],
  legacy: boolean) {
  const receipts = [];
  for (const side of (legacy ? ['current', 'legacy-provider', 'legacy-reader'] : ['current'])) {
    const a = '02' + '11'.repeat(32), b = '03' + '22'.repeat(32);
    const left = endpoint(b), right = endpoint(a);
    const providerApi = side === 'legacy-provider' ? comparison : current;
    const readerApi = side === 'legacy-reader' ? comparison : current;
    // The legacy provider's retained index models a public outbox, populated
    // exclusively by the three explicit publications performed by this fixture.
    const outbox = new current.MemoryEventStore(16);
    const provider = new providerApi.FipsNostrPubsubClient({node:left.node, localPeerId:a, peers:left.peers,
      retainedEventReader: {async query(filters, options) {
        const events = await outbox.query(filters, options);
        return {events:events.map(event => ({event:current.verifyNostrEvent(event), source:current.localIndexSource('outbox'), priority:current.SOURCE_PRIORITY_LOCAL_INDEX})), complete:true};
      }}}).start();
    const reader = new readerApi.FipsNostrPubsubClient({node: right.node, localPeerId:b, peers:right.peers}).start();
    const outbound: FipsPubsubWireMessage[] = [], inbound: FipsPubsubWireMessage[] = [];
    for (const [client, log] of [[provider, outbound], [reader, inbound]] as const) {
      const encode = client.codec.encodeFrame.bind(client.codec);
      client.codec.encodeFrame = message => { log.push(message); return encode(message); };
    }
    const config = {type:'latest' as const, kinds:[1]};
    const send = await providerApi.openPublicPeerFeedSource(new providerApi.FipsNostrPubsubEventSource(provider), config);
    const receive = await readerApi.openPublicPeerFeedSource(new readerApi.FipsNostrPubsubEventSource(reader), config);
    const has = async (id: string) => (await receive.query!([{ids:[id]}])).events.some(record => record.event.id === id);
    let carrier: Carrier | undefined;
    async function join() {
      carrier = await connect(); left.attach(carrier.client); right.attach(carrier.server);
      await until(() => provider.peerSubscriptionCount(b) > 0 && reader.peerSubscriptionCount(a) > 0);
    }
    function leave() { left.attach(); right.attach(); carrier?.close(); carrier = undefined; }
    try {
      await join(); await outbox.put(posts[0]); await provider.publish(posts[0]); await until(() => has(posts[0].id));
      if ([...outbound,...inbound].some(frame => frame.type === 'sync')) throw Error('Negotiated on a cold feed');
      await receive.query!([{ids:[posts[3].id]}, {authors:[posts[3].pubkey]}]);
      leave();
      await Promise.all([provider.idle(), reader.idle()]);
      // A publisher without an admitted peer must not claim a successful send.
      await outbox.put(posts[1]);
      let queuedOffline = false;
      try { await provider.publish(posts[1]); } catch { queuedOffline = true; }
      if (!queuedOffline) throw Error('Offline publication reported a send');
      outbound.length = inbound.length = 0;
      const before = left.bytes() + right.bytes(), start = performance.now();
      await join();
      await until(() => has(posts[1].id));
      if (side === 'current') {
        await until(() => [...outbound, ...inbound].some(f => f.type === 'sync' && f.action === 'end'));
      }
      await outbox.put(posts[2]); await provider.publish(posts[2]); await until(() => has(posts[2].id));
      const frames = [...outbound, ...inbound], sync = frames.filter(f => f.type === 'sync');
      if ((side === 'current') !== !!sync.length || JSON.stringify(frames).includes(posts[3].id))
        throw Error('Unnegotiated frames or browsing ID on the wire');
      if (side === 'current' && outbound.some(f => f.type === 'inv' && f.eventId === posts[0].id))
        throw Error('Duplicate inventory for a known post');
      receipts.push({pair:side, coldAndLive:true, warmGap:await has(posts[1].id),
        controlRecords:sync.length, rtcDatagramBytes:left.bytes()+right.bytes()-before,
        reconnectAndDeliveryMs:performance.now()-start});
    } finally {
      send.close(); receive.close(); await Promise.all([provider.stop(), reader.stop()]); leave();
    }
  }
  return receipts;
}
