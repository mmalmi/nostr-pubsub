// Both strategies run over the same ordered, reliable RTCDataChannel record layer.
import { finalizeEvent } from 'nostr-tools/pure';
import { Reconciliation } from 'reconcile-source';
import { verifyNostrEvent, type NostrVerifiedEvent } from 'pubsub-source/types';
import { FipsPubsubWireCodec } from 'pubsub-source/wire';
import { FipsPubsubEventCache } from 'pubsub-source/fips-pubsub-client-protocol';
import { FipsRetainedReplay } from 'pubsub-source/fips-pubsub-retained';

const codec = new FipsPubsubWireCodec();
const encoder = new TextEncoder();
const filter = { kinds: [1] };
const windowRange = { since: 0n, until: 3_000_000_000n };
const subscriptionId = 'feed';
type Post = NostrVerifiedEvent;
type Strategy = 'inv-want' | 'negentropy' | 'negentropy-warm';
type Scenario = 'sparse-local' | 'sparse-remote' | 'remote-empty' | 'identical' | 'tail-1%' | 'scattered-10%' | 'half' | 'two-sided' | 'disjoint' | 'empty';
const fixtures = new Map<number, Post[]>();
// Browser-native codecs avoid charging the experimental envelope for a slow JS hex loop.
const hex = (data: Uint8Array) => data.toHex();
const bytes = (value: string) => Uint8Array.fromHex(value);

function fixture(count: number): Post[] {
  if (fixtures.has(count)) return fixtures.get(count)!;
  // Fixed synthetic authors and timestamps give every run the same event IDs.
  const secret = new Uint8Array(32);
  const posts = Array.from({length: count * 2}, (_, i) => {
    secret[31] = i % 16 + 1;
    return verifyNostrEvent(finalizeEvent({kind: 1, tags: [], created_at: 1_770_000_000 + (i >> 2),
      content: 'Public benchmark post ' + i + ' ' + 'data '.repeat(32)}, secret));
  });
  fixtures.set(count, posts);
  return posts;
}
function inputs(posts: Post[], scenario: Scenario) {
  const count = posts.length / 2;
  const remoteCount = scenario === 'remote-empty' ? 0 : scenario === 'sparse-remote' ? Math.min(8, count) : count;
  const remote = posts.slice(0, remoteCount);
  const local = {
    'sparse-local': () => remote.slice(0, 8),
    'sparse-remote': () => posts.slice(0, count),
    'remote-empty': () => posts.slice(0, count),
    identical: () => remote,
    'tail-1%': () => remote.slice(0, count - Math.ceil(count / 100)),
    'scattered-10%': () => remote.filter((_, i) => i % 10 !== 0),
    half: () => remote.filter((_, i) => i % 2 === 0),
    'two-sided': () => [...remote.slice(count >> 2), ...posts.slice(count, count + (count >> 2))],
    disjoint: () => posts.slice(count), empty: () => [],
  }[scenario]();
  return {remote, local};
}

async function connect() {
  // Device access in this dedicated fake-device browser allows numeric host candidates.
  const media = await navigator.mediaDevices.getUserMedia({audio: true});
  media.getTracks().forEach(track => track.stop());
  const first = new RTCPeerConnection({iceServers: []}), second = new RTCPeerConnection({iceServers: []});
  async function description(peer: RTCPeerConnection, sdp: RTCSessionDescriptionInit) {
    const gathered = new Promise<void>(resolve => peer.addEventListener('icegatheringstatechange', () => {
      if (peer.iceGatheringState === 'complete') resolve();
    }));
    await peer.setLocalDescription(sdp);
    if (peer.iceGatheringState !== 'complete') await gathered;
    return peer.localDescription!;
  }
  try {
    const remoteChannel = new Promise<RTCDataChannel>(resolve => { second.ondatachannel = e => resolve(e.channel); });
    const client = first.createDataChannel('public-feed', {ordered: true});
    const opened = new Promise<void>((resolve, reject) => {
      client.onopen = () => resolve();
      first.onconnectionstatechange = () => { if (first.connectionState === 'failed') reject(new Error('WebRTC candidate pair failed')); };
    });
    await second.setRemoteDescription(await description(first, await first.createOffer()));
    await first.setRemoteDescription(await description(second, await second.createAnswer()));
    await opened;
    const server = await remoteChannel;
    server.binaryType = client.binaryType = 'arraybuffer';
    return {client, server, close: () => {first.close(); second.close();}};
  } catch (error) { first.close(); second.close(); throw error; }
}

let link: Awaited<ReturnType<typeof connect>>;
async function run(strategy: Strategy, count: number, scenario: Scenario) {
  const {local, remote} = inputs(fixture(count), scenario);
  const remoteMap = new Map(remote.map(event => [event.id, event]));
  const localMap = new Map(local.map(event => [event.id, event]));
  const expected = remote.filter(event => !localMap.has(event.id));
  const expectedLocalOnly = local.filter(event => !remoteMap.has(event.id));
  const advertised = new Set<string>(), localOnly = new Set<string>();
  const received = new Set<string>(), wanted = new Set<string>();
  const traffic = {up: 0, down: 0, messages: 0, barriers: 0, payload: 0, comparisons: 0};
  let done = false;
  const id = subscriptionId;
  const {client, server} = link;
  const records = (events: Post[]) => events.map(e => ({id: bytes(e.id), timestamp: BigInt(e.created_at)}));
  const cache = new FipsPubsubEventCache(count);
  for (const event of remote) cache.remember(event, undefined, 4);
  let reader: Reconciliation, writer: Reconciliation;
  function send(channel: RTCDataChannel, data: Uint8Array | string) {
    const n = typeof data === 'string' ? encoder.encode(data).length : data.byteLength;
    if (data === 'DONE') traffic.barriers++;
    else { traffic[channel === client ? 'up' : 'down'] += n; traffic.messages++; }
    if (typeof data === 'string') channel.send(data);
    else channel.send(data as Uint8Array<ArrayBuffer>);
  }
  const replay = new FipsRetainedReplay(cache, undefined, count, 4, verifyNostrEvent,
    () => true, async (_peer, msg) => send(server, codec.encodeFrame(msg)));
  const delivery = (eventId: string) => {
    const event = remoteMap.get(eventId);
    if (!event) throw new Error('Unknown event requested');
    const frame = codec.encodeFrame({type: 'event', subscriptionId: id, event});
    traffic.payload += frame.byteLength;
    send(server, frame);
  };
  let timer: ReturnType<typeof setTimeout>;
  const started = performance.now();
  try {
    await new Promise<void>((resolve, reject) => {
      timer = setTimeout(() => reject(new Error('Catch-up timed out')), 60_000);
      const finish = () => { if (done && !wanted.size) resolve(); };
      // Each direction processes records in order, including asynchronous hashing.
      function listen(channel: RTCDataChannel, fn: (data: string | ArrayBuffer) => Promise<void>) {
        let previous = Promise.resolve();
        channel.onmessage = message => { previous = previous.then(() => fn(message.data)).catch(reject); };
      }
      listen(server, async data => {
        if (typeof data === 'string') {
          const [op, , arg, initial] = JSON.parse(data);
          if (op === 'NEG-OPEN' || op === 'NEG-MSG') {
            if (op === 'NEG-OPEN') {
              if (JSON.stringify(arg) !== JSON.stringify(filter)) throw new Error('Unexpected peer scope');
              writer = new Reconciliation(records(remote), windowRange);
            }
            traffic.comparisons++;
            const response = await writer.respond(bytes(initial ?? arg));
            send(server, JSON.stringify(['NEG-MSG', id, hex(response)]));
          } else if (op === 'NEG-CLOSE') send(server, 'DONE');
          else throw new Error('Unexpected control message');
        } else {
          const msg = codec.decodeFrame(new Uint8Array(data));
          if (msg.type === 'req') {
            if (strategy !== 'inv-want' || JSON.stringify(msg.filters) !== JSON.stringify([filter])) {
              throw new Error('Unexpected peer scope');
            }
            await replay.replay('client', id, msg.filters);
            send(server, 'DONE');
          } else if (msg.type === 'want') delivery(msg.eventId);
          else throw new Error('Unexpected request');
        }
      });
      listen(client, async data => {
        if (data === 'DONE') { done = true; finish(); return; }
        if (typeof data === 'string') {
          const [op, , arg] = JSON.parse(data);
          if (op !== 'NEG-MSG') throw new Error('Unexpected control response');
          const step = await reader.reconcile(bytes(arg));
          for (const eventId of step.have.map(hex)) localOnly.add(eventId);
          const ids = step.need.map(hex);
          for (const eventId of ids) {
            if (localMap.has(eventId) || wanted.has(eventId)) throw new Error('Unnecessary request');
            wanted.add(eventId);
            // Both strategies use the same existing WANT/EVENT records for gaps in this feed.
            send(client, codec.encodeFrame({type: 'want', eventId}));
          }
          send(client, JSON.stringify(step.next ? ['NEG-MSG', id, hex(step.next)] : ['NEG-CLOSE', id]));
        } else {
          const msg = codec.decodeFrame(new Uint8Array(data));
          if (msg.type === 'inv') {
            if (advertised.has(msg.eventId)) throw new Error('Duplicate inventory announcement');
            advertised.add(msg.eventId);
            if (!localMap.has(msg.eventId) && !wanted.has(msg.eventId)) {
              wanted.add(msg.eventId); send(client, codec.encodeFrame({type: 'want', eventId: msg.eventId}));
            }
          } else if (msg.type === 'event') {
            if (!wanted.delete(msg.event.id) || received.has(msg.event.id)) throw new Error('Unsolicited or duplicate event');
            received.add(msg.event.id); finish();
          } else throw new Error('Unexpected response');
        }
      });
      if (strategy === 'inv-want') send(client, codec.encodeFrame({type: 'req', subscriptionId: id, filters: [filter]}));
      else {
        reader = new Reconciliation(records(local), windowRange);
        void reader.initiate({fingerprintFirst: strategy === 'negentropy-warm'}).then(initial => send(client, JSON.stringify(['NEG-OPEN', id, filter, hex(initial)]))).catch(reject);
      }
    });
    const ms = performance.now() - started;
    if (strategy === 'inv-want') {
      if (advertised.size !== remote.length || remote.some(e => !advertised.has(e.id))) {
        throw new Error('Incomplete retained inventory');
      }
      for (const event of local) if (!advertised.has(event.id)) localOnly.add(event.id);
    }
    if (received.size !== expected.length || expected.some(e => !received.has(e.id)) ||
        localOnly.size !== expectedLocalOnly.length || expectedLocalOnly.some(e => !localOnly.has(e.id)) ||
        traffic.barriers !== 1) throw new Error('Incorrect inventory differences or incomplete exchange');
    return {strategy, count, scenario, local: local.length, remote: remote.length,
      missing: received.size, localOnly: localOnly.size, ...traffic, ms};
  } finally {
    clearTimeout(timer!); client.onmessage = server.onmessage = null; replay.close();
  }
}
Object.assign(window, {bench: {run, async verify(legacy: boolean) {
  const {verifyNegotiation} = await import('./negotiated');
  return verifyNegotiation(connect, fixture(4), legacy);
}, async prepare(count: number) {
  fixture(count);
  if (!link) {
    let timeout: ReturnType<typeof setTimeout>;
    try {
      link = await Promise.race([connect(), new Promise<never>((_, reject) => {
        timeout = setTimeout(() => reject(new Error('RTC connection timed out')), 15_000);
      })]);
    } finally { clearTimeout(timeout!); }
  }
}, close() { link?.close(); }}});
document.getElementById('status')!.textContent = 'Ready';
