import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { expect, it, vi } from 'vitest';
import { FipsNostrPubsubClient, FipsNostrPubsubEventSource, localIndexSource, openPublicPeerFeedSource, verifyNostrEvent, type FipsPubsubWireMessage } from '../src/index.js';
import { FeedSync, FeedWindow } from '../src/fips-feed-sync.js';
import { MemoryFipsNetwork } from './helpers/memory-fips.js';

const a = '02' + '11'.repeat(32), b = '03' + '22'.repeat(32), offline = '02' + '33'.repeat(32);
const key = generateSecretKey();
const post = (content: string, tags: string[][] = []) => verifyNostrEvent(finalizeEvent({ kind: 1, tags, created_at: 100, content }, key));
const codecFrames = (client: FipsNostrPubsubClient) => {
  const frames: FipsPubsubWireMessage[] = [];
  const encode = client.codec.encodeFrame.bind(client.codec);
  vi.spyOn(client.codec, 'encodeFrame').mockImplementation(msg => { frames.push(msg); return encode(msg); });
  return frames;
};
it('two upgraded FIPS nodes negotiate once on warm reconnect, announce only actual gaps and keep live publishing', async () => {
  const network = new MemoryFipsNetwork(), an = network.node(a), bn = network.node(b);
  let connected = true;
  const alice = new FipsNostrPubsubClient({ node: an, localPeerId: a, peers: () => connected ? [b] : [] }).start();
  const bob = new FipsNostrPubsubClient({ node: bn, localPeerId: b, peers: () => connected ? [a] : [] }).start();
  const ar = new FipsNostrPubsubEventSource(alice), br = new FipsNostrPubsubEventSource(bob);
  const tx = codecFrames(alice), rx = codecFrames(bob);
  const settle = () => Promise.all([alice.idle(), bob.idle()]);
  const route = async (up: boolean) => {
    connected = up;
    an.emit('peer', { state: up ? 'connected' : 'disconnected', remotePubkey: b });
    bn.emit('peer', { state: up ? 'connected' : 'disconnected', remotePubkey: a });
    await settle();
  };
  const first = post('first'), missed = post('offline'), live = post('live'), browsed = post('browsed only');
  const peer = await openPublicPeerFeedSource(ar, { type: 'latest' });
  const other = await openPublicPeerFeedSource(br, { type: 'latest' });
  try {
    await vi.waitFor(() => expect(bob.peerSubscriptionCount(a)).toBe(1));
    await alice.publish(first);
    await vi.waitFor(async () => expect((await other.query!([{}])).events).toHaveLength(1));
    expect([...tx,...rx].filter(f => f.type === 'sync')).toHaveLength(0);
    await peer.query!([{ ids: [browsed.id] }, { authors: [browsed.pubkey] }]);
    await route(false);
    await expect(alice.publish(missed)).rejects.toThrow('no admitted');
    tx.length = rx.length = 0;
    await route(true);
    await vi.waitFor(async () => expect((await other.query!([{}])).events.map(r => r.event.id)).toContain(missed.id));
    await vi.waitFor(() => expect(tx.filter(f => f.type === 'sync' && f.action === 'end')).toHaveLength(1));
    expect(tx.filter(f => f.type === 'inv').map(f => f.eventId)).toEqual([missed.id]);
    expect(rx.filter(f => f.type === 'want').map(f => f.eventId)).toEqual([missed.id]);
    expect(rx.filter(f => f.type === 'req').every(f => f.filters.every(f => JSON.stringify(f) === '{"kinds":[1]}'))).toBe(true);
    expect(JSON.stringify([...rx,...tx])).not.toContain(browsed.id);
    await route(false); tx.length = rx.length = 0; await route(true);
    await vi.waitFor(() => expect(tx.filter(f => f.type === 'sync' && f.action === 'end')).toHaveLength(1));
    expect([...tx,...rx].filter(f => f.type === 'inv' || f.type === 'want')).toEqual([]);
    await other.publish!(live, localIndexSource('outbox'));
    await vi.waitFor(async () => expect((await peer.query!([{}])).events.map(r => r.event.id)).toContain(live.id));
  } finally { peer.close(); other.close(); await Promise.all([alice.stop(), bob.stop()]); }
});

it('rejects unsolicited opens, bounds sessions and falls back once on a bad response or deadline', async () => {
  vi.useFakeTimers();
  try {
    const send = vi.fn(), sync = new FeedSync(send, 65_525, 2);
    const window = new FeedWindow({ kinds: [1] }, 256), event = post('public');
    window.remember(event);
    const finish = vi.fn(), first = 'wf1-ts-1', second = 'wf1-ts-2';
    await sync.receive(a, { type: 'sync', action: 'open', subscriptionId: first, payload: 'ff' }, undefined, () => true);
    expect(send).not.toHaveBeenCalled();
    await sync.offer(a, first, window, [event], finish);
    await sync.receive(a, { type: 'sync', action: 'reply', subscriptionId: first, payload: 'ff' }, undefined, () => false);
    expect(finish).toHaveBeenCalledExactlyOnceWith([event], true);
    const fallback = vi.fn();
    await sync.offer(a, first, window, [event], fallback);
    await sync.offer(a, second, window, [event], fallback);
    await sync.offer(b, first, window, [event], fallback);
    expect(fallback).toHaveBeenCalledTimes(1);
    sync.cancel(a, first);
    await vi.advanceTimersByTimeAsync(3000);
    expect(fallback).toHaveBeenCalledTimes(2);
    await vi.advanceTimersByTimeAsync(3000);
    expect(fallback).toHaveBeenCalledTimes(2);
  } finally { vi.useRealTimers(); }
});

it('does not expose a raw lookup via retained replay, fingerprints, guessed WANT, or suppressing a real feed WANT', async () => {
  const network = new MemoryFipsNetwork(), origin = localIndexSource('unrelated-history');
  const unrelated = post('read in navigation'), approved = post('intentionally published'), query = vi.fn(async () => ({
    events: [{ event: unrelated, source: origin, priority: 1 }], complete: true }));
  const alice = new FipsNostrPubsubClient({ node: network.node(a), localPeerId: a, peers: () => [b], retainedEventReader: { query } }).start();
  const bob = new FipsNostrPubsubClient({ node: network.node(b), localPeerId: b, peers: () => [a] }).start();
  const ar = new FipsNostrPubsubEventSource(alice), br = new FipsNostrPubsubEventSource(bob);
  const tx = codecFrames(alice), rx = codecFrames(bob);
  const inject = (client: FipsNostrPubsubClient, peer: string, msg: FipsPubsubWireMessage) => {
    const raw = new TextEncoder().encode(JSON.stringify(msg.type === 'want' ? ['WANT', msg.eventId] :
      msg.type === 'req' ? ['REQ', msg.subscriptionId, ...msg.filters] : []));
    (client as unknown as { handleFrame(id: string, frame: Uint8Array): void }).handleFrame(peer, raw);
  };
  try {
    const lookup = bob.subscribe([{ ids: [unrelated.id] }], () => {});
    await vi.waitFor(() => expect(rx.some(m => m.type === 'want' && m.eventId === unrelated.id)).toBe(true));
    await Promise.all([alice.idle(), bob.idle()]);
    lookup.close();
    await vi.waitFor(() => expect(alice.peerSubscriptionCount(b)).toBe(0));
    tx.length = rx.length = 0; query.mockClear();
    const af = await openPublicPeerFeedSource(ar, { type: 'latest' }), bf = await openPublicPeerFeedSource(br, { type: 'latest' });
    await vi.waitFor(() => expect(alice.peerSubscriptionCount(b)).toBe(1));
    expect(query).not.toHaveBeenCalled();
    inject(alice, b, { type: 'want', eventId: unrelated.id });
    inject(alice, b, { type: 'req', subscriptionId: 'wf1-ts-z', filters: [{ ids: [unrelated.id] }] });
    await Promise.all([alice.idle(), bob.idle()]);
    expect(tx.some(m => JSON.stringify(m).includes(unrelated.id))).toBe(false);
    const received = vi.fn(); await bf.subscribe!([{}], received);
    await alice.publish(approved);
    await vi.waitFor(() => expect(received).toHaveBeenCalledOnce());
    // The same signed note is now independently published; the earlier private read cannot alter Bob's feed WANT.
    await alice.publish(unrelated);
    await vi.waitFor(() => expect(rx.some(m => m.type === 'want' && m.eventId === unrelated.id)).toBe(true));
    await vi.waitFor(() => expect(received).toHaveBeenCalledTimes(2));
    af.close(); bf.close();
  } finally { await Promise.all([alice.stop(), bob.stop()]); }
});

it('rejects invalid and oversized feed control messages at the codec boundary', () => {
  const { codec } = new FipsNostrPubsubClient({ node: new MemoryFipsNetwork().node(a), localPeerId: a, peers: () => [] });
  for (const frame of [
    ['FEED-SYNC/1', 'open', 'ordinary-sub', '61'], ['FEED-SYNC/1', 'open', 'wf1-ts-1', '6'],
    ['FEED-SYNC/1', 'open', 'wf1-ts-1', 'zz'], ['FEED-SYNC/1', 'end', 'wf1-ts-1', '61'],
    ['FEED-SYNC/1', 'step', 'wf1-ts-1', '01'.repeat(8193)], ['FEED-SYNC/1', 'reply', 'wf1-ts-1', '61', 'extra'],
  ]) expect(() => codec.decodeFrame(new TextEncoder().encode(JSON.stringify(frame)))).toThrow('Invalid public feed sync frame');
});

it.skipIf(!process.env.NOSTR_PUBSUB_LEGACY_MODULE)('serves legacy unmarked feeds and rejoins a genuine legacy provider with ordinary inventory', async () => {
  const legacy = await import(/* @vite-ignore */ process.env.NOSTR_PUBSUB_LEGACY_MODULE!);
  const network = new MemoryFipsNetwork(), an = network.node(a), bn = network.node(b);
  let up = true;
  const newer = new FipsNostrPubsubClient({ node: an, localPeerId: a, peers: () => [up ? b : offline] }).start();
  const older = new legacy.FipsNostrPubsubClient({ node: bn, localPeerId: b, peers: () => [up ? a : offline] }).start();
  const tx = codecFrames(newer), rx = codecFrames(older), one = post('old to new'), two = post('new to old'), gap = post('missed');
  const n = await openPublicPeerFeedSource(new FipsNostrPubsubEventSource(newer), { type: 'latest' });
  const o = await legacy.openPublicPeerFeedSource(new legacy.FipsNostrPubsubEventSource(older), { type: 'latest' });
  try {
    await vi.waitFor(() => expect([newer.peerSubscriptionCount(b), older.peerSubscriptionCount(a)]).toEqual([1,1]));
    await newer.publish(two); await older.publish(one);
    await vi.waitFor(async () => {
      expect((await n.query!([{}])).events.map(r => r.event.id)).toContain(one.id);
      expect((await o.query([{}])).events.map((r: {event:{id:string}}) => r.event.id)).toContain(two.id);
    });
    up = false;
    an.emit('peer', { state: 'disconnected', remotePubkey: b }); bn.emit('peer', { state: 'disconnected', remotePubkey: a });
    await Promise.all([newer.idle(), older.idle()]); await older.publish(gap);
    tx.length = rx.length = 0;
    up = true;
    an.emit('peer', { state: 'connected', remotePubkey: b }); bn.emit('peer', { state: 'connected', remotePubkey: a });
    await vi.waitFor(async () => expect((await n.query!([{}])).events.map(r => r.event.id)).toContain(gap.id));
    expect(tx.some(m => m.type === 'req' && m.subscriptionId.startsWith('wf1-'))).toBe(true);
    expect([...tx,...rx].filter(m => m.type === 'sync')).toEqual([]);
  } finally { n.close(); o.close(); await Promise.all([newer.stop(), older.stop()]); }
});

it('delivers a warm gap larger than one replay or transport queue while concurrent live posts arrive', async () => {
  const network = new MemoryFipsNetwork(), an = network.node(a), bn = network.node(b);
  let up = true;
  const bounds = { maxActiveSubscriptions: 8, maxReplayEvents: 4 };
  const alice = new FipsNostrPubsubClient({ node: an, localPeerId: a, peers: () => up ? [b] : [], limits: bounds }).start();
  const bob = new FipsNostrPubsubClient({ node: bn, localPeerId: b, peers: () => up ? [a] : [], limits: bounds }).start();
  const as = await openPublicPeerFeedSource(new FipsNostrPubsubEventSource(alice), { type: 'latest' });
  const bs = await openPublicPeerFeedSource(new FipsNostrPubsubEventSource(bob), { type: 'latest' });
  const updates = vi.fn(); await bs.subscribe!([{}], updates);
  try {
    const first = post('seed'); await vi.waitFor(() => expect(alice.peerSubscriptionCount(b)).toBe(1));
    await alice.publish(first); await vi.waitFor(() => expect(updates).toHaveBeenCalledOnce());
    const route = async (open: boolean) => {
      up = open; const state = up ? 'connected' : 'disconnected';
      an.emit('peer', { state, remotePubkey: b }); bn.emit('peer', { state, remotePubkey: a });
      await Promise.all([alice.idle(), bob.idle()]);
    };
    await route(false);
    const events = Array.from({length: 56}, (_, i) => post('outbox-' + i));
    for (const event of events) await expect(alice.publish(event)).rejects.toThrow('no admitted');
    const tx = codecFrames(alice);
    await route(true);
    await vi.waitFor(() => expect(tx.some(m => m.type === 'sync' && m.action === 'open')).toBe(true));
    const live = post('live alongside catch-up'); await alice.publish(live);
    await vi.waitFor(async () => expect(new Set((await bs.query!([{}])).events.map(r => r.event.id)))
      .toEqual(new Set([first,live,...events].map(e => e.id))), { timeout: 5000 });
    expect(updates).toHaveBeenCalledTimes(58);
  } finally { as.close(); bs.close(); await Promise.all([alice.stop(), bob.stop()]); }
});
