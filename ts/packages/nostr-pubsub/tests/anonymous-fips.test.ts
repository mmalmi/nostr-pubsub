import { splitRelayDiscoveryFilters } from '../src/discovery-scope.js';
import { afterEach, expect, it, vi } from 'vitest';
import { finalizeEvent, generateSecretKey, getPublicKey } from 'nostr-tools/pure';
import { createAnonymousFipsPublisher, createAnonymousFipsDiscovery } from '../src/anonymous-fips.js';
import { FipsNostrPubsubClient } from '../src/fips-pubsub-client.js';
import { FipsPubsubWireCodec, type FipsPubsubWireMessage } from '../src/wire.js';
import { FipsPublicationReceipts } from '../src/fips-publication-receipts.js';
import { FipsPrivacyRolePolicy } from '../src/fips-privacy-role.js';
import { verifyNostrEvent } from '../src/types.js';
import { MemoryFipsNetwork } from './helpers/memory-fips.js';

const PROVIDER = `02${'11'.repeat(32)}`;
const clients: FipsNostrPubsubClient[] = [];
afterEach(async () => { await Promise.all(clients.splice(0).map(client => client.stop())); });
const header = ['header', 'encrypted header'];
/** A ratchet message that names no recipient; its receiver finds it by the sender key. */
const envelope = (content: string, tags = [['v', '2'], header], secret = generateSecretKey()) =>
  finalizeEvent({ kind: 1060, tags, content, created_at: 1700000000 }, secret);
/** The same message naming its recipient device in a `p` tag. */
const legacyEnvelope = (p: string, content: string) => envelope(content, [['p', p], ['v', '2'], header]);

it('uses one fresh TCP sender for each copy/retry and waits for the exact provider OK', async () => {
  const network = new MemoryFipsNetwork();
  const peers: string[] = [];
  const provider = new FipsNostrPubsubClient({ node: network.node(PROVIDER), localPeerId: PROVIDER, peers: () => peers, acceptAnonymousPublications: true }).start();
  clients.push(provider);
  const frames: FipsPubsubWireMessage[] = [];
  const decode = provider.codec.decodeFrame.bind(provider.codec);
  vi.spyOn(provider.codec, 'decodeFrame').mockImplementation(frame => { const message = decode(frame); frames.push(message); return message; });
  const closed: string[] = [];
  const source = createAnonymousFipsPublisher({ timeoutMs: 2000, createEndpoint: async () => {
    const id = `02${(peers.length + 30).toString(16).padStart(64, '0')}`;
    peers.push(id);
    return { node: network.node(id), localPeerId: id, peers: () => [PROVIDER], close: async () => { closed.push(id); } };
  } });
  const peer = envelope('peer copy');
  for (const event of [envelope('sibling copy'), peer, peer, legacyEnvelope('a'.repeat(64), 'names its recipient')]) expect((await source.publish(event)).accepted).toBe(true);
  expect(new Set(peers).size).toBe(4); expect(closed).toEqual(peers);
  expect(frames.filter(frame => frame.type === 'event')).toHaveLength(4);
  expect(frames.filter(frame => frame.type === 'req' || frame.type === 'inv' || frame.type === 'want')).toHaveLength(0);
});

it('sends message envelopes with or without a recipient on the anonymous publisher role', () => {
  const publisher = new FipsPrivacyRolePolicy('publisher');
  for (const allowed of [envelope('names no recipient'), legacyEnvelope('a'.repeat(64), 'names one'), envelope('tagged, no header', [['p', 'a'.repeat(64)], ['v', '2']])]) {
    const event = verifyNostrEvent(allowed);
    expect(() => publisher.assertPublisher(event)).not.toThrow();
    expect(() => publisher.outbound({ type: 'event', event })).not.toThrow();
  }
  for (const refused of [envelope('bare shared ciphertext', [['v', '2']]), envelope('old version', [['v', '1'], header]),
    envelope('two recipients', [['p', 'a'.repeat(64)], ['p', 'b'.repeat(64)], ['v', '2'], header])]) {
    const event = verifyNostrEvent(refused);
    expect(() => publisher.assertPublisher(event)).toThrow();
    expect(() => publisher.outbound({ type: 'event', event })).toThrow();
  }
  const invite = (tags: string[][]) => verifyNostrEvent(finalizeEvent({ kind: 1059, tags, content: 'invite response', created_at: 1700000000 }, generateSecretKey()));
  expect(() => publisher.assertPublisher(invite([['p', 'a'.repeat(64)]]))).not.toThrow();
  expect(() => publisher.assertPublisher(invite([]))).toThrow();
  expect(() => publisher.assertPublisher(invite([['p', 'a'.repeat(64)], ['p', 'b'.repeat(64)]]))).toThrow();
});

it('forwards message envelopes with or without a recipient through a provider', async () => {
  const network = new MemoryFipsNetwork();
  const older = `02${'44'.repeat(32)}`, reader = `03${'55'.repeat(32)}`;
  const start = (localPeerId: string, peers: string[], options = {}) => {
    const client = new FipsNostrPubsubClient({ node: network.node(localPeerId), localPeerId, peers: () => peers, ...options }).start();
    clients.push(client); return client;
  };
  const provider = start(PROVIDER, [older, reader], { acceptAnonymousPublications: true });
  const subscriber = start(reader, [PROVIDER], { role: 'receiver' });
  const oldClient = start(older, [PROVIDER]);
  const settle = async () => { for (let turn = 0; turn < 4; turn++) await Promise.all([provider, subscriber, oldClient].map(client => client.idle())); };
  const recipient = 'a'.repeat(64), senderSecret = generateSecretKey();
  const received: string[] = [];
  subscriber.subscribe([{ kinds: [1060], '#p': [recipient] }, { kinds: [1060], authors: [getPublicKey(senderSecret)] }], event => { received.push(event.id); });
  await settle();
  expect(provider.peerSubscriptionCount(reader)).toBe(1);
  const legacy = legacyEnvelope(recipient, 'names its recipient');
  const current = envelope('names no recipient', undefined, senderSecret);
  const acknowledged: string[] = [];
  const decode = oldClient.codec.decodeFrame.bind(oldClient.codec);
  vi.spyOn(oldClient.codec, 'decodeFrame').mockImplementation(frame => {
    const message = decode(frame); if (message.type === 'ok' && message.accepted) acknowledged.push(message.eventId); return message;
  });
  // The provider admits whatever a sender submits; it has no envelope shape check of its own.
  const raw = oldClient as unknown as { connectPeer(peer: string): void; send(peer: string, message: FipsPubsubWireMessage): Promise<void> };
  raw.connectPeer(PROVIDER);
  const send = raw.send.bind(oldClient);
  for (const event of [legacy, current]) { await send(PROVIDER, { type: 'event', event: verifyNostrEvent(event) }); await settle(); }
  expect(acknowledged).toEqual([legacy.id, current.id]);
  expect(received.sort()).toEqual([legacy.id, current.id].sort());
});

it('does not treat local cache/transport success as remote acceptance and releases endpoints', async () => {
  const network = new MemoryFipsNetwork();
  const sender = `02${'22'.repeat(32)}`;
  const provider = new FipsNostrPubsubClient({ node: network.node(PROVIDER), localPeerId: PROVIDER, peers: () => [sender] }).start();
  clients.push(provider);
  let closed = 0;
  const source = createAnonymousFipsPublisher({ timeoutMs: 60, createEndpoint: async () => ({ node: network.node(sender), localPeerId: sender, peers: () => [PROVIDER], close: async () => { closed++; } }) });
  await expect(source.publish(envelope('pending'))).rejects.toThrow(/receipt timed out/);
  expect(closed).toBe(1);
  await expect(source.publish(envelope('reuse forbidden'))).rejects.toThrow(/reused/);
});

it('binds receipts to the pending event and explicit provider, and cancels cleanly', async () => {
  const receipts = new FipsPublicationReceipts();
  const event = verifyNostrEvent(envelope('binding'));
  const abort = new AbortController();
  const pending = receipts.publish(event, [PROVIDER], async () => {}, { timeoutMs: 1000, signal: abort.signal });
  receipts.receive('unknown', event.id, true, '');
  receipts.receive(PROVIDER, 'f'.repeat(64), true, '');
  abort.abort();
  await expect(pending).rejects.toThrow(/aborted/);
});

it('keeps peer adverts, profiles and invite responses on one anonymous subject scope', () => {
  const author = 'a'.repeat(64);
  const scope = new FipsPrivacyRolePolicy({ discovery: [{ kinds: [37195], authors: [author] }] });
  expect(() => scope.filters([{ kinds: [0, 1059, 37195], authors: [author] }])).not.toThrow();
  expect(() => scope.filters([{ kinds: [0], authors: ['b'.repeat(64)] }])).toThrow();
  const receiver = new FipsPrivacyRolePolicy('receiver');
  expect(() => receiver.filters([{ kinds: [37195], authors: [author] }])).toThrow();
  expect(() => receiver.filters([{ kinds: [1059], '#p': [author] }])).not.toThrow();
  expect(() => receiver.filters([{ kinds: [1060], authors: [author] }])).not.toThrow();
  expect(() => receiver.filters([{ kinds: [1060] }])).toThrow();
  for (const cached of [envelope('cached'), legacyEnvelope(author, 'cached')]) {
    expect(() => receiver.outbound({ type: 'event', event: verifyNostrEvent(cached) })).toThrow();
    expect(() => new FipsPrivacyRolePolicy('full').outbound({ type: 'event', event: verifyNostrEvent(cached) })).not.toThrow();
  }
  expect(() => receiver.outbound({ type: 'inv', eventId: 'a'.repeat(64), eventKind: 1060, payloadBytes: 500, hopLimit: 1, subscriptionIds: ['stranger'] })).toThrow();
});

it('validates anonymous discovery before allocating an endpoint', async () => {
  const createEndpoint = vi.fn();
  const source = createAnonymousFipsDiscovery({ createEndpoint });
  await expect(source.query([{ kinds: [0], authors: ['a'.repeat(64), 'b'.repeat(64)] }])).rejects.toThrow();
  expect(createEndpoint).not.toHaveBeenCalled();
});

it('roundtrips the standard Nostr provider receipt exactly', () => {
  const codec = new FipsPubsubWireCodec();
  const message = { type: 'ok', eventId: 'a'.repeat(64), accepted: true, message: '' } as const;
  expect(new TextDecoder().decode(codec.encodeFrame(message))).toBe(JSON.stringify(['OK', message.eventId, true, '']));
  expect(codec.decodeFrame(codec.encodeFrame(message))).toEqual(message);
});

it('rejects unapproved public and unknown authored kinds on an identified receiver', () => {
  const author = 'a'.repeat(64);
  const receiver = new FipsPrivacyRolePolicy('receiver');
  expect(() => receiver.filters([{ kinds: [1], authors: [author] }])).toThrow();
  expect(() => receiver.filters([{ kinds: [54321], authors: [author] }])).toThrow();
  const publicFeed = new FipsPrivacyRolePolicy('receiver', () => [author]);
  expect(() => publicFeed.filters([{ kinds: [1], authors: [author] }])).not.toThrow();
});

it('keeps mixed invite inbox and reverse-device lookup filters on their own roles', () => {
  const key = 'a'.repeat(64);
  const split = splitRelayDiscoveryFilters([{ kinds: [1059, 37368], '#p': [key] }]);
  expect(split.receiver).toEqual([{ kinds: [1059], '#p': [key] }]);
  expect(split.discovery).toEqual([[{ kinds: [37368], '#p': [key] }]]);
});

it('blocks cached public profile, endpoint advert and future-kind exports by unapproved authors', () => {
  const own = 'a'.repeat(64);
  const receiver = new FipsPrivacyRolePolicy('receiver', () => [own]);
  for (const kind of [0, 37195, 54321]) {
    const event = verifyNostrEvent(finalizeEvent({ kind, tags: [], content: 'private lookup cache', created_at: 1700000000 }, generateSecretKey()));
    expect(() => receiver.outbound({ type: 'event', event })).toThrow();
    expect(() => receiver.outbound({ type: 'inv', eventId: event.id, eventKind: kind, payloadBytes: 500, hopLimit: 1, subscriptionIds: ['stranger'] })).toThrow();
  }
});

it('never exports private lookup authors from live cache or retained restart replay over actual TCP', async () => {
  for (const primeCache of [true, false]) {
    const network = new MemoryFipsNetwork();
    const receiverId = `02${'44'.repeat(32)}`;
    const requesterId = `02${'55'.repeat(32)}`;
    const ownSecret = generateSecretKey();
    const approved = verifyNostrEvent(finalizeEvent({ kind: 1, tags: [], content: 'approved public head', created_at: 1700000000 }, ownSecret));
    const foreign = [0, 37195, 54321].map(kind => verifyNostrEvent(finalizeEvent({ kind, tags: [], content: 'private lookup cache', created_at: 1700000000 }, generateSecretKey())));
    const receiver = new FipsNostrPubsubClient({ node: network.node(receiverId), localPeerId: receiverId, peers: () => [requesterId], role: 'receiver', receiverPublicAuthors: () => [approved.pubkey],
      retainedEventReader: { query: async () => ({ complete: true, events: [...foreign, approved].map(event => ({ event, source: { kind: 'local-index', id: 'shared-history' }, priority: 0 })) }) },
    }).start();
    const requester = new FipsNostrPubsubClient({ node: network.node(requesterId), localPeerId: requesterId, peers: () => [receiverId] }).start();
    clients.push(receiver, requester);
    const kinds = [0, 1, 37195, 54321];
    if (primeCache) {
      const seeded: string[] = [];
      const seed = receiver.subscribe([{ kinds }], event => seeded.push(event.id));
      await vi.waitFor(() => expect(requester.peerSubscriptionCount(receiverId)).toBe(1));
      for (const event of foreign) await requester.publish(event);
      await vi.waitFor(() => expect(seeded).toHaveLength(3));
      seed.close();
    }
    const frames: FipsPubsubWireMessage[] = [];
    const decode = requester.codec.decodeFrame.bind(requester.codec);
    vi.spyOn(requester.codec, 'decodeFrame').mockImplementation(frame => { const message = decode(frame); frames.push(message); return message; });
    const received: string[] = [];
    requester.subscribe([{ kinds }], event => received.push(event.id));
    await vi.waitFor(() => expect(received).toContain(approved.id));
    const raw = requester as unknown as { send(peer: string, message: FipsPubsubWireMessage): Promise<void> };
    for (const event of foreign) await raw.send(receiverId, { type: 'want', eventId: event.id });
    for (let turn = 0; turn < 4; turn++) { await receiver.idle(); await requester.idle(); }
    expect(received).toEqual([approved.id]);
    const foreignIds = new Set(foreign.map(event => event.id));
    expect(frames.filter(frame => frame.type === 'event' ? foreignIds.has(frame.event.id) : frame.type === 'inv' && foreignIds.has(frame.eventId))).toEqual([]);
    await receiver.stop(); await requester.stop();
  }
});
