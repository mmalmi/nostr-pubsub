import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { expect, it, vi } from 'vitest';
import {
  FipsNostrPubsubClient, FipsNostrPubsubEventSource, localIndexSource,
  publicPeerSource, subscribePublicPeerFeed, type NostrFilter, type QueryEvent, type RuntimeSource,
} from '../src/index.js';
import { MemoryFipsNetwork } from './helpers/memory-fips.js';

const a = '02' + '11'.repeat(32), b = '03' + '22'.repeat(32);
const sign = (kind: number, tags: string[][] = []) =>
  finalizeEvent({ kind, tags, created_at: Math.floor(Date.now() / 1000), content: '' }, generateSecretKey());

it('opens only the independently selected feed on the peer and delivers a public publication', async () => {
  const network = new MemoryFipsNetwork();
  const reader = new FipsNostrPubsubClient({ node: network.node(a), localPeerId: a, peers: () => [b] }).start();
  const author = new FipsNostrPubsubClient({ node: network.node(b), localPeerId: b, peers: () => [a] }).start();
  try {
    const post = sign(1), contacts = sign(3, [['p', post.pubkey]]);
    const raw = new FipsNostrPubsubEventSource(reader), app = publicPeerSource(raw);
    const viewed: NostrFilter[] = [{ ids: [post.id] }, { authors: [post.pubkey], kinds: [0] },
      { kinds: [1], authors: [post.pubkey] }, { '#e': [post.id] }, { kinds: [1] }];
    const sent = vi.spyOn(reader, 'subscribe');
    await app.query!(viewed);
    const view = await app.subscribe!(viewed, vi.fn());
    view.close();
    expect(sent).not.toHaveBeenCalled();
    const receive = vi.fn();
    const feed = await subscribePublicPeerFeed(raw, { type: 'following', contacts, owner: contacts.pubkey }, receive);
    await vi.waitFor(() => expect(author.peerSubscriptionCount(a)).toBe(1));
    expect(sent.mock.calls.map(args => args[0])).toEqual([[{ kinds: [1], authors: [post.pubkey] }]]);
    await app.query!([{ ids: [post.id] }]);
    const publishing = publicPeerSource(new FipsNostrPubsubEventSource(author));
    expect(publishing.publishAcceptance).toBe('queued');
    await publishing.publish!(post, localIndexSource('outbox'));
    await vi.waitFor(() => expect(receive.mock.calls[0]?.[0].event.id).toBe(post.id));
    expect(sent).toHaveBeenCalledTimes(1);
    feed.close();
  } finally {
    await Promise.all([reader.stop(), author.stop()]);
  }
});

it('requires verified public follows and sends no global fallback for an empty list', async () => {
  const subscribe = vi.fn(() => ({ close() {} })), source: RuntimeSource = { id: 'test', subscribe };
  const contacts = sign(3), handler = vi.fn();
  await subscribePublicPeerFeed(source, { type: 'following', owner: contacts.pubkey, contacts }, handler);
  await expect(subscribePublicPeerFeed(source, { type: 'following', owner: a, contacts }, handler)).rejects.toThrow();
  await expect(subscribePublicPeerFeed(source, { type: 'following', owner: contacts.pubkey,
    contacts: { ...contacts, tags: [['p', b.slice(2)]] } }, handler)).rejects.toThrow();
  expect(subscribe).not.toHaveBeenCalled();
  await subscribePublicPeerFeed(source, { type: 'latest' }, handler);
  expect(subscribe).toHaveBeenCalledWith([{ kinds: [1] }], expect.any(Function));
});

it('matches exact lookups locally and drops old history and late callbacks when the feed rotates', async () => {
  const { openPublicPeerFeedSource } = await import('../src/index.js');
  const subscriptions: Array<{close: ReturnType<typeof vi.fn>; push: (event: QueryEvent) => void}> = [];
  const wirePublish = vi.fn(async () => ({ accepted: true, priority: 10 }));
  const source: RuntimeSource = { id: 'fips', publish: wirePublish, subscribe: (_, push) => {
    const s = { close: vi.fn(), push }; subscriptions.push(s); return s;
  } };
  const feed = await openPublicPeerFeedSource(source, { type: 'latest' });
  const post = sign(1), info = { event: post, source: localIndexSource('test'), priority: 1 };
  const handler = vi.fn();
  const handle = await feed.subscribe!([{ ids: [post.id] }], handler);
  subscriptions[0]!.push(info);
  expect(handler).toHaveBeenCalledOnce();
  expect((await feed.query!([{ authors: [post.pubkey] }])).events.map(e => e.event.id)).toEqual([post.id]);
  expect(subscriptions).toHaveLength(1);
  const contacts = sign(3);
  await feed.replaceFeed({ type: 'following', contacts, owner: contacts.pubkey });
  expect(subscriptions[0]!.close).toHaveBeenCalledOnce();
  subscriptions[0]!.push(info);
  expect(handler).toHaveBeenCalledOnce();
  expect((await feed.query!([{ ids: [post.id] }])).events).toEqual([]);
  await expect(feed.publish!(sign(1059), localIndexSource('outbox'))).rejects.toThrow('Private event');
  expect(wirePublish).not.toHaveBeenCalled();
  await feed.replaceFeed({ type: 'latest' });
  subscriptions[1]!.push(info);
  expect(handler).toHaveBeenCalledTimes(2);
  handle.close();
  feed.close();
  subscriptions[1]!.push(info);
  expect(handler).toHaveBeenCalledTimes(2);
  expect(subscriptions[1]!.close).toHaveBeenCalledOnce();
});

it('batches every signed follow deterministically and cleans up when a carrier rejects a batch', async () => {
  const authors = Array.from({ length: 600 }, (_, i) => (i + 1).toString(16).padStart(64, '0'));
  const contacts = sign(3, [...authors].reverse().map(author => ['p', author]).concat([
    ['p', authors[0]!], ['p', 'invalid'], ['e', 'ff'.repeat(32)],
  ]));
  const feed = { type: 'following' as const, owner: contacts.pubkey, contacts };
  const close = vi.fn(), filters: NostrFilter[] = [];
  const subscribe = vi.fn((batch: NostrFilter[]) => {
    filters.push(...batch);
    if (filters.length === 2) throw new Error('rejected');
    return { close };
  });
  await expect(subscribePublicPeerFeed({ id: 'fips', subscribe }, feed, vi.fn())).rejects.toThrow('rejected');
  expect(filters.map(filter => filter.authors!.length)).toEqual([256, 256, 88]);
  expect(filters.flatMap(filter => filter.authors!)).toEqual(authors);
  expect(close).toHaveBeenCalledTimes(2);
});
