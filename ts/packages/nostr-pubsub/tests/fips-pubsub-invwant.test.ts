import { describe, expect, it } from 'vitest';
import { FipsPubsubInvWantState } from '../src/fips-pubsub-invwant.js';

const EVENT_ID = 'ab'.repeat(32);

describe('global FIPS pubsub inventory selection', () => {
  it('sends one WANT for duplicate live or historical inventories from many peers and subs', () => {
    const state = new FipsPubsubInvWantState(16, 8);
    const first = state.accept('peer-a', {
      type: 'inv',
      subscriptionIds: ['history-a', 'live-a'],
      eventId: EVENT_ID,
      eventKind: 1,
      payloadBytes: 512,
      hopLimit: 4,
    }, ['history-a', 'live-a'], 10);
    const duplicatePeer = state.accept('peer-b', {
      type: 'inv',
      subscriptionIds: ['history-b', 'live-b'],
      eventId: EVENT_ID,
      eventKind: 1,
      payloadBytes: 512,
      hopLimit: 4,
    }, ['history-b', 'live-b'], 11);

    expect(first).toEqual({ peerId: 'peer-a', eventId: EVENT_ID });
    expect(duplicatePeer).toBeUndefined();
    expect(state.complete('peer-a', 'live-a', EVENT_ID, 1, 512)).toBe(3);
    expect(state.retryDue(10_000, 1)).toEqual([]);
  });

  it('retries one alternate provider when the selected peer does not answer', () => {
    const state = new FipsPubsubInvWantState(16, 8);
    state.accept('peer-a', {
      type: 'inv', subscriptionIds: ['sub-a'], eventId: EVENT_ID,
      eventKind: 1, payloadBytes: 512, hopLimit: 4,
    }, ['sub-a'], 10);
    state.accept('peer-b', {
      type: 'inv', subscriptionIds: ['sub-b'], eventId: EVENT_ID,
      eventKind: 1, payloadBytes: 512, hopLimit: 4,
    }, ['sub-b'], 11);

    expect(state.retryDue(100, 50)).toEqual([{ peerId: 'peer-b', eventId: EVENT_ID }]);
  });
  it('accepts a queued single-provider TCP response after the retry interval', () => {
    const state = new FipsPubsubInvWantState(16, 8);
    state.accept('peer-a', { type: 'inv', subscriptionIds: ['sub-a'], eventId: EVENT_ID,
      eventKind: 1, payloadBytes: 512, hopLimit: 4 }, ['sub-a'], 10);
    expect(state.retryDue(2000, 500)).toEqual([]);
    expect(state.complete('peer-a', 'sub-a', EVENT_ID, 1, 512)).toBe(3);
  });

  it('accepts the first requested provider even after trying an alternate', () => {
    const state = new FipsPubsubInvWantState(16, 8);
    for (const peer of ['a', 'b']) state.accept(`peer-${peer}`, { type: 'inv', subscriptionIds: [`sub-${peer}`],
      eventId: EVENT_ID, eventKind: 1, payloadBytes: 512, hopLimit: 4 }, [`sub-${peer}`], 10);
    expect(state.retryDue(1000, 500)).toEqual([{ peerId: 'peer-b', eventId: EVENT_ID }]);
    expect(state.complete('peer-a', 'sub-a', EVENT_ID, 1, 512)).toBe(3);
    expect(state.complete('peer-b', 'sub-b', EVENT_ID, 1, 512)).toBeUndefined();
  });

  it('expires unanswered requests and requests an alternative after disconnection', () => {
    const state = new FipsPubsubInvWantState(16, 8);
    for (const peer of ['a', 'b']) state.accept(`peer-${peer}`, { type: 'inv', subscriptionIds: [`sub-${peer}`],
      eventId: EVENT_ID, eventKind: 1, payloadBytes: 512, hopLimit: 4 }, [`sub-${peer}`], 10);
    state.dropPeer('peer-a');
    expect(state.retryDue(1000, 500)).toEqual([{ peerId: 'peer-b', eventId: EVENT_ID }]);
    expect(state.retryDue(10010, 500)).toEqual([]);
    expect(state.complete('peer-b', 'sub-b', EVENT_ID, 1, 512)).toBeUndefined();
  });

});
