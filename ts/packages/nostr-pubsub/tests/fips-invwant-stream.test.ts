import { describe, expect, it } from 'vitest';
import type { Event } from 'nostr-tools/core';
import { finalizeEvent } from 'nostr-tools/pure';
import vectors from '../../../../crates/nostr-pubsub/tests/data/interop-vectors.json';
import {
  FipsInvWantStream,
  allowWithPriority,
  drop,
  verifyNostrEvent,
  type FipsInvWantStreamAction,
  type NostrVerifiedEvent,
} from '../src/index.js';

const vectorEvent = verifyNostrEvent(vectors.events.fipsAdvert as Event);

describe('reliable FIPS inv/want stream records', () => {
  it('roundtrips a record larger than one FSP datagram across partial reads', async () => {
    const alice = stream(256 * 1024);
    const bob = stream(256 * 1024);
    const event = signedEvent('x'.repeat(128 * 1024));

    const inventory = onlyRecord(alice.publish(event, ['bob'], 1), 'bob');
    const want = onlyRecord(
      await bob.receiveBytes('alice', inventory, ['alice'], 2),
      'alice',
    );
    const frame = onlyRecord(
      await alice.receiveBytes('bob', want, ['bob'], 3),
      'bob',
    );

    expect(frame.byteLength).toBeGreaterThan(0xffff);
    expect(new DataView(frame.buffer, frame.byteOffset, 4).getUint32(0, false))
      .toBe(frame.byteLength - 4);
    const first = 2;
    const second = Math.floor(frame.byteLength / 3);
    expect(await bob.receiveBytes('alice', frame.subarray(0, first), ['alice'], 4)).toEqual([]);
    expect(await bob.receiveBytes('alice', frame.subarray(first, second), ['alice'], 5)).toEqual([]);
    const actions = await bob.receiveBytes('alice', frame.subarray(second), ['alice'], 6);
    expect(deliveredIds(actions)).toEqual([event.id]);
  });

  it('retains coalesced records and drains only the configured number per turn', async () => {
    const options = { maxRecordsPerReceive: 1 };
    const alice = new FipsInvWantStream(options);
    const bob = new FipsInvWantStream(options);
    const first = onlyRecord(alice.publish(vectorEvent, ['bob'], 1), 'bob');
    const second = onlyRecord(alice.publish(
      verifyNostrEvent(vectors.events.hashtreeRoot as Event),
      ['bob'],
      2,
    ), 'bob');
    const coalesced = new Uint8Array(first.byteLength + second.byteLength);
    coalesced.set(first);
    coalesced.set(second, first.byteLength);

    expect(sendTargets(await bob.receiveBytes('alice', coalesced, ['alice'], 3)))
      .toEqual(['alice']);
    expect(bob.hasReadyInput('alice')).toBe(true);
    expect(sendTargets(await bob.receiveBytes('alice', new Uint8Array(), ['alice'], 4)))
      .toEqual(['alice']);
    expect(bob.hasReadyInput('alice')).toBe(false);
  });

  it('strictly bounds retained peers, declarations, and buffered input', async () => {
    const service = new FipsInvWantStream({
      maxRecordBytes: 256,
      maxInputPeers: 1,
    });

    expect(await service.receiveBytes('alice', Uint8Array.of(0, 0), [], 1)).toEqual([]);
    await expect(service.receiveBytes('bob', Uint8Array.of(0), [], 2))
      .rejects.toThrow(/input peer limit/);
    service.disconnectPeer('alice');
    await expect(service.receiveBytes('alice', Uint8Array.of(0, 0, 1, 1), [], 3))
      .rejects.toThrow(/declares 257 bytes/);
    expect(service.bufferedInputBytes('alice')).toBe(0);
    await expect(service.receiveBytes(
      'alice',
      new Uint8Array(261),
      [],
      4,
    )).rejects.toThrow(/maximum buffered input/);
    expect(service.bufferedInputBytes('alice')).toBe(0);
  });

  it('replays bounded seeded state to late and reconnected peers', () => {
    const service = new FipsInvWantStream({ mesh: { maxCachedEvents: 1 } });
    service.seed(vectorEvent, 1);
    service.seed(verifyNostrEvent(vectors.events.hashtreeRoot as Event), 2);

    expect(sendTargets(service.peerConnected('late', 3))).toEqual(['late']);
    expect(sendTargets(service.peerConnected('late', 4))).toEqual(['late']);
    expect(service.retainedState().cachedEvents).toBe(1);
  });

  it('applies peer and event policy before queueing, caching, or forwarding', async () => {
    const provider = new FipsInvWantStream().withPeerPolicy({
      selectMeshPeer: (peerId) => ['good', 'consumer'].includes(peerId)
        ? { id: peerId }
        : undefined,
    });
    expect(sendTargets(provider.publish(vectorEvent, ['bad', 'good'], 1))).toEqual(['good']);

    const consumer = new FipsInvWantStream().withEventPolicy({
      checkEvent: () => drop('blocked by test policy'),
      checkSource: () => allowWithPriority(0),
    });
    const inventory = onlyRecord(provider.peerConnected('consumer', 2), 'consumer');
    const want = onlyRecord(
      await consumer.receiveBytes('provider', inventory, ['provider', 'next'], 3),
      'provider',
    );
    const frame = onlyRecord(
      await provider.receiveBytes('consumer', want, ['consumer'], 4),
      'consumer',
    );
    expect(await consumer.receiveBytes('provider', frame, ['provider', 'next'], 5)).toEqual([]);
    expect(consumer.retainedState().cachedEvents).toBe(0);
  });

  it('preserves a configured namespace and rejects unsendable seed state atomically', async () => {
    const left = new FipsInvWantStream({ protocol: 'nvpn.control.pubsub' });
    const right = new FipsInvWantStream({ protocol: 'nvpn.control.pubsub' });
    const inventory = onlyRecord(left.publish(vectorEvent, ['right'], 1), 'right');
    expect(sendTargets(await right.receiveBytes('left', inventory, ['left'], 2)))
      .toEqual(['left']);

    const bounded = new FipsInvWantStream({ maxRecordBytes: 256 });
    expect(() => bounded.seed(signedEvent('x'.repeat(512)), 3)).toThrow(/maximum/);
    expect(bounded.retainedState().cachedEvents).toBe(0);
  });

  it('validates the shared namespace, version, and resource options eagerly', () => {
    expect(() => new FipsInvWantStream({ protocol: '  ' })).toThrow(/protocol/);
    expect(() => new FipsInvWantStream({ protocolVersion: 256 })).toThrow(/version/);
    expect(() => new FipsInvWantStream({ maxRecordBytes: 0 })).toThrow(/record/);
    expect(() => new FipsInvWantStream({ maxInputPeers: 0 })).toThrow(/input peers/);
    expect(() => new FipsInvWantStream({ maxRecordsPerReceive: 0 })).toThrow(/records/);
  });
});

function stream(maxEventBytes: number): FipsInvWantStream {
  return new FipsInvWantStream({
    mesh: {
      maxEventBytes,
      maxCachedEventBytes: maxEventBytes * 4,
    },
    maxRecordBytes: maxEventBytes + 4096,
  });
}

function signedEvent(content: string): NostrVerifiedEvent {
  return verifyNostrEvent(finalizeEvent({
    kind: 1,
    created_at: 1_700_000_000,
    tags: [],
    content,
  }, new Uint8Array(32).fill(7)));
}

function onlyRecord(
  actions: readonly FipsInvWantStreamAction[],
  expectedPeer: string,
): Uint8Array {
  const records = actions.filter(
    (action): action is Extract<FipsInvWantStreamAction, { type: 'send' }> =>
      action.type === 'send',
  );
  expect(records).toHaveLength(1);
  expect(records[0]?.peerId).toBe(expectedPeer);
  return records[0]!.record;
}

function sendTargets(actions: readonly FipsInvWantStreamAction[]): string[] {
  return actions.flatMap((action) => action.type === 'send' ? [action.peerId] : []);
}

function deliveredIds(actions: readonly FipsInvWantStreamAction[]): string[] {
  return actions.flatMap((action) => action.type === 'deliver' ? [action.event.event.id] : []);
}
