import { describe, expect, it } from 'vitest';
import type { Event } from 'nostr-tools/core';
import vectors from '../../../../crates/nostr-pubsub/tests/data/interop-vectors.json';
import {
  InvWantCodec,
  InvWantMesh,
  meshEventJsonBytes,
  meshPeer,
  selectMeshPeers,
  verifyNostrEvent,
  type InvWantAction,
  type InvWantWireMessage,
} from '../src/index.js';

const rawEvent = vectors.events.fipsAdvert as Event;
const event = verifyNostrEvent(rawEvent);
const protocol = 'iris.fips.pubsub';

describe('bounded inv/want mesh', () => {
  it('encodes the deployed Rust JSON envelope exactly', () => {
    for (const testCase of vectors.meshWireCases) {
      const codec = new InvWantCodec(testCase.protocol, testCase.version, 64 * 1024);
      const message = wireMessageFromVector(testCase.message);
      const encoded = codec.encode(message);
      expect(new TextDecoder().decode(encoded), testCase.name).toBe(testCase.json);
      expect(codec.decode(encoded)).toEqual(message);
    }
  });

  it('delivers once across three hops and never amplifies an unproven inventory', () => {
    const alice = mesh();
    const bob = mesh();
    const carol = mesh();

    const inventoryForBob = onlyMessage(alice.publish(event, [meshPeer('bob')], 1));
    const bobActions = bob.receive(
      'alice',
      inventoryForBob,
      [meshPeer('alice'), meshPeer('carol')],
      2,
    );
    expect(bobActions).toEqual([
      { type: 'send', peerId: 'alice', message: { type: 'want', eventId: event.id } },
    ]);

    const frameForBob = onlyMessage(alice.receive('bob', onlyMessage(bobActions), [], 3));
    const bobFrameActions = bob.receive(
      'alice',
      frameForBob,
      [meshPeer('alice'), meshPeer('carol')],
      4,
    );
    expect(deliveredIds(bobFrameActions)).toEqual([event.id]);
    const inventoryForCarol = messageFor(bobFrameActions, 'carol');
    const carolActions = carol.receive('bob', inventoryForCarol, [meshPeer('bob')], 5);
    const frameForCarol = onlyMessage(
      bob.receive('carol', onlyMessage(carolActions), [meshPeer('carol')], 6),
    );
    expect(deliveredIds(carol.receive('bob', frameForCarol, [meshPeer('bob')], 7))).toEqual([
      event.id,
    ]);
    expect(carol.receive('bob', frameForCarol, [meshPeer('bob')], 8)).toEqual([]);
    carol.maintain(120_008);
    expect(() => carol.receive('bob', frameForCarol, [meshPeer('bob')], 120_009))
      .toThrow(/unsolicited/);
  });

  it('uses bounded alternate providers to recover a blackhole', () => {
    const consumer = mesh();
    const inventory = inventoryFor(event, 4);
    expect(() => consumer.receive(
      'unsolicited',
      { type: 'frame', eventId: event.id, event },
      [],
      1,
    )).toThrow(/unsolicited/);

    for (const [nowMs, provider, hopLimit] of [
      [2, 'blackhole', 2], [3, 'honest', 4], [4, 'backup', 3],
    ] as const) {
      expect(consumer.receive(provider, inventoryFor(event, hopLimit), [], nowMs)).toEqual([
        { type: 'send', peerId: provider, message: { type: 'want', eventId: event.id } },
      ]);
    }
    expect(consumer.receive('excess', inventory, [], 5)).toEqual([]);
    expect(() => consumer.receive(
      'excess',
      { type: 'frame', eventId: event.id, event },
      [],
      6,
    )).toThrow(/not requested/);
    expect(deliveredIds(consumer.receive(
      'honest',
      { type: 'frame', eventId: event.id, event },
      [],
      7,
    ))).toEqual([event.id]);
    expect(consumer.receive(
      'backup',
      { type: 'frame', eventId: event.id, event },
      [],
      8,
    )).toEqual([]);

    const lateAlternate = mesh({ routeTtlMs: 10, eventTtlMs: 20 });
    lateAlternate.receive('blackhole', inventory, [], 1);
    lateAlternate.receive('honest', inventory, [], 9);
    expect(deliveredIds(lateAlternate.receive(
      'honest',
      { type: 'frame', eventId: event.id, event },
      [],
      15,
    ))).toEqual([event.id]);
  });

  it('absorbs late requested frames without poisoning provider behavior', () => {
    const signedEvents = Object.values(vectors.events)
      .map((candidate) => verifyNostrEvent(candidate as Event));
    const consumer = new InvWantMesh({
      maxHops: 4,
      routeTtlMs: 10,
      eventTtlMs: 20,
      allowedKinds: new Set(signedEvents.map((candidate) => candidate.kind)),
    });
    for (const [sequence, signedEvent] of signedEvents.slice(0, 3).entries()) {
      const nowMs = sequence * 4 + 1;
      consumer.receive('primary', inventoryFor(signedEvent, 2), [], nowMs);
      consumer.receive('alternate', inventoryFor(signedEvent, 4), [], nowMs + 1);
      expect(deliveredIds(consumer.receive(
        'primary', { type: 'frame', eventId: signedEvent.id, event: signedEvent }, [], nowMs + 2,
      ))).toEqual([signedEvent.id]);
      expect(consumer.receive(
        'alternate', { type: 'frame', eventId: signedEvent.id, event: signedEvent }, [], nowMs + 3,
      )).toEqual([]);
    }
    consumer.maintain(30);
    expect(consumer.peerBehaviorObservation('alternate')).toBeUndefined();
    expect(consumer.peerBehaviorObservation('primary')).toMatchObject({
      validFrames: 3, invalidMessages: 0, unservedInventories: 0,
    });

    const rejected = signedEvents[3];
    consumer.receive('primary', inventoryFor(rejected, 2), [], 31);
    consumer.receive('alternate', inventoryFor(rejected, 4), [], 32);
    consumer.dismissFrame('primary', rejected.id);
    expect(consumer.receive(
      'alternate', { type: 'frame', eventId: rejected.id, event: rejected }, [], 33,
    )).toEqual([]);
    expect(() => consumer.receive(
      'unrequested', { type: 'frame', eventId: rejected.id, event: rejected }, [], 34,
    )).toThrow(/not requested/);
    consumer.maintain(50);
    expect(consumer.peerBehaviorObservation('alternate')).toBeUndefined();
  });

  it('evicts transient routes atomically and forgets route-less wants', () => {
    const consumer = mesh({ maxSeenEvents: 1, routeTtlMs: 10, eventTtlMs: 20 });
    const second = { ...event, id: '01'.repeat(32) };
    expect(consumer.receive('ghost', { type: 'want', eventId: event.id }, [], 1)).toEqual([]);
    consumer.receive('provider-a', inventoryFor(event, 4), [], 2);
    consumer.receive('waiting', { type: 'want', eventId: event.id }, [], 3);
    consumer.receive('provider-b', {
      type: 'inventory', eventId: second.id, eventKind: event.kind, payloadBytes: 512, hopLimit: 4,
    }, [], 4);
    expect(() => consumer.receive(
      'provider-a',
      { type: 'frame', eventId: event.id, event },
      [],
      5,
    )).toThrow(/unsolicited/);

    consumer.receive('provider-a', inventoryFor(event, 4), [], 6);
    const actions = consumer.receive(
      'provider-a',
      { type: 'frame', eventId: event.id, event },
      [],
      7,
    );
    expect(actions.some(
      (action) => action.type === 'send' && action.message.type === 'frame' &&
        (action.peerId === 'ghost' || action.peerId === 'waiting'),
    )).toBe(false);

    consumer.receive('provider-c', inventoryFor(event, 4), [], 20);
    consumer.receive('waiting-after-ttl', { type: 'want', eventId: event.id }, [], 29);
    consumer.maintain(31);
    consumer.receive('provider-c', inventoryFor(event, 4), [], 32);
    const afterTtl = consumer.receive(
      'provider-c',
      { type: 'frame', eventId: event.id, event },
      [],
      33,
    );
    expect(afterTtl.some(
      (action) => action.type === 'send' && action.peerId === 'waiting-after-ttl' &&
        action.message.type === 'frame',
    )).toBe(false);
  });

  it('requires canonical event IDs and enforces the local hop bound', () => {
    const consumer = mesh();
    expect(() => consumer.receive('peer', {
      type: 'inventory', eventId: 'AB'.repeat(32), eventKind: event.kind, payloadBytes: 512, hopLimit: 4,
    }, [], 1)).toThrow(/event id/);
    expect(() => consumer.receive('peer', {
      type: 'inventory', eventId: 'ab'.repeat(32), eventKind: event.kind, payloadBytes: 512, hopLimit: 5,
    }, [], 2)).toThrow(/local maximum/);
  });

  it('replays cached events and reserves fanout for unknown peers', () => {
    const provider = mesh();
    expect(provider.publish(event, [], 1)).toEqual([]);
    const replay = provider.replayToPeer(event, 'late-peer', 20 * 60 * 1_000);
    expect(onlyMessage(replay)).toMatchObject({
      type: 'inventory',
      eventId: event.id,
      eventKind: event.kind,
      hopLimit: 4,
    });
    expect(
      provider.receive(
        'late-peer',
        { type: 'want', eventId: event.id },
        [meshPeer('late-peer')],
        20 * 60 * 1_000 + 1,
      ),
    ).toHaveLength(1);

    expect(
      selectMeshPeers(
        [
          meshPeer('good-a', 100),
          meshPeer('good-b', 90),
          meshPeer('good-c', 80),
          meshPeer('bad', -100),
          meshPeer('newcomer'),
        ],
        undefined,
        3,
        1,
      ).map((peer) => peer.id),
    ).toEqual(['good-a', 'good-b', 'newcomer']);
  });

  it('maintenance penalizes expired inventories that were never served', () => {
    const consumer = mesh({ routeTtlMs: 10, eventTtlMs: 20 });
    for (let nowMs = 1; nowMs <= 3; nowMs += 1) {
      const eventId = nowMs.toString(16).padStart(64, '0');
      expect(consumer.receive('blackhole', {
        type: 'inventory',
        eventId,
        eventKind: event.kind,
        payloadBytes: 512,
        hopLimit: 4,
      }, [meshPeer('blackhole')], nowMs)).toEqual([
        { type: 'send', peerId: 'blackhole', message: { type: 'want', eventId } },
      ]);
    }

    expect(consumer.peerBehaviorScore('blackhole')).toBeUndefined();
    consumer.maintain(20);
    expect(consumer.peerBehaviorScore('blackhole')).toBeLessThan(0);
    expect(consumer.peerBehaviorObservation('blackhole')?.unservedInventories).toBe(3);

    const malformed = mesh();
    for (let sample = 0; sample < 3; sample += 1) malformed.recordInvalidMessage('malformed');
    expect(malformed.peerBehaviorObservation('malformed')).toMatchObject({
      samples: 3,
      invalidMessages: 3,
      validFrames: 0,
      unservedInventories: 0,
    });
  });

  it('rejects forged, mismatched, oversized, and wrong-protocol messages', () => {
    const consumer = mesh({ maxEventBytes: meshEventJsonBytes(event) - 1 });
    expect(() => consumer.publish(event, [], 1)).toThrow(/maximum/);
    const forged = { ...event, content: 'tampered' };
    expect(() => mesh().publish(forged, [], 1)).toThrow(/invalid Nostr event/);

    const codec = new InvWantCodec(protocol, 1, 512);
    expect(() => codec.decode(new TextEncoder().encode(
      `{"protocol":"other","version":1,"message":{"type":"want","event_id":"${event.id}"}}`,
    ))).toThrow(/protocol/);
    expect(() => new InvWantCodec(protocol, 1, 128).encode({
      type: 'frame', eventId: event.id, event,
    })).toThrow(/maximum/);
  });
});

function mesh(options: ConstructorParameters<typeof InvWantMesh>[0] = {}): InvWantMesh {
  return new InvWantMesh({ maxHops: 4, allowedKinds: new Set([event.kind]), ...options });
}

function onlyMessage(actions: InvWantAction[]): InvWantWireMessage {
  expect(actions).toHaveLength(1);
  const [action] = actions;
  if (action.type !== 'send') throw new Error('expected send action');
  return action.message;
}

function inventoryFor(signedEvent: Event, hopLimit: number): InvWantWireMessage {
  return {
    type: 'inventory',
    eventId: signedEvent.id,
    eventKind: signedEvent.kind,
    payloadBytes: meshEventJsonBytes(signedEvent),
    hopLimit,
  };
}

function messageFor(actions: InvWantAction[], peerId: string): InvWantWireMessage {
  const action = actions.find((candidate) => candidate.type === 'send' && candidate.peerId === peerId);
  if (action?.type !== 'send') throw new Error(`missing message for ${peerId}`);
  return action.message;
}

function deliveredIds(actions: InvWantAction[]): string[] {
  return actions
    .filter((action) => action.type === 'deliver')
    .map((action) => action.event.id);
}

function wireMessageFromVector(
  message: (typeof vectors.meshWireCases)[number]['message'],
): InvWantWireMessage {
  switch (message.type) {
    case 'inventory':
      return {
        type: 'inventory',
        eventId: message.eventId,
        eventKind: message.eventKind,
        payloadBytes: message.payloadBytes,
        hopLimit: message.hopLimit,
      };
    case 'want':
      return { type: 'want', eventId: message.eventId };
    case 'frame':
      return { type: 'frame', eventId: message.eventId, event: rawEvent };
  }
}
