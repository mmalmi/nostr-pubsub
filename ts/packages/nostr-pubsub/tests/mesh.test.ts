import { describe, expect, it } from 'vitest';
import type { Event } from 'nostr-tools/core';
import vectors from '../test-data/interop-vectors.json';
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
