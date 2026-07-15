import { describe, expect, it } from 'vitest';
import type { Event } from 'nostr-tools/core';
import vectors from '../../../../crates/nostr-pubsub/tests/data/interop-vectors.json';
import {
  InvWantMesh,
  meshEventJsonBytes,
  verifyNostrEvent,
} from '../src/index.js';

const events = Object.values(vectors.events)
  .map((candidate) => verifyNostrEvent(candidate as Event));
const kinds = new Set(events.map((event) => event.kind));

describe('transport disruption attribution', () => {
  it('suppresses only matching expiry penalties and clears bounded marks', () => {
    const mesh = new InvWantMesh({ routeTtlMs: 10, eventTtlMs: 20, allowedKinds: kinds });
    for (let nowMs = 1; nowMs <= 3; nowMs += 1) {
      const disruptedId = (nowMs + 10).toString(16).padStart(64, '0');
      mesh.receive('transport-failed', {
        type: 'inventory', eventId: disruptedId, eventKind: events[0].kind,
        payloadBytes: 512, hopLimit: 4,
      }, [], nowMs);
      expect(mesh.recordTransportDisruption('transport-failed', disruptedId)).toBe(true);
      expect(mesh.recordTransportDisruption('transport-failed', disruptedId)).toBe(false);
      const blackholeId = (nowMs + 20).toString(16).padStart(64, '0');
      mesh.receive('blackhole', {
        type: 'inventory', eventId: blackholeId, eventKind: events[0].kind,
        payloadBytes: 512, hopLimit: 4,
      }, [], nowMs);
      expect(mesh.recordTransportDisruption('blackhole', blackholeId)).toBe(true);
      expect(mesh.receive('blackhole', {
        type: 'inventory', eventId: blackholeId, eventKind: events[0].kind,
        payloadBytes: 512, hopLimit: 4,
      }, [], nowMs)).toHaveLength(1);
    }
    expect(mesh.retainedState().transportDisruptedRoutePeers).toBe(3);
    mesh.maintain(20);
    expect(mesh.peerBehaviorObservation('transport-failed')).toBeUndefined();
    expect(mesh.peerBehaviorObservation('blackhole')?.unservedInventories).toBe(3);
    expect(mesh.retainedState().transportDisruptedRoutePeers).toBe(0);
  });

  it('credits valid frames recovered after a marked disruption', () => {
    const mesh = new InvWantMesh({ routeTtlMs: 10, eventTtlMs: 20, allowedKinds: kinds });
    for (const [index, event] of events.slice(0, 3).entries()) {
      const nowMs = index * 2 + 1;
      mesh.receive('provider', {
        type: 'inventory', eventId: event.id, eventKind: event.kind,
        payloadBytes: meshEventJsonBytes(event), hopLimit: 4,
      }, [], nowMs);
      expect(mesh.recordTransportDisruption('provider', event.id)).toBe(true);
      mesh.receive(
        'provider', { type: 'frame', eventId: event.id, event }, [], nowMs + 1,
      );
    }
    expect(mesh.retainedState().transportDisruptedRoutePeers).toBe(0);
    expect(mesh.peerBehaviorObservation('provider')).toMatchObject({
      validFrames: 3, invalidMessages: 0, unservedInventories: 0,
    });
  });
});
