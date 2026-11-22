import { describe, expect, it } from 'vitest';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { FipsNostrPubsubClient } from '../src/index.js';
import { parseConnectionEvent } from '../src/fips-pubsub-client-support.js';
import { MemoryFipsNetwork } from './helpers/memory-fips.js';

const readerId = '02' + '11'.repeat(32);
const x = '22'.repeat(32);

describe('FIPS subscription peer identity', () => {
  it('resolves an x-only disconnect only with consistent authenticated metadata', () => {
    expect(parseConnectionEvent({ state: 'closed', remotePubkey: x, peerId: x })).toEqual({ peerId: '02' + x, connected: false });
    expect(parseConnectionEvent({ state: 'closed', remotePubkey: x })).toBeUndefined();
    expect(parseConnectionEvent({ state: 'closed', remotePubkey: '03' + x, peerId: x.slice(1) })).toBeUndefined();
  });

  it.each([
    ['02' + x, '03' + x, undefined, true],
    ['03' + x, '02' + x, undefined, true],
    ['02' + x, '03' + x.slice(0, -1) + '3', undefined, false],
    ['02' + x, '04' + x, undefined, false],
    ['02' + x, x, undefined, false],
    ['02' + x, '03' + x, x, true],
    ['03' + x, x, x, true],
    ['03' + x, x, x.slice(0, -1) + '3', false],
    ['03' + x, '02' + x, x, true],
    ['02' + x, '03' + x, x.slice(0, -1) + '3', false],
    ['02' + x, '03' + x, x.slice(2), false],
  ] as const)('reads a signed event only from the admitted identity (%s / %s)', async (configured, source, peerId, admitted) => {
    const network = new MemoryFipsNetwork();
    const alternate = (configured.startsWith('02') ? '03' : '02') + x;
    let connected = (): readonly string[] => [];
    const reader = new FipsNostrPubsubClient({
      node: network.node(readerId), localPeerId: readerId, peers: () => configured.startsWith('02') ? [configured] : [configured, alternate],
      observeConnections: read => { connected = read; },
    }).start();
    const seed = new FipsNostrPubsubClient({
      node: network.node(configured, source, peerId), localPeerId: configured, peers: () => [readerId],
    }).start();
    try {
      const event = finalizeEvent({ kind: 1, content: 'fixture', tags: [], created_at: 1_700_000_000 }, generateSecretKey());
      await seed.publish(event);
      const deliveries: [string, string][] = [];
      reader.subscribe([{ ids: [event.id] }], (received, peer) => { deliveries.push([received.id, peer]); });
      for (let turn = 0; turn < 30 && !deliveries.length; turn++) {
        await Promise.all([reader.idle(), seed.idle()]);
        await new Promise(resolve => setTimeout(resolve, 0));
      }
      expect(deliveries).toEqual(admitted ? [[event.id, configured]] : []);
      expect(connected()).toEqual(admitted ? [configured] : []);
    } finally {
      await Promise.all([reader.stop(), seed.stop()]);
    }
  });
});
