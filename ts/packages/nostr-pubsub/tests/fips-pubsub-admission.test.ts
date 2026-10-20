import { describe, expect, it } from 'vitest';
import { Segment, type FipsDatagramEndpoint, type FipsServiceContext } from '@fips/tcp';
import { finalizeEvent, generateSecretKey } from 'nostr-tools/pure';
import { FipsPubsubTcpTransport } from '../src/fips-pubsub-tcp-transport.js';
import { FipsPubsubWireCodec, type FipsPubsubWireMessage } from '../src/wire.js';

const alice = `02${'11'.repeat(32)}`, sibling = `03${'22'.repeat(32)}`;
type Handler = (context: FipsServiceContext) => Promise<void> | void;

class Endpoint implements FipsDatagramEndpoint {
  remote!: Endpoint;
  readonly packets: Uint8Array[] = [];
  private readonly handlers = new Map<number, Handler>();
  constructor(readonly id: string) {}
  registerService(port: number, handler: Handler): () => void {
    this.handlers.set(port, handler);
    return () => { this.handlers.delete(port); };
  }
  async sendDatagram(args: { dst: string; srcPort?: number; dstPort: number; payload: Uint8Array }): Promise<void> {
    if (args.dst !== this.remote.id) throw new Error('unknown peer');
    this.packets.push(args.payload.slice());
    queueMicrotask(() => { void this.remote.handlers.get(args.dstPort)?.({
      src: this.id, srcPort: args.srcPort ?? 0, dstPort: args.dstPort, payload: args.payload.slice(),
    }); });
  }
}

describe('FIPS provider admission at transmission', () => {
  it.each(['req', 'event'] as const)('blocks an already queued %s after its peer is revoked', async type => {
    const left = new Endpoint(alice), right = new Endpoint(sibling);
    left.remote = right; right.remote = left;
    const admitted = new Set([sibling]);
    const codec = new FipsPubsubWireCodec();
    const frames: Uint8Array[] = [];
    const options = { servicePort: 7399, maxPeers: 2, maxFrameBytes: 65_525,
      maxQueuedRecordsPerPeer: 8, maxQueuedBytesPerPeer: 128 * 1024,
      maxIoBytesPerDrive: 16 * 1024, maxFramesPerDrive: 8 };
    const callbacks = { connected() {}, disconnected() {}, tick() {}, error() {} };
    const sender = new FipsPubsubTcpTransport(left, alice, options, {
      ...callbacks, admitsPeer: peer => admitted.has(peer), frame() {},
    });
    const receiver = new FipsPubsubTcpTransport(right, sibling, options, {
      ...callbacks, admitsPeer: peer => peer === alice, frame: (_peer, frame) => { frames.push(frame); },
    });
    const pump = async () => {
      await Promise.allSettled([sender.idle(), receiver.idle()]);
      await new Promise(resolve => setTimeout(resolve, 0));
    };
    try {
      await sender.connectPeer(sibling);
      sender.queueFrame(sibling, codec.encodeFrame({ type: 'req', subscriptionId: 'initial', filters: [{ kinds: [1060] }] }));
      for (let turn = 0; turn < 30 && !frames.length; turn++) await pump();
      expect(frames.map(frame => codec.decodeFrame(frame).type)).toEqual(['req']);
      const before = left.packets.length;
      const event = finalizeEvent({ kind: 1060, created_at: 1_700_000_000,
        tags: [['p', '44'.repeat(32)], ['v', '2']], content: 'encrypted fixture' }, generateSecretKey());
      const message: FipsPubsubWireMessage = type === 'req'
        ? { type, subscriptionId: 'queued', filters: [{ kinds: [1060], '#p': ['44'.repeat(32)] }] }
        : { type, subscriptionId: 'initial', event };
      sender.queueFrame(sibling, codec.encodeFrame(message));
      // Revocation occurs before the asynchronous TCP driver can flush its queue.
      admitted.clear();
      for (let turn = 0; turn < 5; turn++) await pump();
      expect(left.packets.slice(before).map(packet => Segment.decode(packet).payload).filter(bytes => bytes.length)).toEqual([]);
      expect(frames).toHaveLength(1);
    } finally {
      await Promise.allSettled([sender.dispose(), receiver.dispose()]);
    }
  });
});
