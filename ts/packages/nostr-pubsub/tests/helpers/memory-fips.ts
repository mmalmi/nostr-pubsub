import type { FipsServiceContext } from '@fips/tcp';
import type { FipsPubsubClientNode } from '../../src/fips-pubsub-client-types.js';
type FipsPubsubServiceHandler = (context: FipsServiceContext) => Promise<void> | void;

export class MemoryFipsNetwork {
  private readonly nodes = new Map<string, MemoryFipsNode>();

  node(peerId: string): MemoryFipsNode {
    const node = new MemoryFipsNode(peerId, this);
    this.nodes.set(peerId, node);
    return node;
  }

  get(peerId: string): MemoryFipsNode | undefined {
    return this.nodes.get(peerId);
  }
}

export class MemoryFipsNode implements FipsPubsubClientNode {
  private readonly services = new Map<number, FipsPubsubServiceHandler>();
  private readonly listeners = new Map<string, Set<(event: unknown) => void>>();

  constructor(readonly id: string, private readonly network: MemoryFipsNetwork) {}

  registerService(port: number, handler: FipsPubsubServiceHandler): () => void {
    this.services.set(port, handler);
    return () => {
      if (this.services.get(port) === handler) this.services.delete(port);
    };
  }

  on(event: 'peer' | 'session', listener: (event: unknown) => void): () => void {
    let listeners = this.listeners.get(event);
    if (listeners === undefined) {
      listeners = new Set();
      this.listeners.set(event, listeners);
    }
    listeners.add(listener);
    return () => listeners?.delete(listener);
  }

  async sendDatagram(args: {
    dst: string;
    srcPort?: number;
    dstPort: number;
    payload: Uint8Array;
  }): Promise<void> {
    const target = this.network.get(args.dst);
    if (target === undefined) throw new Error(`unroutable FIPS peer ${args.dst}`);
    queueMicrotask(() => void target.receive({
        src: this.id,
        srcPort: args.srcPort ?? 0,
        dstPort: args.dstPort,
        payload: new Uint8Array(args.payload),
        reply: async (payload, destinationPort) => target.sendDatagram({
          dst: this.id,
          srcPort: args.dstPort,
          dstPort: destinationPort ?? args.srcPort ?? 0,
          payload,
        }),
      }).catch(() => undefined));
  }

  emit(event: 'peer' | 'session', value: unknown): void {
    for (const listener of this.listeners.get(event) ?? []) listener(value);
  }

  private async receive(context: FipsServiceContext): Promise<void> {
    const handler = this.services.get(context.dstPort);
    if (handler === undefined) throw new Error(`no FIPS service on ${context.dstPort}`);
    await handler(context);
  }
}

