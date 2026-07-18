import {
  FipsTcpEndpoint,
  State,
  type ConnectionId,
  type FipsDatagramEndpoint,
  type FipsServiceContext,
} from '@fips/tcp';
import { encodeInvWantRecord, InvWantRecordDecoder } from './fips-invwant-record.js';
import { InvWantRecordQueues } from './fips-invwant-tcp-queue.js';
import { fipsInvWantTcpPeerOrderKey } from './fips-invwant-tcp-types.js';
import { PubsubError } from './types.js';

const STREAM_IO_CHUNK_BYTES = 16 * 1024;
const TCP_POLL_INTERVAL_MS = 50;
const MAX_READY_INPUT_TURNS = 16;
const ACTIVE_STATES = new Set([State.Established, State.CloseWait]);
const CONNECTING_STATES = new Set([
  State.SynSent,
  State.SynReceived,
  State.Established,
  State.CloseWait,
]);

type Direction = 'inbound' | 'outbound';

interface TrackedConnection {
  peer: string;
  direction: Direction;
}

export interface FipsPubsubTcpTransportOptions {
  servicePort: number;
  maxPeers: number;
  maxFrameBytes: number;
  maxQueuedRecordsPerPeer: number;
  maxQueuedBytesPerPeer: number;
  maxIoBytesPerDrive: number;
  maxFramesPerDrive: number;
}

export interface FipsPubsubTcpTransportCallbacks {
  frame(peerId: string, frame: Uint8Array): void;
  connected(peerId: string): void;
  disconnected(peerId: string): void;
  tick(nowMs: number): void;
  error(error: Error): void;
}

/** Reliable record transport shared with Rust's `WireTcpDriver`. */
export class FipsPubsubTcpTransport {
  private readonly tcp: FipsTcpEndpoint;
  private readonly connections = new Map<ConnectionId, TrackedConnection>();
  private active = new Map<string, ConnectionId>();
  private readonly queues: InvWantRecordQueues;
  private readonly inputs = new Map<string, InvWantRecordDecoder>();
  private readonly localPeerOrderKey: string;
  private operation: Promise<void> = Promise.resolve();
  private timer?: ReturnType<typeof setTimeout>;
  private disposed = false;

  constructor(
    endpoint: FipsDatagramEndpoint,
    localPeerId: string,
    readonly options: FipsPubsubTcpTransportOptions,
    private readonly callbacks: FipsPubsubTcpTransportCallbacks,
    isnSeed: bigint | number = 1n,
  ) {
    validateOptions(options);
    if (localPeerId.trim() === '') throw validation('local peer identity must not be empty');
    const notifying = new NotifyingEndpoint(endpoint, () => this.scheduleDrive(false));
    this.tcp = new FipsTcpEndpoint(notifying, options.servicePort, {
      receiveBuffer: 0xffff,
      sendBuffer: options.maxQueuedBytesPerPeer,
      maxConnections: options.maxPeers * 2,
      maxConnectionsPerPeer: 2,
    }, isnSeed);
    this.queues = new InvWantRecordQueues({
      serviceNamespace: 'nostr.pubsub',
      serviceVersion: 1,
      servicePort: options.servicePort,
      maxPeers: options.maxPeers,
      maxQueuedRecordsPerPeer: options.maxQueuedRecordsPerPeer,
      maxQueuedBytesPerPeer: options.maxQueuedBytesPerPeer,
      maxIoBytesPerDrive: options.maxIoBytesPerDrive,
    });
    this.localPeerOrderKey = fipsInvWantTcpPeerOrderKey(localPeerId);
  }

  async connectPeer(peer: string, nowMs = Date.now()): Promise<void> {
    this.ensureOpen();
    for (const [id, connection] of this.connections) {
      const state = await this.tcp.state(id);
      if (connection.peer === peer && state !== undefined && CONNECTING_STATES.has(state)) return;
    }
    this.ensurePeerCapacity(peer);
    const id = await transport('connect TCP/FIPS Nostr pubsub peer', this.tcp.connect(peer, nowMs));
    const authenticated = await this.tcp.peer(id);
    if (authenticated !== peer) {
      await this.tcp.abort(id);
      throw storage('outbound TCP/FIPS stream peer identity changed');
    }
    this.connections.set(id, { peer, direction: 'outbound' });
    this.scheduleDrive(false);
  }

  queueFrame(peerId: string, frame: Uint8Array): void {
    this.ensureOpen();
    const record = encodeInvWantRecord(frame, this.options.maxFrameBytes);
    this.queues.enqueue([{ peerId, record }]);
    this.scheduleDrive(false);
  }

  async abortPeer(peer: string): Promise<void> {
    this.ensureOpen();
    const ids = [...this.connections]
      .filter(([, connection]) => connection.peer === peer)
      .map(([id]) => id);
    for (const id of ids) {
      if (await this.tcp.state(id) !== undefined) {
        await transport('abort TCP/FIPS Nostr pubsub peer', this.tcp.abort(id));
      }
      this.connections.delete(id);
    }
    if (this.active.delete(peer)) this.callbacks.disconnected(peer);
    this.inputs.delete(peer);
    this.queues.restart(peer);
  }

  connectedPeerCount(): number {
    return this.active.size;
  }

  isConnected(peerId: string): boolean {
    return this.active.has(peerId);
  }

  async idle(): Promise<void> {
    this.ensureOpen();
    for (let turn = 0; turn < 4; turn += 1) {
      await this.enqueueDrive(true);
      await Promise.resolve();
    }
  }

  async dispose(): Promise<void> {
    if (this.disposed) return;
    this.disposed = true;
    if (this.timer !== undefined) clearTimeout(this.timer);
    await this.operation;
    const ids = [...this.connections.keys()];
    for (const id of ids) {
      if (await this.tcp.state(id) !== undefined) await this.tcp.abort(id).catch(() => undefined);
    }
    this.connections.clear();
    this.active.clear();
    this.inputs.clear();
    this.queues.clear();
    await this.tcp.dispose();
  }

  private scheduleDrive(poll: boolean): void {
    if (this.disposed) return;
    void this.enqueueDrive(poll).catch((error: unknown) => this.callbacks.error(asError(error)));
  }

  private enqueueDrive(poll: boolean): Promise<void> {
    const next = this.operation.then(async () => {
      if (this.disposed) return;
      if (poll) await transport('poll TCP/FIPS Nostr pubsub transport', this.tcp.poll());
      await this.driveReady(Date.now());
    });
    this.operation = next.catch(() => undefined);
    return next;
  }

  private async driveReady(nowMs: number): Promise<void> {
    await this.acceptConnections();
    await this.refreshActive();
    await this.readActive(nowMs);
    await this.flushQueues(nowMs);
    await this.finishRemoteCloses(nowMs);
    await this.refreshActive();
    this.callbacks.tick(nowMs);
    this.armTimer();
  }

  private async acceptConnections(): Promise<void> {
    for (;;) {
      const id = await this.tcp.accept();
      if (id === undefined) return;
      const peer = await this.tcp.peer(id);
      if (peer === undefined) {
        await this.tcp.abort(id);
        continue;
      }
      try {
        this.ensurePeerCapacity(peer);
      } catch {
        await this.tcp.abort(id);
        continue;
      }
      if (!this.connections.has(id)) this.connections.set(id, { peer, direction: 'inbound' });
    }
  }

  private async refreshActive(): Promise<void> {
    const candidates = new Map<string, Array<[ConnectionId, Direction]>>();
    for (const [id, connection] of [...this.connections]) {
      const state = await this.tcp.state(id);
      if (state === undefined) {
        this.connections.delete(id);
        continue;
      }
      const authenticated = await this.tcp.peer(id);
      if (authenticated !== connection.peer) {
        await this.tcp.abort(id);
        this.connections.delete(id);
        throw storage('TCP/FIPS stream peer identity changed');
      }
      if (ACTIVE_STATES.has(state)) {
        const streams = candidates.get(connection.peer) ?? [];
        streams.push([id, connection.direction]);
        candidates.set(connection.peer, streams);
      }
    }

    const next = new Map<string, ConnectionId>();
    const extras: ConnectionId[] = [];
    for (const peer of [...candidates.keys()].sort()) {
      const preferOutbound = this.localPeerOrderKey < fipsInvWantTcpPeerOrderKey(peer);
      const streams = candidates.get(peer)!;
      streams.sort(([leftId, left], [rightId, right]) => {
        const leftPreferred = (left === 'outbound') === preferOutbound;
        const rightPreferred = (right === 'outbound') === preferOutbound;
        return Number(rightPreferred) - Number(leftPreferred) || leftId - rightId;
      });
      next.set(peer, streams[0]![0]);
      extras.push(...streams.slice(1).map(([id]) => id));
    }
    for (const id of extras) {
      await this.tcp.abort(id);
      this.connections.delete(id);
    }

    const previous = this.active;
    this.active = next;
    const changed = new Set([...previous.keys(), ...next.keys()]);
    for (const peer of [...changed].sort()) {
      if (previous.get(peer) === next.get(peer)) continue;
      this.inputs.delete(peer);
      this.queues.restart(peer);
      if (previous.has(peer)) this.callbacks.disconnected(peer);
      if (next.has(peer)) this.callbacks.connected(peer);
    }
  }

  private async readActive(nowMs: number): Promise<void> {
    let byteBudget = this.options.maxIoBytesPerDrive;
    let frameBudget = this.options.maxFramesPerDrive;
    for (const [peer, id] of this.active) {
      const decoder = this.inputs.get(peer) ?? new InvWantRecordDecoder(this.options.maxFrameBytes);
      this.inputs.set(peer, decoder);
      for (let turn = 0; turn < MAX_READY_INPUT_TURNS && frameBudget > 0; turn += 1) {
        const ready = decoder.push(new Uint8Array(), frameBudget);
        for (const frame of ready) {
          frameBudget -= 1;
          this.callbacks.frame(peer, frame);
        }
        if (decoder.hasCompleteRecord || byteBudget === 0) continue;
        const maximum = Math.min(decoder.remainingCapacity, STREAM_IO_CHUNK_BYTES, byteBudget);
        if (maximum === 0) break;
        const bytes = await transport(
          'read TCP/FIPS Nostr pubsub stream',
          this.tcp.read(id, maximum, nowMs),
        );
        if (bytes.byteLength === 0) break;
        byteBudget -= bytes.byteLength;
        const frames = decoder.push(bytes, frameBudget);
        for (const frame of frames) {
          frameBudget -= 1;
          this.callbacks.frame(peer, frame);
        }
      }
      if (byteBudget === 0 || frameBudget === 0) break;
    }
  }

  private async flushQueues(nowMs: number): Promise<void> {
    let budget = this.options.maxIoBytesPerDrive;
    for (const [peer, id] of this.active) {
      while (budget > 0) {
        const chunk = this.queues.nextChunk(peer, Math.min(STREAM_IO_CHUNK_BYTES, budget));
        if (chunk === undefined) break;
        const accepted = await transport(
          'write TCP/FIPS Nostr pubsub stream',
          this.tcp.write(id, chunk, nowMs),
        );
        if (accepted === 0) break;
        budget -= accepted;
        this.queues.accept(peer, accepted);
      }
    }
  }

  private async finishRemoteCloses(nowMs: number): Promise<void> {
    for (const [peer, id] of this.active) {
      if (await this.tcp.isReadClosed(id) && !this.queues.has(peer)) {
        await this.tcp.close(id, nowMs);
      }
    }
  }

  private armTimer(): void {
    if (this.timer !== undefined) clearTimeout(this.timer);
    if (this.disposed || (this.connections.size === 0 && this.queues.snapshot().records === 0)) {
      this.timer = undefined;
      return;
    }
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.scheduleDrive(true);
    }, TCP_POLL_INTERVAL_MS);
    const timer = this.timer as ReturnType<typeof setTimeout> & { unref?: () => void };
    timer.unref?.();
  }

  private ensurePeerCapacity(peer: string): void {
    const peers = new Set([...this.connections.values()].map((connection) => connection.peer));
    if (!peers.has(peer) && peers.size >= this.options.maxPeers) {
      throw storage(`TCP/FIPS Nostr pubsub peer limit is ${this.options.maxPeers}`);
    }
  }

  private ensureOpen(): void {
    if (this.disposed) throw storage('TCP/FIPS Nostr pubsub transport is disposed');
  }
}

class NotifyingEndpoint implements FipsDatagramEndpoint {
  constructor(
    private readonly endpoint: FipsDatagramEndpoint,
    private readonly received: () => void,
  ) {}

  registerService(
    port: number,
    handler: (context: FipsServiceContext) => Promise<void> | void,
  ): () => void {
    return this.endpoint.registerService(port, async (context) => {
      await handler(context);
      this.received();
    });
  }

  sendDatagram(args: {
    dst: string;
    srcPort?: number;
    dstPort: number;
    payload: Uint8Array;
  }): Promise<void> {
    return this.endpoint.sendDatagram(args);
  }
}

function validateOptions(options: FipsPubsubTcpTransportOptions): void {
  for (const [name, value] of Object.entries(options)) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw validation(`${name} must be a positive safe integer`);
    }
  }
  if (options.maxPeers * 2 > Number.MAX_SAFE_INTEGER) {
    throw validation('TCP connection limit overflows');
  }
}

async function transport<T>(context: string, operation: Promise<T>): Promise<T> {
  try {
    return await operation;
  } catch (error) {
    throw storage(`${context}: ${asError(error).message}`);
  }
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error));
}

function validation(message: string): PubsubError {
  return PubsubError.validation(message);
}

function storage(message: string): PubsubError {
  return PubsubError.storage(message);
}
