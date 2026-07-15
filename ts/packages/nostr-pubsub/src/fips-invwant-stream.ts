import type { QueryEvent } from './event-bus.js';
import {
  encodeInvWantRecord,
  InvWantRecordDecoder,
  INV_WANT_RECORD_PREFIX_BYTES,
} from './fips-invwant-record.js';
import {
  DEFAULT_INV_WANT_MAX_WIRE_BYTES,
  InvWantCodec,
  type InvWantWireMessage,
} from './mesh-codec.js';
import { meshPeer, type MeshPeer } from './mesh-peer.js';
import type { InvWantMeshRetainedState } from './mesh-resources.js';
import { InvWantMesh, type InvWantAction, type InvWantMeshOptions } from './mesh.js';
import type { PubsubPolicy } from './policy.js';
import { fipsEndpointSource, sourceKindDefaultPriority } from './source.js';
import {
  PubsubError,
  verifyNostrEvent,
  type NostrVerifiedEvent,
} from './types.js';

export const FIPS_NOSTR_PUBSUB_INV_WANT_PROTOCOL = 'nostr.pubsub';
export const FIPS_NOSTR_PUBSUB_INV_WANT_VERSION = 1;

export interface FipsInvWantStreamOptions {
  mesh: Partial<InvWantMeshOptions>;
  protocol: string;
  protocolVersion: number;
  maxRecordBytes: number;
  maxInputPeers: number;
  maxRecordsPerReceive: number;
}

export interface MeshPeerPolicy {
  selectMeshPeer(peerId: string): MeshPeer | undefined;
}

export type FipsInvWantStreamAction =
  | { type: 'send'; peerId: string; record: Uint8Array }
  | { type: 'deliver'; event: QueryEvent };

export function defaultFipsInvWantStreamOptions(): FipsInvWantStreamOptions {
  return {
    mesh: {},
    protocol: FIPS_NOSTR_PUBSUB_INV_WANT_PROTOCOL,
    protocolVersion: FIPS_NOSTR_PUBSUB_INV_WANT_VERSION,
    maxRecordBytes: DEFAULT_INV_WANT_MAX_WIRE_BYTES,
    maxInputPeers: 64,
    maxRecordsPerReceive: 64,
  };
}

/** Bounded Inv/WANT state above an authenticated reliable byte stream. */
export class FipsInvWantStream {
  readonly options: FipsInvWantStreamOptions;
  private readonly mesh: InvWantMesh;
  private readonly codec: InvWantCodec;
  private readonly inputs = new Map<string, InvWantRecordDecoder>();
  private eventPolicy?: PubsubPolicy;
  private peerPolicy?: MeshPeerPolicy;

  constructor(options: Partial<FipsInvWantStreamOptions> = {}) {
    const defaults = defaultFipsInvWantStreamOptions();
    this.options = {
      ...defaults,
      ...options,
      mesh: { ...defaults.mesh, ...options.mesh },
    };
    validateOptions(this.options);
    this.mesh = new InvWantMesh(this.options.mesh);
    this.codec = new InvWantCodec(
      this.options.protocol,
      this.options.protocolVersion,
      this.options.maxRecordBytes,
    );
  }

  withEventPolicy(policy: PubsubPolicy): this {
    this.eventPolicy = policy;
    return this;
  }

  withPeerPolicy(policy: MeshPeerPolicy): this {
    this.peerPolicy = policy;
    return this;
  }

  seed(event: NostrVerifiedEvent, nowMs: number): void {
    this.ensureEventRecordFits(event);
    this.mesh.seedVerified(event, nowMs);
  }

  publish(
    event: NostrVerifiedEvent,
    connectedPeers: Iterable<string>,
    nowMs: number,
  ): FipsInvWantStreamAction[] {
    this.ensureEventRecordFits(event);
    return this.encodeActions(
      this.mesh.publishVerified(event, this.selectPeers(connectedPeers), nowMs),
    );
  }

  peerConnected(peerId: string, nowMs: number): FipsInvWantStreamAction[] {
    if (this.selectPeer(peerId) === undefined) return [];
    return this.encodeActions(this.mesh.replayCachedToPeer(peerId, nowMs));
  }

  disconnectPeer(peerId: string): void {
    this.inputs.delete(peerId);
  }

  async receiveBytes(
    sourcePeer: string,
    bytes: Uint8Array,
    connectedPeers: Iterable<string>,
    nowMs: number,
  ): Promise<FipsInvWantStreamAction[]> {
    if (this.selectPeer(sourcePeer) === undefined) {
      this.disconnectPeer(sourcePeer);
      return [];
    }
    const records = this.decodeRecords(sourcePeer, bytes);
    const peers = this.selectPeers(connectedPeers);
    const output: FipsInvWantStreamAction[] = [];
    for (const record of records) {
      let message: InvWantWireMessage;
      try {
        message = this.codec.decode(record);
      } catch (error) {
        this.mesh.recordInvalidMessage(sourcePeer);
        throw error;
      }
      output.push(...await this.receiveMessage(sourcePeer, message, peers, nowMs));
    }
    return output;
  }

  retainedState(): InvWantMeshRetainedState {
    return this.mesh.retainedState();
  }

  bufferedInputBytes(peerId: string): number {
    return this.inputs.get(peerId)?.length ?? 0;
  }

  inputPeerCount(): number {
    return this.inputs.size;
  }

  remainingInputCapacity(peerId: string): number {
    return this.inputs.get(peerId)?.remainingCapacity ??
      this.options.maxRecordBytes + INV_WANT_RECORD_PREFIX_BYTES;
  }

  hasReadyInput(peerId: string): boolean {
    return this.inputs.get(peerId)?.hasCompleteRecord ?? false;
  }

  maintain(nowMs: number): void {
    this.mesh.maintain(nowMs);
  }

  private async receiveMessage(
    sourcePeer: string,
    message: InvWantWireMessage,
    peers: readonly MeshPeer[],
    nowMs: number,
  ): Promise<FipsInvWantStreamAction[]> {
    if (message.type !== 'frame') {
      return this.encodeActions(this.mesh.receive(sourcePeer, message, peers, nowMs));
    }
    let verified: NostrVerifiedEvent;
    try {
      verified = verifyNostrEvent(message.event);
    } catch (error) {
      this.mesh.recordInvalidMessage(sourcePeer);
      throw error;
    }
    const source = fipsEndpointSource(sourcePeer);
    let priority = sourceKindDefaultPriority(source.kind);
    if (this.eventPolicy !== undefined) {
      let decision;
      try {
        decision = await this.eventPolicy.checkEvent({ event: verified, source });
      } catch (error) {
        this.mesh.dismissFrame(sourcePeer, message.eventId);
        throw error;
      }
      if (decision.type === 'drop') {
        this.mesh.dismissFrame(sourcePeer, message.eventId);
        return [];
      }
      priority = decision.priority;
    }
    const actions = this.mesh.receiveVerifiedFrame(
      sourcePeer,
      message.eventId,
      verified,
      peers,
      nowMs,
    );
    return this.encodeActions(actions, { event: verified, source, priority });
  }

  private selectPeers(peerIds: Iterable<string>): MeshPeer[] {
    const selected = new Map<string, MeshPeer>();
    for (const peerId of peerIds) {
      const peer = this.selectPeer(peerId);
      if (peer !== undefined) selected.set(peerId, peer);
    }
    return [...selected.entries()]
      .sort(([left], [right]) => left < right ? -1 : Number(left > right))
      .map(([, peer]) => peer);
  }

  private selectPeer(peerId: string): MeshPeer | undefined {
    const selected = this.peerPolicy?.selectMeshPeer(peerId) ??
      (this.peerPolicy === undefined ? meshPeer(peerId) : undefined);
    return selected === undefined ? undefined : { ...selected, id: peerId };
  }

  private ensureEventRecordFits(event: NostrVerifiedEvent): void {
    this.codec.encode({ type: 'frame', eventId: event.id, event });
  }

  private decodeRecords(peerId: string, bytes: Uint8Array): Uint8Array[] {
    let decoder = this.inputs.get(peerId);
    if (decoder === undefined) {
      if (this.inputs.size >= this.options.maxInputPeers) {
        throw PubsubError.storage(
          `FIPS pubsub input peer limit is ${this.options.maxInputPeers}`,
        );
      }
      decoder = new InvWantRecordDecoder(this.options.maxRecordBytes);
      this.inputs.set(peerId, decoder);
    }
    return decoder.push(bytes, this.options.maxRecordsPerReceive);
  }

  private encodeActions(
    actions: readonly InvWantAction[],
    admittedDelivery?: QueryEvent,
  ): FipsInvWantStreamAction[] {
    return actions.map((action) => {
      if (action.type === 'send') {
        return {
          type: 'send',
          peerId: action.peerId,
          record: encodeInvWantRecord(
            this.codec.encode(action.message),
            this.options.maxRecordBytes,
          ),
        };
      }
      if (admittedDelivery === undefined) {
        throw PubsubError.storage('mesh delivered an event outside frame admission');
      }
      return { type: 'deliver', event: admittedDelivery };
    });
  }
}

function validateOptions(options: FipsInvWantStreamOptions): void {
  if (options.protocol.trim() === '') throw validation('protocol must not be empty');
  requireU8(options.protocolVersion, 'protocol version');
  requirePositive(options.maxRecordBytes, 'max record bytes');
  if (options.maxRecordBytes > 0xffff_ffff) {
    throw validation('max record bytes exceeds the record prefix');
  }
  requirePositive(options.maxInputPeers, 'max input peers');
  requirePositive(options.maxRecordsPerReceive, 'max records per receive');
  if (!Number.isSafeInteger(options.maxRecordBytes + INV_WANT_RECORD_PREFIX_BYTES)) {
    throw validation('record buffer size overflows');
  }
}

function requirePositive(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw validation(`${name} must be greater than zero`);
  }
}

function requireU8(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 0 || value > 255) {
    throw validation(`${name} must be an unsigned byte`);
  }
}

function validation(message: string): PubsubError {
  return PubsubError.validation(message);
}
