import { encodeInvWantRecord, InvWantRecordDecoder, INV_WANT_RECORD_PREFIX_BYTES, } from './fips-invwant-record.js';
import { DEFAULT_INV_WANT_MAX_WIRE_BYTES, InvWantCodec, } from './mesh-codec.js';
import { meshPeer } from './mesh-peer.js';
import { InvWantMesh } from './mesh.js';
import { fipsEndpointSource, sourceKindDefaultPriority } from './source.js';
import { PubsubError, verifyNostrEvent, } from './types.js';
export const FIPS_NOSTR_PUBSUB_INV_WANT_PROTOCOL = 'nostr.pubsub';
export const FIPS_NOSTR_PUBSUB_INV_WANT_VERSION = 1;
export function defaultFipsInvWantStreamOptions() {
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
    options;
    mesh;
    codec;
    inputs = new Map();
    eventPolicy;
    peerPolicy;
    constructor(options = {}) {
        const defaults = defaultFipsInvWantStreamOptions();
        this.options = {
            ...defaults,
            ...options,
            mesh: { ...defaults.mesh, ...options.mesh },
        };
        validateOptions(this.options);
        this.mesh = new InvWantMesh(this.options.mesh);
        this.codec = new InvWantCodec(this.options.protocol, this.options.protocolVersion, this.options.maxRecordBytes);
    }
    withEventPolicy(policy) {
        this.eventPolicy = policy;
        return this;
    }
    withPeerPolicy(policy) {
        this.peerPolicy = policy;
        return this;
    }
    seed(event, nowMs) {
        this.ensureEventRecordFits(event);
        this.mesh.seedVerified(event, nowMs);
    }
    publish(event, connectedPeers, nowMs) {
        this.ensureEventRecordFits(event);
        return this.encodeActions(this.mesh.publishVerified(event, this.selectPeers(connectedPeers), nowMs));
    }
    peerConnected(peerId, nowMs) {
        if (this.selectPeer(peerId) === undefined)
            return [];
        return this.encodeActions(this.mesh.replayCachedToPeer(peerId, nowMs));
    }
    disconnectPeer(peerId) {
        this.inputs.delete(peerId);
    }
    async receiveBytes(sourcePeer, bytes, connectedPeers, nowMs) {
        if (this.selectPeer(sourcePeer) === undefined) {
            this.disconnectPeer(sourcePeer);
            return [];
        }
        const records = this.decodeRecords(sourcePeer, bytes);
        const peers = this.selectPeers(connectedPeers);
        const output = [];
        for (const record of records) {
            let message;
            try {
                message = this.codec.decode(record);
            }
            catch (error) {
                this.mesh.recordInvalidMessage(sourcePeer);
                throw error;
            }
            output.push(...await this.receiveMessage(sourcePeer, message, peers, nowMs));
        }
        return output;
    }
    retainedState() {
        return this.mesh.retainedState();
    }
    bufferedInputBytes(peerId) {
        return this.inputs.get(peerId)?.length ?? 0;
    }
    inputPeerCount() {
        return this.inputs.size;
    }
    remainingInputCapacity(peerId) {
        return this.inputs.get(peerId)?.remainingCapacity ??
            this.options.maxRecordBytes + INV_WANT_RECORD_PREFIX_BYTES;
    }
    hasReadyInput(peerId) {
        return this.inputs.get(peerId)?.hasCompleteRecord ?? false;
    }
    maintain(nowMs) {
        this.mesh.maintain(nowMs);
    }
    async receiveMessage(sourcePeer, message, peers, nowMs) {
        if (message.type !== 'frame') {
            return this.encodeActions(this.mesh.receive(sourcePeer, message, peers, nowMs));
        }
        let verified;
        try {
            verified = verifyNostrEvent(message.event);
        }
        catch (error) {
            this.mesh.recordInvalidMessage(sourcePeer);
            throw error;
        }
        const source = fipsEndpointSource(sourcePeer);
        let priority = sourceKindDefaultPriority(source.kind);
        if (this.eventPolicy !== undefined) {
            let decision;
            try {
                decision = await this.eventPolicy.checkEvent({ event: verified, source });
            }
            catch (error) {
                this.mesh.dismissFrame(sourcePeer, message.eventId);
                throw error;
            }
            if (decision.type === 'drop') {
                this.mesh.dismissFrame(sourcePeer, message.eventId);
                return [];
            }
            priority = decision.priority;
        }
        const actions = this.mesh.receiveVerifiedFrame(sourcePeer, message.eventId, verified, peers, nowMs);
        return this.encodeActions(actions, { event: verified, source, priority });
    }
    selectPeers(peerIds) {
        const selected = new Map();
        for (const peerId of peerIds) {
            const peer = this.selectPeer(peerId);
            if (peer !== undefined)
                selected.set(peerId, peer);
        }
        return [...selected.entries()]
            .sort(([left], [right]) => left < right ? -1 : Number(left > right))
            .map(([, peer]) => peer);
    }
    selectPeer(peerId) {
        const selected = this.peerPolicy?.selectMeshPeer(peerId) ??
            (this.peerPolicy === undefined ? meshPeer(peerId) : undefined);
        return selected === undefined ? undefined : { ...selected, id: peerId };
    }
    ensureEventRecordFits(event) {
        this.codec.encode({ type: 'frame', eventId: event.id, event });
    }
    decodeRecords(peerId, bytes) {
        let decoder = this.inputs.get(peerId);
        if (decoder === undefined) {
            if (this.inputs.size >= this.options.maxInputPeers) {
                throw PubsubError.storage(`FIPS pubsub input peer limit is ${this.options.maxInputPeers}`);
            }
            decoder = new InvWantRecordDecoder(this.options.maxRecordBytes);
            this.inputs.set(peerId, decoder);
        }
        return decoder.push(bytes, this.options.maxRecordsPerReceive);
    }
    encodeActions(actions, admittedDelivery) {
        return actions.map((action) => {
            if (action.type === 'send') {
                return {
                    type: 'send',
                    peerId: action.peerId,
                    record: encodeInvWantRecord(this.codec.encode(action.message), this.options.maxRecordBytes),
                };
            }
            if (admittedDelivery === undefined) {
                throw PubsubError.storage('mesh delivered an event outside frame admission');
            }
            return { type: 'deliver', event: admittedDelivery };
        });
    }
}
function validateOptions(options) {
    if (options.protocol.trim() === '')
        throw validation('protocol must not be empty');
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
function requirePositive(value, name) {
    if (!Number.isSafeInteger(value) || value <= 0) {
        throw validation(`${name} must be greater than zero`);
    }
}
function requireU8(value, name) {
    if (!Number.isSafeInteger(value) || value < 0 || value > 255) {
        throw validation(`${name} must be an unsigned byte`);
    }
}
function validation(message) {
    return PubsubError.validation(message);
}
//# sourceMappingURL=fips-invwant-stream.js.map