import { FipsTcpEndpoint, State, } from '@fips/tcp';
import { InvWantRecordQueues, } from './fips-invwant-tcp-queue.js';
import { MonitoredFipsEndpoint, fipsInvWantTcpPeerOrderKey, validateFipsInvWantTcpOptions, } from './fips-invwant-tcp-types.js';
import { PubsubError } from './types.js';
const STREAM_IO_CHUNK_BYTES = 16 * 1024;
const MAX_READY_INPUT_TURNS = 16;
const ACTIVE_STATES = new Set([State.Established, State.CloseWait]);
const CONNECTING_STATES = new Set([
    State.SynSent,
    State.SynReceived,
    State.Established,
    State.CloseWait,
]);
/** Manually driven reliable Inv/WANT service over authenticated FIPS peers. */
export class FipsInvWantTcpDriver {
    stream;
    options;
    tcp;
    monitored;
    connections = new Map();
    active = new Map();
    queues;
    localPeerOrderKey;
    disposed = false;
    constructor(localPeerId, stream, options, endpoint, isnSeed) {
        this.stream = stream;
        this.options = options;
        this.monitored = new MonitoredFipsEndpoint(endpoint);
        this.tcp = new FipsTcpEndpoint(this.monitored, options.servicePort, {
            receiveBuffer: 0xffff,
            sendBuffer: options.maxQueuedBytesPerPeer,
            maxConnections: options.maxPeers * 2,
            maxConnectionsPerPeer: 2,
        }, isnSeed);
        this.queues = new InvWantRecordQueues(options);
        this.localPeerOrderKey = fipsInvWantTcpPeerOrderKey(localPeerId);
    }
    static bind(endpoint, localPeerId, stream, options, isnSeed = 1n) {
        validateFipsInvWantTcpOptions(options);
        if (localPeerId.trim() === '')
            throw validation('local peer identity must not be empty');
        return new FipsInvWantTcpDriver(localPeerId, stream, options, endpoint, isnSeed);
    }
    async connectPeer(peer, nowMs = Date.now()) {
        this.ensureOpen();
        if (peer.trim() === '')
            throw validation('peer identity must not be empty');
        for (const [id, connection] of this.connections) {
            const state = await this.tcp.state(id);
            if (connection.peer === peer &&
                state !== undefined &&
                CONNECTING_STATES.has(state))
                return;
        }
        this.ensurePeerCapacity(peer);
        const id = await transport('connect TCP/FIPS pubsub peer', this.tcp.connect(peer, nowMs));
        const authenticated = await this.tcp.peer(id);
        if (authenticated !== peer) {
            await this.tcp.abort(id);
            throw storage('outbound TCP/FIPS stream peer identity changed');
        }
        this.connections.set(id, { peer, direction: 'outbound' });
    }
    seed(event, nowMs) {
        this.ensureOpen();
        this.stream.seed(event, nowMs);
    }
    publish(event, nowMs) {
        this.ensureOpen();
        this.applyActions(this.stream.publish(event, this.active.keys(), nowMs), []);
        return this.queueSnapshot();
    }
    async receive(nowMs = Date.now()) {
        this.ensureOpen();
        const report = this.newReport();
        await this.driveReady(nowMs, report);
        Object.assign(report, this.monitored.drainCounters());
        return report;
    }
    async poll(nowMs = Date.now()) {
        this.ensureOpen();
        await transport('poll TCP/FIPS pubsub transport', this.tcp.poll(nowMs));
        const report = this.newReport();
        await this.driveReady(nowMs, report);
        Object.assign(report, this.monitored.drainCounters());
        return report;
    }
    async abortPeer(peer) {
        this.ensureOpen();
        const ids = [...this.connections]
            .filter(([, connection]) => connection.peer === peer)
            .map(([id]) => id);
        for (const id of ids) {
            if (await this.tcp.state(id) !== undefined) {
                await transport('abort TCP/FIPS pubsub peer', this.tcp.abort(id));
            }
            this.connections.delete(id);
        }
        this.active.delete(peer);
        this.queues.delete(peer);
        this.stream.disconnectPeer(peer);
    }
    connectedPeerCount() {
        return this.active.size;
    }
    queueSnapshot() {
        return this.queues.snapshot();
    }
    async dispose() {
        if (this.disposed)
            return;
        this.disposed = true;
        const ids = [...this.connections.keys()];
        let failure;
        try {
            for (const id of ids) {
                try {
                    if (await this.tcp.state(id) !== undefined)
                        await this.tcp.abort(id);
                }
                catch (error) {
                    failure ??= error;
                }
            }
        }
        finally {
            this.connections.clear();
            this.active.clear();
            this.queues.clear();
            await this.tcp.dispose();
        }
        if (failure !== undefined) {
            throw storage(`dispose TCP/FIPS pubsub driver: ${errorMessage(failure)}`);
        }
    }
    newReport() {
        return {
            fipsDatagrams: 0,
            rejectedTcpSegments: 0,
            streamBytesRead: 0,
            streamBytesWritten: 0,
            connectedPeers: 0,
            deliveries: [],
        };
    }
    async driveReady(nowMs, report) {
        this.stream.maintain(nowMs);
        await this.acceptConnections();
        await this.refreshActive(nowMs, report);
        await this.readActive(nowMs, report);
        await this.flushQueues(nowMs, report);
        await this.finishRemoteCloses(nowMs);
        await this.refreshActive(nowMs, report);
        report.connectedPeers = this.active.size;
    }
    async acceptConnections() {
        for (;;) {
            const id = await this.tcp.accept();
            if (id === undefined)
                return;
            const peer = await this.tcp.peer(id);
            if (peer === undefined) {
                throw storage('accepted TCP/FIPS stream has no authenticated peer');
            }
            try {
                this.ensurePeerCapacity(peer);
            }
            catch {
                await transport('reject excess TCP/FIPS peer', this.tcp.abort(id));
                continue;
            }
            if (!this.connections.has(id))
                this.connections.set(id, { peer, direction: 'inbound' });
        }
    }
    async refreshActive(nowMs, report) {
        const candidates = new Map();
        for (const [id, connection] of [...this.connections]) {
            const state = await this.tcp.state(id);
            if (state === undefined) {
                this.connections.delete(id);
                continue;
            }
            const authenticated = await this.tcp.peer(id);
            if (authenticated !== connection.peer) {
                await transport('reject mismatched TCP/FIPS peer', this.tcp.abort(id));
                this.connections.delete(id);
                throw storage('TCP/FIPS stream peer identity changed');
            }
            if (ACTIVE_STATES.has(state)) {
                const streams = candidates.get(connection.peer) ?? [];
                streams.push([id, connection.direction]);
                candidates.set(connection.peer, streams);
            }
        }
        const next = new Map();
        const extras = [];
        for (const peer of [...candidates.keys()].sort()) {
            const preferOutbound = this.localPeerOrderKey < fipsInvWantTcpPeerOrderKey(peer);
            const streams = candidates.get(peer);
            streams.sort(([leftId, left], [rightId, right]) => {
                const leftPreferred = (left === 'outbound') === preferOutbound;
                const rightPreferred = (right === 'outbound') === preferOutbound;
                return Number(rightPreferred) - Number(leftPreferred) || leftId - rightId;
            });
            next.set(peer, streams[0][0]);
            extras.push(...streams.slice(1).map(([id]) => id));
        }
        for (const id of extras) {
            await transport('deduplicate TCP/FIPS pubsub stream', this.tcp.abort(id));
            this.connections.delete(id);
        }
        const changed = new Set([...this.active.keys(), ...next.keys()]);
        const previous = this.active;
        this.active = next;
        for (const peer of [...changed].sort()) {
            if (previous.get(peer) === next.get(peer))
                continue;
            this.stream.disconnectPeer(peer);
            this.queues.delete(peer);
            if (next.has(peer)) {
                this.applyActions(this.stream.peerConnected(peer, nowMs), report.deliveries);
            }
        }
    }
    async readActive(nowMs, report) {
        const connected = [...this.active.keys()];
        let budget = this.options.maxIoBytesPerDrive;
        for (const [peer, id] of this.active) {
            for (let turns = 0; turns < MAX_READY_INPUT_TURNS; turns += 1) {
                if (this.stream.hasReadyInput(peer)) {
                    this.applyActions(await this.stream.receiveBytes(peer, new Uint8Array(), connected, nowMs), report.deliveries);
                    continue;
                }
                if (budget === 0)
                    break;
                const maximum = Math.min(this.stream.remainingInputCapacity(peer), STREAM_IO_CHUNK_BYTES, budget);
                if (maximum === 0)
                    break;
                const bytes = await transport('read TCP/FIPS pubsub stream', this.tcp.read(id, maximum, nowMs));
                if (bytes.byteLength === 0)
                    break;
                budget -= bytes.byteLength;
                report.streamBytesRead += bytes.byteLength;
                this.applyActions(await this.stream.receiveBytes(peer, bytes, connected, nowMs), report.deliveries);
            }
        }
    }
    async flushQueues(nowMs, report) {
        let budget = this.options.maxIoBytesPerDrive;
        for (const [peer, id] of this.active) {
            while (budget > 0) {
                const chunk = this.queues.nextChunk(peer, Math.min(STREAM_IO_CHUNK_BYTES, budget));
                if (chunk === undefined)
                    break;
                const accepted = await transport('write TCP/FIPS pubsub stream', this.tcp.write(id, chunk, nowMs));
                if (accepted === 0)
                    break;
                budget -= accepted;
                report.streamBytesWritten += accepted;
                this.queues.accept(peer, accepted);
            }
        }
    }
    async finishRemoteCloses(nowMs) {
        for (const [peer, id] of this.active) {
            if (await this.tcp.isReadClosed(id) && !this.queues.has(peer)) {
                await transport('close TCP/FIPS pubsub stream', this.tcp.close(id, nowMs));
            }
        }
    }
    applyActions(actions, deliveries) {
        const sends = [];
        const admitted = [];
        for (const action of actions) {
            if (action.type === 'send')
                sends.push({ peerId: action.peerId, record: action.record });
            else
                admitted.push(action.event);
        }
        this.queues.enqueue(sends);
        deliveries.push(...admitted);
    }
    ensurePeerCapacity(peer) {
        const peers = new Set([...this.connections.values()].map((connection) => connection.peer));
        if (!peers.has(peer) && peers.size >= this.options.maxPeers) {
            throw storage(`TCP/FIPS pubsub peer limit is ${this.options.maxPeers}`);
        }
    }
    ensureOpen() {
        if (this.disposed)
            throw storage('TCP/FIPS pubsub driver is disposed');
    }
}
async function transport(context, operation) {
    try {
        return await operation;
    }
    catch (error) {
        throw storage(`${context}: ${errorMessage(error)}`);
    }
}
function errorMessage(error) {
    return error instanceof Error ? error.message : String(error);
}
function validation(message) {
    return PubsubError.validation(message);
}
function storage(message) {
    return PubsubError.storage(message);
}
//# sourceMappingURL=fips-invwant-tcp-driver.js.map