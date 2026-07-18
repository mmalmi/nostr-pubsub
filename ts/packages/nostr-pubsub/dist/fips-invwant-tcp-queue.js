import { saturatingAdd } from './mesh-state.js';
import { PubsubError } from './types.js';
export class InvWantRecordQueues {
    options;
    queues = new Map();
    constructor(options) {
        this.options = options;
    }
    snapshot() {
        let records = 0;
        let bytes = 0;
        for (const queue of this.queues.values()) {
            records = saturatingAdd(records, queue.records.length);
            bytes = saturatingAdd(bytes, queue.bytes);
        }
        return { peers: this.queues.size, records, bytes };
    }
    enqueue(records) {
        const additions = new Map();
        for (const { peerId, record } of records) {
            const addition = additions.get(peerId) ?? { records: 0, bytes: 0 };
            addition.records += 1;
            addition.bytes += record.byteLength;
            additions.set(peerId, addition);
        }
        const newPeers = [...additions.keys()].filter((peer) => !this.queues.has(peer)).length;
        if (this.queues.size + newPeers > this.options.maxPeers) {
            throw storage('TCP/FIPS pubsub queue peer limit reached');
        }
        for (const [peer, addition] of additions) {
            const queue = this.queues.get(peer);
            if ((queue?.records.length ?? 0) + addition.records >
                this.options.maxQueuedRecordsPerPeer) {
                throw queueFull(peer, 'record count');
            }
            if ((queue?.bytes ?? 0) + addition.bytes > this.options.maxQueuedBytesPerPeer) {
                throw queueFull(peer, 'byte count');
            }
        }
        for (const { peerId, record } of records) {
            const queue = this.queues.get(peerId) ?? { records: [], bytes: 0 };
            queue.records.push({ bytes: record, offset: 0 });
            queue.bytes += record.byteLength;
            this.queues.set(peerId, queue);
        }
    }
    nextChunk(peerId, maximum) {
        const record = this.queues.get(peerId)?.records[0];
        if (record === undefined)
            return undefined;
        return record.bytes.slice(record.offset, Math.min(record.offset + maximum, record.bytes.length));
    }
    accept(peerId, bytes) {
        const queue = this.queues.get(peerId);
        const record = queue?.records[0];
        if (queue === undefined || record === undefined || bytes <= 0)
            return;
        record.offset += bytes;
        queue.bytes = Math.max(0, queue.bytes - bytes);
        if (record.offset === record.bytes.byteLength)
            queue.records.shift();
        if (queue.records.length === 0)
            this.queues.delete(peerId);
    }
    has(peerId) {
        return this.queues.has(peerId);
    }
    delete(peerId) {
        this.queues.delete(peerId);
    }
    restart(peerId) {
        const queue = this.queues.get(peerId);
        if (queue === undefined)
            return;
        queue.bytes = 0;
        for (const record of queue.records) {
            record.offset = 0;
            queue.bytes = saturatingAdd(queue.bytes, record.bytes.byteLength);
        }
    }
    clear() {
        this.queues.clear();
    }
}
function queueFull(peer, resource) {
    return storage(`TCP/FIPS pubsub queue ${resource} limit reached for ${peer}`);
}
function storage(message) {
    return PubsubError.storage(message);
}
//# sourceMappingURL=fips-invwant-tcp-queue.js.map