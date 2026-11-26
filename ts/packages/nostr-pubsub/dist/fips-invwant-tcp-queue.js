import { saturatingAdd } from './mesh-state.js';
import { PubsubError } from './types.js';
export class InvWantRecordQueues {
    options;
    queues = new Map();
    constructor(options) {
        this.options = options;
    }
    snapshot() {
        let records = 0, bytes = 0;
        for (const queue of this.queues.values()) {
            records = saturatingAdd(records, queue.records.length - queue.head);
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
        let newPeers = 0;
        for (const [peer, addition] of additions) {
            const queue = this.queues.get(peer);
            if (!queue)
                newPeers += 1;
            if ((queue ? queue.records.length - queue.head : 0) + addition.records > this.options.maxQueuedRecordsPerPeer) {
                throw storage(`TCP/FIPS pubsub queue record count limit reached for ${peer}`);
            }
            if ((queue?.bytes ?? 0) + addition.bytes > this.options.maxQueuedBytesPerPeer) {
                throw storage(`TCP/FIPS pubsub queue byte count limit reached for ${peer}`);
            }
        }
        if (this.queues.size + newPeers > this.options.maxPeers)
            throw storage('TCP/FIPS pubsub queue peer limit reached');
        for (const { peerId, record } of records) {
            const queue = this.queues.get(peerId) ?? { records: [], head: 0, bytes: 0 };
            queue.records.push({ bytes: record, offset: 0 });
            queue.bytes += record.byteLength;
            this.queues.set(peerId, queue);
        }
    }
    nextChunk(peerId, maximum) {
        const queue = this.queues.get(peerId);
        if (queue === undefined || queue.head >= queue.records.length || maximum <= 0)
            return undefined;
        const first = queue.records[queue.head];
        const available = first.bytes.byteLength - first.offset;
        if (queue.head + 1 === queue.records.length || available >= maximum) {
            return first.bytes.slice(first.offset, first.offset + Math.min(available, maximum));
        }
        // Coalesce queued records into one bounded TCP write so replay bursts pack
        // multiple records per segment instead of emitting one segment per frame.
        const total = Math.min(queue.bytes, maximum);
        const chunk = new Uint8Array(total);
        for (let index = queue.head, written = 0; index < queue.records.length && written < total; index += 1) {
            const record = queue.records[index];
            const take = Math.min(record.bytes.byteLength - record.offset, total - written);
            chunk.set(record.bytes.subarray(record.offset, record.offset + take), written);
            written += take;
        }
        return chunk;
    }
    accept(peerId, bytes) {
        const queue = this.queues.get(peerId);
        if (queue === undefined || queue.head >= queue.records.length || bytes <= 0)
            return;
        let remaining = Math.min(bytes, queue.bytes);
        queue.bytes = Math.max(0, queue.bytes - remaining);
        while (remaining > 0 && queue.head < queue.records.length) {
            const record = queue.records[queue.head];
            const consumed = Math.min(remaining, record.bytes.byteLength - record.offset);
            record.offset += consumed;
            remaining -= consumed;
            if (record.offset === record.bytes.byteLength)
                queue.head += 1;
        }
        if (queue.head >= queue.records.length)
            this.queues.delete(peerId);
        else if (queue.head >= 32 && queue.head * 2 >= queue.records.length) {
            queue.records.splice(0, queue.head);
            queue.head = 0;
        }
    }
    has(peerId) { return this.queues.has(peerId); }
    delete(peerId) { this.queues.delete(peerId); }
    clear() { this.queues.clear(); }
    restart(peerId) {
        const queue = this.queues.get(peerId);
        if (queue === undefined)
            return;
        if (queue.head > 0) {
            queue.records.splice(0, queue.head);
            queue.head = 0;
        }
        queue.bytes = 0;
        for (const record of queue.records) {
            record.offset = 0;
            queue.bytes = saturatingAdd(queue.bytes, record.bytes.byteLength);
        }
    }
}
function storage(message) { return PubsubError.storage(message); }
//# sourceMappingURL=fips-invwant-tcp-queue.js.map