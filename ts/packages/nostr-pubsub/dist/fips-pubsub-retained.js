import { inventoryMessage } from './fips-pubsub-client-protocol.js';
/** Only queries application-owned retained storage; never discovers peers or fetches remotely. */
export class FipsRetainedReplay {
    cache;
    reader;
    limit;
    maxHops;
    admit;
    interested;
    send;
    pending = new Map();
    constructor(cache, reader, limit, maxHops, admit, interested, send) {
        this.cache = cache;
        this.reader = reader;
        this.limit = limit;
        this.maxHops = maxHops;
        this.admit = admit;
        this.interested = interested;
        this.send = send;
    }
    async replay(peer, subscription, filters) {
        const announced = new Set();
        const offer = async (event, hopLimit) => {
            if (announced.size >= this.limit || announced.has(event.id) || !this.interested(peer, subscription, event))
                return;
            announced.add(event.id);
            this.cache.remember(event, undefined, hopLimit);
            await this.send(peer, inventoryMessage(event, [subscription], hopLimit));
        };
        for (const cached of this.cache.replay(filters, this.limit))
            await offer(cached.event, cached.hopLimit);
        if (!this.reader || announced.size >= this.limit)
            return;
        const key = `${peer}:${subscription}`;
        if (this.pending.has(key) || this.pending.size >= 32)
            return;
        const controller = new AbortController();
        this.pending.set(key, controller);
        let timer;
        try {
            const query = this.reader.query(filters, { limit: this.limit, deadline: Date.now() + 2000, signal: controller.signal });
            const report = await Promise.race([query, new Promise((_, reject) => {
                    timer = setTimeout(() => { controller.abort(); reject(new Error('Retained event query timed out')); }, 2000);
                })]);
            if (controller.signal.aborted)
                return;
            for (const { event } of report.events.slice(0, this.limit)) {
                try {
                    await offer(this.admit(event), this.maxHops);
                }
                catch { /* invalid/disallowed retained events are never advertised */ }
            }
        }
        finally {
            if (timer)
                clearTimeout(timer);
            this.pending.delete(key);
        }
    }
    close() { for (const controller of this.pending.values())
        controller.abort(); this.pending.clear(); }
}
//# sourceMappingURL=fips-pubsub-retained.js.map