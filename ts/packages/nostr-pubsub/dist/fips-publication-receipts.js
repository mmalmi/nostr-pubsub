/** One event per endpoint lifetime. A local queue/cache report is never a receipt. */
export class FipsPublicationReceipts {
    used = false;
    pending;
    async publish(event, peers, send, options) {
        options.signal?.throwIfAborted();
        const timeoutMs = options.timeoutMs ?? 10_000;
        if (this.used || !peers.length || !Number.isSafeInteger(timeoutMs) || timeoutMs < 1)
            throw new Error('Anonymous publication requires a fresh endpoint, explicit provider and positive timeout');
        this.used = true;
        return await new Promise((resolve, reject) => {
            let settled = false;
            const finish = (error, receipt) => {
                if (settled)
                    return;
                settled = true;
                clearTimeout(timer);
                options.signal?.removeEventListener('abort', cancel);
                this.pending = undefined;
                if (error)
                    reject(error);
                else
                    resolve(receipt);
            };
            const cancel = () => finish(new DOMException('Anonymous publication aborted', 'AbortError'));
            const timer = setTimeout(() => finish(new Error('Anonymous provider receipt timed out')), timeoutMs);
            this.pending = { id: event.id, peers: new Set(peers), finish };
            options.signal?.addEventListener('abort', cancel, { once: true });
            for (const peer of peers)
                void send(peer, { type: 'event', event }).catch(() => this.receive(peer, event.id, false, 'transport failed'));
        });
    }
    receive(peer, eventId, accepted, message) {
        const pending = this.pending;
        if (!pending || pending.id !== eventId || !pending.peers.delete(peer))
            return;
        if (accepted)
            pending.finish(undefined, { eventId, provider: peer });
        else if (!pending.peers.size)
            pending.finish(new Error(`Provider rejected anonymous publication: ${message}`));
    }
    close() { this.pending?.finish(new Error('Anonymous publisher closed')); }
}
//# sourceMappingURL=fips-publication-receipts.js.map