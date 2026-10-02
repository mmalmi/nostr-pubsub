/** Bounded NIP-11 eligibility cache. A positive result is only permission to try NEG. */
export class RelayReconciliationCapabilities {
    fetcher;
    timeoutMs;
    cache = new Map();
    constructor(fetcher, timeoutMs) {
        this.fetcher = fetcher;
        this.timeoutMs = timeoutMs;
    }
    async supports(url, signal) {
        const previous = this.cache.get(url);
        if (previous && previous.expires > Date.now())
            return previous.supported;
        if (!this.fetcher || signal.aborted)
            return false;
        const controller = new AbortController();
        const cancel = () => controller.abort();
        signal.addEventListener('abort', cancel, { once: true });
        const timer = setTimeout(cancel, this.timeoutMs);
        let supported = false;
        try {
            const endpoint = new URL(url);
            endpoint.protocol = endpoint.protocol === 'wss:' ? 'https:' : 'http:';
            const response = await this.fetcher(endpoint.toString(), {
                headers: { Accept: 'application/nostr+json' }, credentials: 'omit', redirect: 'error', signal: controller.signal,
            });
            if (response.ok && response.body) {
                const reader = response.body.getReader();
                const chunks = [];
                let size = 0;
                try {
                    for (;;) {
                        const part = await reader.read();
                        if (part.done)
                            break;
                        size += part.value.byteLength;
                        if (size > 65536)
                            throw new RangeError('Relay metadata too large');
                        chunks.push(part.value);
                    }
                    const bytes = new Uint8Array(size);
                    let offset = 0;
                    for (const chunk of chunks) {
                        bytes.set(chunk, offset);
                        offset += chunk.byteLength;
                    }
                    const metadata = JSON.parse(new TextDecoder().decode(bytes));
                    supported = typeof metadata === 'object' && metadata !== null && 'supported_nips' in metadata
                        && Array.isArray(metadata.supported_nips) && metadata.supported_nips.includes(77);
                }
                finally {
                    await reader.cancel().catch(() => undefined);
                    reader.releaseLock();
                }
            }
            else
                await response.body?.cancel();
        }
        catch { /* Ordinary REQ remains the fallback for CORS, metadata and network errors. */ }
        finally {
            clearTimeout(timer);
            signal.removeEventListener('abort', cancel);
        }
        if (!signal.aborted) {
            this.cache.delete(url);
            this.cache.set(url, { supported, expires: Date.now() + 300000 });
            if (this.cache.size > 128)
                this.cache.delete(this.cache.keys().next().value);
        }
        return supported && !signal.aborted;
    }
}
//# sourceMappingURL=runtime-relay-capabilities.js.map