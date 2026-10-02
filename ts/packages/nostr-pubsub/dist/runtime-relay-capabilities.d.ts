/** Bounded NIP-11 eligibility cache. A positive result is only permission to try NEG. */
export declare class RelayReconciliationCapabilities {
    private readonly fetcher;
    private readonly timeoutMs;
    private readonly cache;
    constructor(fetcher: typeof fetch | undefined, timeoutMs: number);
    supports(url: string, signal: AbortSignal): Promise<boolean>;
}
//# sourceMappingURL=runtime-relay-capabilities.d.ts.map