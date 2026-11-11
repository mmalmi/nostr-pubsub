import type { NostrEvent } from './types.js';
/** NIP-65 relay list metadata. */
export declare const RELAY_LIST_KIND = 10002;
export declare const MAX_RELAY_URL_LENGTH = 256;
export type RelayListUse = 'read' | 'write';
export interface RelayListEntry {
    url: string;
    read: boolean;
    write: boolean;
}
export interface RankedRelay {
    url: string;
    /** Distinct counted authors announcing this relay for the requested use. */
    authors: number;
}
export interface RelayRankOptions {
    /** Count only these authors, typically the caller's follows. Omit to count every list supplied. */
    authors?: Iterable<string>;
    /** `write` is where authors publish, `read` where they receive. Default `write`. */
    use?: RelayListUse;
    /** Omit relays announced by fewer counted authors. Default 3. */
    minAuthors?: number;
    /** Ignore lists naming more relays than this; they carry no routing signal. Default 10. */
    maxRelaysPerAuthor?: number;
    /** `popularity` orders by announcing authors. `coverage` orders by authors not yet
     * reached through an earlier relay and stops once nothing new is reached. Default `popularity`. */
    order?: 'popularity' | 'coverage';
    /** Relay URLs to leave out, such as those already configured. */
    exclude?: Iterable<string>;
    limit?: number;
}
/** Canonical public `wss:` relay URL, or undefined for anything a stranger should not make us dial. */
export declare function normalizeRelayUrl(value: unknown): string | undefined;
/** The valid relays one relay-list event announces, in list order. Pass verified events. */
export declare function relayListEntries(event: NostrEvent): RelayListEntry[];
/**
 * Rank relays by how many of the given authors currently announce them. Only each
 * author's newest list counts. The result is a suggestion: nothing here connects
 * anywhere, and the caller decides whether to use any of it.
 */
export declare function rankRelays(events: Iterable<NostrEvent>, options?: RelayRankOptions): RankedRelay[];
//# sourceMappingURL=relay-lists.d.ts.map