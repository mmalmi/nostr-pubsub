import { PubsubPeerSubscriptionStore } from './subscription.js';
import { PubsubError } from './types.js';
import { type FipsNostrPubsubClientLimits } from './fips-pubsub-client-types.js';
export declare function validateClientLimits(overrides: Partial<FipsNostrPubsubClientLimits> | undefined): FipsNostrPubsubClientLimits;
export declare function createClientPeerSubscriptionStore(limits: FipsNostrPubsubClientLimits): PubsubPeerSubscriptionStore;
export declare function normalizeAllowedKinds(kinds: readonly number[] | undefined): Set<number> | undefined;
export declare function normalizePeerId(value: unknown): string | undefined;
export declare function parseConnectionEvent(event: unknown): {
    peerId: string;
    connected: boolean;
} | undefined;
export declare function rememberId(ids: Set<string>, order: string[], id: string, maximum: number): void;
export declare function clientError(message: string): PubsubError;
//# sourceMappingURL=fips-pubsub-client-support.d.ts.map