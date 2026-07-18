import { FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES } from './wire.js';
export function defaultFipsNostrPubsubClientLimits() {
    return {
        maxPeers: 64,
        maxActiveSubscriptions: 64,
        maxSubscriptionsPerPeer: 64,
        maxFiltersPerSubscription: 4,
        maxReplayEvents: 8,
        maxCachedEvents: 256,
        maxFrameBytes: FIPS_NOSTR_PUBSUB_MAX_FRAME_BYTES,
        maxHops: 4,
        receiveBatchSize: 64,
    };
}
//# sourceMappingURL=fips-pubsub-client-types.js.map