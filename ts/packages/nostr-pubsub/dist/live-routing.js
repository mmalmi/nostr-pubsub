import { allowWithPriority, } from './policy.js';
import { verifyNostrEvent } from './types.js';
const DEFAULT_LIVE_DEDUP_EVENTS = 4_096;
/** Opens every allowed live route and globally deduplicates noisy mesh/relay delivery. */
export async function subscribeRoutesWithPolicy(routes, filters, policy, handler, options = {}) {
    const maximum = options.maxSeenEvents ?? DEFAULT_LIVE_DEDUP_EVENTS;
    if (!Number.isSafeInteger(maximum) || maximum <= 0) {
        throw new RangeError('Live route deduplication limit must be a positive safe integer');
    }
    const seen = new Set();
    const order = [];
    const active = [];
    for (const source of routes) {
        const candidate = {
            source: source.route.source,
            priority: source.route.priority,
            reason: source.route.reason,
            health: {},
        };
        const decision = await policy.checkSource({
            candidate,
            authorPubkey: options.authorPubkey,
            capabilities: options.capabilities ?? source.route.capabilities,
        });
        if (decision.type === 'drop')
            continue;
        const subscription = await source.subscriber.subscribe(filters, (incoming) => {
            const event = verifyNostrEvent(incoming.event);
            if (seen.has(event.id))
                return;
            seen.add(event.id);
            order.push(event.id);
            while (order.length > maximum) {
                const removed = order.shift();
                if (removed !== undefined)
                    seen.delete(removed);
            }
            handler({ ...incoming, event, route: source.route });
        });
        active.push({ route: source.route, subscription });
    }
    return {
        routeIds: active.map(({ route }) => route.id),
        close: () => {
            for (const { subscription } of active.splice(0))
                subscription.close();
            seen.clear();
            order.length = 0;
        },
    };
}
export const allowAllLiveRoutes = {
    checkEvent: () => allowWithPriority(0),
    checkSource: () => allowWithPriority(0),
};
//# sourceMappingURL=live-routing.js.map