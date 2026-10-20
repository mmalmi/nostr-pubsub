import { splitRelayDiscoveryFilters } from './anonymous-relay-discovery.js';
import { isPrivateRelayEvent } from './private-relay-publisher.js';
/** Keep the live receiver, anonymous private sending and public lookups separate.
 * Public gossip remains queued; the publisher's event-specific remote receipt
 * passes through unchanged so a real provider acceptance can retire its event. */
export function createScopedFipsSource(options) {
    const controller = new AbortController();
    const subscriptions = new Set();
    const requireOpen = () => controller.signal.throwIfAborted();
    const routes = (filters) => {
        requireOpen();
        if (!filters.length)
            throw new RangeError('FIPS subscriptions require at least one filter');
        const split = splitRelayDiscoveryFilters(filters);
        return [
            ...(split.receiver.length ? [{ source: options.receiver, filters: split.receiver }] : []),
            ...split.discovery.map(filters => ({ source: options.discovery, filters })),
        ];
    };
    return {
        id: options.id ?? 'fips',
        publishAcceptance: 'queued',
        async publish(event, origin) {
            requireOpen();
            if (isPrivateRelayEvent(event))
                return options.publisher.publish(event, origin);
            if (!options.receiver.publish)
                throw new Error('Public FIPS publication is unavailable');
            return options.receiver.publish(event, origin);
        },
        async query(filters, queryOptions = {}) {
            const signal = queryOptions.signal
                ? AbortSignal.any([queryOptions.signal, controller.signal]) : controller.signal;
            signal.throwIfAborted();
            const reports = await Promise.all(routes(filters).map(route => {
                if (!route.source.query)
                    throw new Error('FIPS history is unavailable');
                return route.source.query(route.filters, { ...queryOptions, signal });
            }));
            signal.throwIfAborted();
            return { complete: reports.every(report => report.complete !== false),
                events: [...new Map(reports.flatMap(report => report.events).map(event => [event.event.id, event])).values()] };
        },
        async subscribe(filters, handler) {
            let closed = false;
            const reports = await Promise.allSettled(routes(filters).map(async (route) => {
                if (!route.source.subscribe)
                    throw new Error('FIPS subscriptions are unavailable');
                return route.source.subscribe(route.filters, event => {
                    if (!closed && !controller.signal.aborted)
                        handler(event);
                });
            }));
            const opened = reports.flatMap(report => report.status === 'fulfilled' ? [report.value] : []);
            const failed = reports.find(report => report.status === 'rejected');
            if (failed || controller.signal.aborted) {
                closed = true;
                for (const subscription of opened)
                    subscription.close();
                if (failed?.status === 'rejected')
                    throw failed.reason;
                requireOpen();
            }
            const subscription = { close() {
                    if (closed)
                        return;
                    closed = true;
                    subscriptions.delete(subscription);
                    for (const handle of opened)
                        handle.close();
                } };
            subscriptions.add(subscription);
            return subscription;
        },
        close() {
            if (controller.signal.aborted)
                return;
            controller.abort();
            for (const subscription of subscriptions)
                subscription.close();
        },
    };
}
//# sourceMappingURL=scoped-fips-source.js.map