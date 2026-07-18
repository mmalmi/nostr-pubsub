import { subscribeRoutesWithPolicy, } from './live-routing.js';
import { queryRoutesWithPolicy, } from './routing.js';
/** Owned transport-neutral router for indexes, FIPS peers, and Nostr relays. */
export class NostrPubsubRouter {
    policy;
    querySources;
    publishSources;
    liveSources;
    constructor(options) {
        this.policy = options.policy;
        this.querySources = [...(options.querySources ?? [])];
        this.publishSources = [...(options.publishSources ?? [])];
        this.liveSources = [...(options.liveSources ?? [])];
    }
    queryWithContext(filters, options = {}, authorPubkey, capabilities) {
        return queryRoutesWithPolicy(this.querySources, filters, options, this.policy, authorPubkey, capabilities);
    }
    async query(filters, options = {}) {
        const report = await this.queryWithContext(filters, { query: options });
        return {
            events: report.events.map(({ event, source, priority }) => ({ event, source, priority })),
            complete: report.complete,
        };
    }
    async publish(event, source) {
        const selected = [];
        for (const target of this.publishSources) {
            const candidate = {
                source: target.route.source,
                priority: target.route.priority,
                reason: target.route.reason,
                health: {},
            };
            const decision = await this.policy.checkSource({
                candidate,
                capabilities: target.route.capabilities,
            });
            if (decision.type !== 'drop')
                selected.push(target);
        }
        const reports = await Promise.all(selected.map(async (target) => {
            try {
                return { routeId: target.route.id, report: await target.publisher.publish(event, source) };
            }
            catch (error) {
                return { routeId: target.route.id, error };
            }
        }));
        const accepted = reports.filter((result) => result.report?.accepted === true);
        const failures = reports.flatMap((result) => {
            if (result.report?.accepted === true)
                return [];
            if (result.error !== undefined)
                return [`${result.routeId}: ${errorMessage(result.error)}`];
            return [`${result.routeId}: ${result.report?.reason ?? 'rejected'}`];
        });
        return {
            accepted: accepted.length > 0,
            priority: accepted.length === 0
                ? 0
                : accepted.reduce((maximum, result) => Math.max(maximum, result.report.priority), Number.NEGATIVE_INFINITY),
            reason: failures.length > 0
                ? failures.join('; ')
                : selected.length === 0 ? 'no publish route was selected' : undefined,
        };
    }
    subscribeWithOptions(filters, handler, options = {}) {
        return subscribeRoutesWithPolicy(this.liveSources, filters, this.policy, handler, options);
    }
    async subscribe(filters, handler) {
        return this.subscribeWithOptions(filters, ({ event, source, priority }) => {
            handler({ event, source, priority });
        });
    }
}
function errorMessage(error) {
    return error instanceof Error ? error.message : String(error);
}
//# sourceMappingURL=router.js.map