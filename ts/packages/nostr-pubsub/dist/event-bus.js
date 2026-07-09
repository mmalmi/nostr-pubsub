import { filterLimit, filtersMatch } from './filter.js';
import { allowWithPriority, reportParts } from './policy.js';
import { verifyNostrEvent, } from './types.js';
export class InMemoryEventBus {
    policy;
    events = [];
    constructor(policy) {
        this.policy = policy;
    }
    async publish(event, source) {
        const verifiedEvent = verifyNostrEvent(event);
        const decision = this.policy === undefined
            ? allowWithPriority(0)
            : await this.policy.checkEvent({ event: verifiedEvent, source });
        const report = reportParts(decision);
        if (report.accepted) {
            this.events.push({ event: verifiedEvent, source, priority: report.priority });
        }
        return report;
    }
    async query(filters, options = {}) {
        const limit = options.limit ?? filterLimit(filters);
        const events = [];
        for (const stored of this.events) {
            if (limit !== undefined && events.length >= limit)
                break;
            if (filtersMatch(filters, stored.event)) {
                events.push({ ...stored });
            }
        }
        return { events };
    }
}
//# sourceMappingURL=event-bus.js.map