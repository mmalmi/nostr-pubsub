import type { NostrEventPublisher, NostrEventReader, NostrEventSubscriber } from './event-bus.js';
import type { RuntimeSource } from './runtime-types.js';
export interface ScopedFipsSourceOptions {
    /** A role:receiver source on the persistent live/file endpoint. */
    receiver: RuntimeSource;
    /** A fresh anonymous endpoint publisher, never the receiver's publisher. */
    publisher: NostrEventPublisher;
    /** Fresh, fixed-subject anonymous endpoint operations. */
    discovery: NostrEventReader & NostrEventSubscriber;
    id?: string;
}
/** Keep the live receiver, anonymous private sending and public lookups separate.
 * Public gossip remains queued; the publisher's event-specific remote receipt
 * passes through unchanged so a real provider acceptance can retire its event. */
export declare function createScopedFipsSource(options: ScopedFipsSourceOptions): RuntimeSource & {
    close(): void;
};
//# sourceMappingURL=scoped-fips-source.d.ts.map