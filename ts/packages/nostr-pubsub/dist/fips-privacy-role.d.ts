import type { NostrEvent, NostrFilter } from './types.js';
import type { FipsPubsubWireMessage } from './wire.js';
export type FipsPrivacyRole = 'full' | 'receiver' | 'publisher' | {
    discovery: NostrFilter[];
};
/** Guards every final serialized send, including retained cache and WANT replay. */
export declare class FipsPrivacyRolePolicy {
    readonly role: FipsPrivacyRole;
    private readonly publicAuthors;
    private readonly scope?;
    constructor(role?: FipsPrivacyRole, publicAuthors?: () => readonly string[]);
    filters(filters: NostrFilter[]): void;
    publication(event: NostrEvent): void;
    outbound(message: FipsPubsubWireMessage): void;
    get ignoresRequests(): boolean;
    assertPublisher(event: NostrEvent): void;
}
//# sourceMappingURL=fips-privacy-role.d.ts.map