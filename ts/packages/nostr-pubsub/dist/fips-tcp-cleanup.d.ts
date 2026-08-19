import type { ConnectionId, FipsTcpEndpoint } from '@fips/tcp';
type AbortableTcpEndpoint = Pick<FipsTcpEndpoint, 'abort' | 'state'>;
/**
 * Abort one tracked stream without reporting a concurrent remote close as an
 * application failure. `state()` and `abort()` are separate queued endpoint
 * operations, so an authenticated FIN/RST may release the tuple between them.
 */
export declare function abortTcpConnectionIfPresent(tcp: AbortableTcpEndpoint, id: ConnectionId): Promise<void>;
export {};
//# sourceMappingURL=fips-tcp-cleanup.d.ts.map