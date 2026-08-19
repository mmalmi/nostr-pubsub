/**
 * Abort one tracked stream without reporting a concurrent remote close as an
 * application failure. `state()` and `abort()` are separate queued endpoint
 * operations, so an authenticated FIN/RST may release the tuple between them.
 */
export async function abortTcpConnectionIfPresent(tcp, id) {
    if (await tcp.state(id) === undefined)
        return;
    try {
        await tcp.abort(id);
    }
    catch (error) {
        if (await tcp.state(id) === undefined)
            return;
        throw error;
    }
}
//# sourceMappingURL=fips-tcp-cleanup.js.map