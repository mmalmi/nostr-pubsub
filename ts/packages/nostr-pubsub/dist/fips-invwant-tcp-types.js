import { npubEncode } from 'nostr-tools/nip19';
import { saturatingAdd } from './mesh-state.js';
import { PubsubError } from './types.js';
export function fipsInvWantTcpCapabilityName(options) {
    const namespace = options.serviceNamespace.trim().replace(/\/+$/, '');
    return `${namespace}/${options.serviceVersion}`;
}
/** Match Rust's npub ordering when TypeScript FIPS exposes a hex peer key. */
export function fipsInvWantTcpPeerOrderKey(peerId) {
    const lowercase = peerId.toLowerCase();
    if (/^(02|03)[0-9a-f]{64}$/.test(lowercase))
        return npubEncode(lowercase.slice(2));
    if (/^[0-9a-f]{64}$/.test(lowercase))
        return npubEncode(lowercase);
    return lowercase.startsWith('npub1') ? lowercase : peerId;
}
export function validateFipsInvWantTcpOptions(options) {
    if (options.serviceNamespace.trim().replace(/\/+$/, '') === '') {
        throw validation('service namespace must not be empty');
    }
    unsigned(options.serviceVersion, 0xff, 'service version');
    unsigned(options.servicePort, 0xffff, 'service port', false);
    positive(options.maxPeers, 'max peers');
    positive(options.maxQueuedRecordsPerPeer, 'max queued records per peer');
    positive(options.maxQueuedBytesPerPeer, 'max queued bytes per peer');
    positive(options.maxIoBytesPerDrive, 'max I/O bytes per drive');
    if (!Number.isSafeInteger(options.maxPeers * 2)) {
        throw validation('TCP connection limit overflows');
    }
}
export class MonitoredFipsEndpoint {
    endpoint;
    datagrams = 0;
    rejected = 0;
    constructor(endpoint) {
        this.endpoint = endpoint;
    }
    registerService(port, handler) {
        return this.endpoint.registerService(port, async (context) => {
            this.datagrams = saturatingAdd(this.datagrams, 1);
            try {
                await handler(context);
            }
            catch {
                // Match Rust receive_report: isolate malformed or over-capacity segments.
                this.rejected = saturatingAdd(this.rejected, 1);
            }
        });
    }
    sendDatagram(args) {
        return this.endpoint.sendDatagram(args);
    }
    drainCounters() {
        const counters = {
            fipsDatagrams: this.datagrams,
            rejectedTcpSegments: this.rejected,
        };
        this.datagrams = 0;
        this.rejected = 0;
        return counters;
    }
}
function positive(value, name) {
    if (!Number.isSafeInteger(value) || value <= 0) {
        throw validation(`${name} must be greater than zero`);
    }
}
function unsigned(value, maximum, name, allowZero = true) {
    if (!Number.isSafeInteger(value) || value < (allowZero ? 0 : 1) || value > maximum) {
        throw validation(`${name} is out of range`);
    }
}
function validation(message) {
    return PubsubError.validation(message);
}
//# sourceMappingURL=fips-invwant-tcp-types.js.map