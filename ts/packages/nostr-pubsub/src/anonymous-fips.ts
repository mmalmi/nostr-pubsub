import { discoveryScope } from './anonymous-relay-discovery.js';
import { FipsNostrPubsubClient } from './fips-pubsub-client.js';
import { FipsNostrPubsubEventSource } from './fips-pubsub-event-source.js';
import type { FipsPubsubClientNode } from './fips-pubsub-client-types.js';
import type { FipsReceiptOptions } from './fips-publication-receipts.js';
import { SOURCE_PRIORITY_FIPS_ENDPOINT } from './source.js';
import type { NostrEvent, NostrFilter, QueryOptions } from './types.js';
import type { NostrEventSubscription, QueryEvent, QueryReport } from './event-bus.js';

function rememberEndpoint(used: Set<string>, id: string): void {
  if (used.has(id)) throw new Error('Anonymous endpoint was reused');
  used.add(id);
  if (used.size > 4096) used.delete(used.values().next().value!);
}

/** Fresh random endpoint, with explicit approved public providers only. Never
 * return the account/live receiver endpoint, contacts, or discovered phone peers. */
export interface AnonymousFipsEndpoint {
  node: FipsPubsubClientNode;
  localPeerId: string;
  peers(): readonly string[];
  close(): Promise<void>;
}
export interface AnonymousFipsOptions {
  createEndpoint(filters?: NostrFilter[]): Promise<AnonymousFipsEndpoint>;
  timeoutMs?: number;
  id?: string;
}

/** Remote-acknowledged RuntimeSource publisher. Every publish/copy/retry gets
 * a new endpoint. An old, rejecting or unreachable provider leaves queues pending. */
export function createAnonymousFipsPublisher(options: AnonymousFipsOptions) {
  const used = new Set<string>();
  return {
    id: options.id ?? 'fips-private-send',
    publishAcceptance: 'remote' as const,
    async publish(event: NostrEvent, _source?: unknown, receiptOptions: FipsReceiptOptions = {}) {
      receiptOptions.signal?.throwIfAborted();
      const endpoint = await options.createEndpoint();
      let client: FipsNostrPubsubClient | undefined;
      try {
        rememberEndpoint(used, endpoint.localPeerId);
        client = new FipsNostrPubsubClient({ ...endpoint, role: 'publisher', peers: () => endpoint.peers() }).start();
        const receipt = await client.publishWithReceipt(event, { timeoutMs: options.timeoutMs, ...receiptOptions });
        return { accepted: true, remoteAccepted: true, priority: SOURCE_PRIORITY_FIPS_ENDPOINT, reason: `accepted by ${receipt.provider}` };
      } finally { try { await client?.stop(); } finally { await endpoint.close(); } }
    },
  };
}

/** One public author/device scope per fresh endpoint. A lookup never reuses
 * an identified receiver or another subject's transport. */
export function createAnonymousFipsDiscovery(options: AnonymousFipsOptions) {
  const used = new Set<string>();
  const open = async (filters: NostrFilter[]) => {
    discoveryScope(filters);
    const endpoint = await options.createEndpoint(filters);
    try {
      rememberEndpoint(used, endpoint.localPeerId);
      const client = new FipsNostrPubsubClient({ ...endpoint, role: { discovery: filters }, peers: () => endpoint.peers() }).start();
      const source = new FipsNostrPubsubEventSource(client, options.timeoutMs ?? 2_000);
      return { client, source, close: async () => { try { await client.stop(); } finally { await endpoint.close(); } } };
    } catch (error) { await endpoint.close(); throw error; }
  };
  return {
    id: options.id ?? 'fips-anonymous-discovery',
    async query(filters: NostrFilter[], queryOptions: QueryOptions = {}): Promise<QueryReport> {
      queryOptions.signal?.throwIfAborted();
      const operation = await open(filters);
      try { return await operation.source.query(filters, queryOptions); }
      finally { await operation.close(); }
    },
    async subscribe(filters: NostrFilter[], handler: (event: QueryEvent) => void): Promise<NostrEventSubscription> {
      const operation = await open(filters);
      let subscription: NostrEventSubscription;
      try { subscription = await operation.source.subscribe(filters, handler); }
      catch (error) { await operation.close(); throw error; }
      let closed = false;
      return { close: () => { if (closed) return; closed = true; subscription.close(); void operation.close().catch(() => undefined); } };
    },
  };
}
