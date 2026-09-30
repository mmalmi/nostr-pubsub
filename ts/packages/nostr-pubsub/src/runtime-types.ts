import type { NostrEvent, NostrFilter, QueryOptions, NostrEventVerifier } from './types.js';
import type { NostrEventPublisher, NostrEventReader, NostrEventSubscriber } from './event-bus.js';

export type RuntimeCacheMode = 'cache-first' | 'network-only' | 'cache-only';
export interface RuntimeOutboxEntry { event: NostrEvent; attempts: number; updatedAt: number; relays?: readonly string[]; sources?: readonly string[]; }
/** Storage only; the runtime owns signature admission and event replacement/deletion rules. */
export interface RuntimeEventStore {
  query(filters: NostrFilter[], options?: QueryOptions): Promise<NostrEvent[]>;
  put(event: NostrEvent): Promise<void>;
  delete(ids: string[]): Promise<void>;
  listPending(): Promise<RuntimeOutboxEntry[]>;
  putPending(entry: RuntimeOutboxEntry): Promise<void>;
  deletePending(id: string): Promise<void>;
  close?(): void | Promise<void>;
}
export interface RuntimeSource extends Partial<NostrEventReader>, Partial<NostrEventPublisher>, Partial<NostrEventSubscriber> {
  id: string;
  /** Set queued for mesh transports which accept sending without a remote acknowledgment. */
  publishAcceptance?: 'remote' | 'queued';
}
export interface RuntimeCompletion {
  complete: boolean;
  reason: 'eose' | 'cache' | 'timeout' | 'closed' | 'unavailable';
  sources: Array<{ id: string; complete: boolean; error?: string }>;
}
export interface RuntimeEventInfo { source: string; cached: boolean; }
export interface RuntimeSubscriptionHandlers {
  onEvent(event: NostrEvent, info: RuntimeEventInfo): void;
  /** Initial history status. Live subscriptions continue after this callback. */
  onEose?(status: RuntimeCompletion): void;
  onError?(error: Error): void;
}
export interface RuntimeSubscribeOptions {
  cache?: RuntimeCacheMode;
  /** Deliver older replaceable versions returned by the network without caching them.
   * Requires cache: 'network-only'; deletion and expiration checks still apply. */
  includeSuperseded?: boolean;
  /** Explicit relay scope also excludes additional sources unless sources is supplied. */
  relays?: readonly string[];
  sources?: readonly string[];
  signal?: AbortSignal;
  /** History bound only; never closes a live subscription. */
  deadline?: number;
}
export interface RuntimeSubscription { close(): void; }
export interface RuntimeQueryOptions extends QueryOptions, RuntimeSubscribeOptions {}
export interface RuntimePublishOptions {
  relays?: readonly string[]; sources?: readonly string[];
  /** Commit locally only after a positive remote acknowledgment. Implies queue:false and localEcho:false until accepted. */
  requireAck?: boolean;
  localEcho?: boolean;
  queue?: boolean;
}
export interface RuntimeQueryResult extends RuntimeCompletion { events: NostrEvent[]; }
export interface RuntimePublishResult {
  /** True only when at least one remote source accepted publication. */
  accepted: boolean;
  remoteAccepted: boolean;
  queued: boolean;
  sources: Array<{ id: string; accepted: boolean; pending?: boolean; queued?: boolean; error?: string }>;
}
export interface RuntimeRelayStats { url: string; connected: boolean; }
export interface RuntimeMetrics {
  subscriptions: number;
  relaySubscriptions: number;
  receivedEvents: number;
  deliveredEvents: number;
  duplicateEvents: number;
  verificationFailures: number;
}
export interface NostrRuntimeOptions {
  relays?: readonly string[];
  store?: RuntimeEventStore;
  sources?: readonly RuntimeSource[];
  websocketImplementation?: typeof WebSocket;
  verifyEvent?: NostrEventVerifier;
  signAuthEvent?: (relay: string, event: import('nostr-tools').EventTemplate) => Promise<NostrEvent>;
  batchWindowMs?: number;
  maxFiltersPerBatch?: number;
  maxFilterBytesPerBatch?: number;
  maxSubscriptions?: number;
  maxSeenEventsPerSubscription?: number;
  historyTimeoutMs?: number;
  publishTimeoutMs?: number;
  reconnectDelayMs?: number;
  onError?: (error: Error) => void;
}
