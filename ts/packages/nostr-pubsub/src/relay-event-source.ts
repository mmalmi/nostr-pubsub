import type {
  NostrEventPublisher,
  NostrEventReader,
  NostrEventSubscriber,
  NostrEventSubscription,
  PublishReport,
  QueryEvent,
  QueryReport,
} from './event-bus.js';
import {
  SOURCE_PRIORITY_RELAY,
  relaySource,
  type EventSource,
} from './source.js';
import {
  validateQueryOptions,
  verifyNostrEvent,
  type NostrEvent,
  type NostrFilter,
  type NostrVerifiedEvent,
  type QueryOptions,
} from './types.js';

export interface NostrRelaySubscription {
  close(reason?: string): void;
}

export interface NostrRelayTransportHandlers {
  onEvent(event: NostrEvent): void;
  /** Called after a terminal subscription close. */
  onClose?(reasons?: readonly string[]): void;
}

export interface NostrRelayTransportSubscribeOptions {
  /** Historical queries may close on aggregate EOSE or a transport-owned bound. */
  closeOnEose?: boolean;
}

export interface NostrRelayTransport {
  subscribe(
    filters: NostrFilter[],
    handlers: NostrRelayTransportHandlers,
    options?: NostrRelayTransportSubscribeOptions,
  ): NostrRelaySubscription;
  publish(event: NostrVerifiedEvent): Promise<void> | void;
}

/** Traditional Nostr relay adapter for the shared reader/publisher/live router. */
export class NostrRelayEventSource
implements NostrEventReader, NostrEventPublisher, NostrEventSubscriber {
  readonly source: EventSource;

  constructor(readonly url: string, private readonly transport: NostrRelayTransport) {
    this.source = relaySource(url);
  }

  async publish(event: NostrEvent, _source?: EventSource): Promise<PublishReport> {
    const verified = verifyNostrEvent(event);
    await this.transport.publish(verified);
    return { accepted: true, priority: SOURCE_PRIORITY_RELAY };
  }

  subscribe(filters: NostrFilter[], handler: (event: QueryEvent) => void): NostrEventSubscription {
    const subscription = this.transport.subscribe(filters, {
      onEvent: (event) => {
        try {
          handler({
            event: verifyNostrEvent(event),
            source: this.source,
            priority: SOURCE_PRIORITY_RELAY,
          });
        } catch {
          // Invalid relay events never cross the reader boundary.
        }
      },
    });
    return { close: () => subscription.close('nostr-pubsub subscription closed') };
  }

  query(filters: NostrFilter[], options: QueryOptions = {}): Promise<QueryReport> {
    validateQueryOptions(options);
    if (options.signal?.aborted) return Promise.reject(abortError(options.signal.reason));
    if (options.deadline !== undefined && Date.now() >= options.deadline) {
      return Promise.reject(new DOMException('Nostr relay query deadline exceeded', 'TimeoutError'));
    }
    return new Promise((resolve, reject) => {
      const events = new Map<string, QueryEvent>();
      let settled = false;
      let closeOnAssign = false;
      let timer: ReturnType<typeof setTimeout> | undefined;
      let subscription: NostrRelaySubscription | undefined;
      const finish = (error?: unknown): void => {
        if (settled) return;
        settled = true;
        if (timer !== undefined) clearTimeout(timer);
        options.signal?.removeEventListener('abort', cancel);
        if (subscription === undefined) closeOnAssign = true;
        else subscription.close('nostr-pubsub query complete');
        if (error !== undefined) reject(error);
        else resolve({ events: ordered(events.values(), options.limit) });
      };
      const cancel = (): void => finish(abortError(options.signal?.reason));
      options.signal?.addEventListener('abort', cancel, { once: true });
      timer = options.deadline === undefined
        ? undefined
        : setTimeout(
          () => finish(new DOMException('Nostr relay query deadline exceeded', 'TimeoutError')),
          Math.max(0, options.deadline - Date.now()),
        );
      subscription = this.transport.subscribe(filters, {
        onEvent: (event) => {
          try {
            const verified = verifyNostrEvent(event);
            if (!events.has(verified.id)) {
              events.set(verified.id, {
                event: verified,
                source: this.source,
                priority: SOURCE_PRIORITY_RELAY,
              });
            }
            if (options.limit !== undefined && events.size >= options.limit) finish();
          } catch {
            // Invalid relay events never enter a query result.
          }
        },
        onClose: () => finish(),
      }, { closeOnEose: true });
      if (closeOnAssign) subscription.close('nostr-pubsub query complete');
    });
  }
}

function ordered(events: Iterable<QueryEvent>, limit: number | undefined): QueryEvent[] {
  const sorted = [...events].sort((left, right) =>
    right.event.created_at - left.event.created_at ||
    (left.event.id < right.event.id ? -1 : left.event.id > right.event.id ? 1 : 0));
  return limit === undefined ? sorted : sorted.slice(0, limit);
}

function abortError(reason: unknown): DOMException {
  return new DOMException(
    typeof reason === 'string' ? reason : 'Nostr relay query cancelled',
    'AbortError',
  );
}
