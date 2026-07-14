import type { Event, VerifiedEvent } from 'nostr-tools/core';
import type { Filter } from 'nostr-tools/filter';
import { verifiedSymbol, verifyEvent } from 'nostr-tools/pure';

const verifiedEventCopies = new WeakSet<NostrVerifiedEvent>();

export type NostrEvent = Event;
export type NostrFilter = Filter;
export type NostrVerifiedEvent = VerifiedEvent;
export type SourceId = string;

export interface QueryOptions {
  limit?: number;
}

export class PubsubError extends Error {
  readonly kind: 'validation' | 'storage';

  private constructor(kind: 'validation' | 'storage', message: string) {
    super(message);
    this.name = 'PubsubError';
    this.kind = kind;
  }

  static validation(message: string): PubsubError {
    return new PubsubError('validation', message);
  }

  static storage(message: string): PubsubError {
    return new PubsubError('storage', message);
  }
}

export function verifyNostrEvent(event: NostrEvent): NostrVerifiedEvent {
  const candidate = cloneNostrEvent(event);
  if (!verifyEvent(candidate)) {
    throw PubsubError.validation('invalid Nostr event id or signature');
  }
  return freezeVerifiedEvent(candidate);
}

/** Defensive immutable copy for an event already checked at a trust boundary. */
export function copyVerifiedNostrEvent(event: NostrVerifiedEvent): NostrVerifiedEvent {
  if (!verifiedEventCopies.has(event)) {
    throw PubsubError.validation('verified mesh paths require verifyNostrEvent output');
  }
  const candidate = cloneNostrEvent(event) as NostrVerifiedEvent;
  candidate[verifiedSymbol] = true;
  return freezeVerifiedEvent(candidate);
}

function cloneNostrEvent(event: NostrEvent): NostrEvent {
  try {
    if (
      !Array.isArray(event.tags) ||
      event.tags.some(
        (tag) => !Array.isArray(tag) || tag.some((item) => typeof item !== 'string'),
      )
    ) {
      throw new TypeError('invalid tags');
    }
    return {
      id: event.id,
      pubkey: event.pubkey,
      created_at: event.created_at,
      kind: event.kind,
      tags: event.tags.map((tag) => [...tag]),
      content: event.content,
      sig: event.sig,
    };
  } catch {
    throw PubsubError.validation('invalid Nostr event structure');
  }
}

function freezeVerifiedEvent(candidate: NostrVerifiedEvent): NostrVerifiedEvent {
  if (
    !Number.isSafeInteger(candidate.created_at) ||
    candidate.created_at < 0 ||
    !Number.isSafeInteger(candidate.kind) ||
    candidate.kind < 0 ||
    candidate.kind > 65_535
  ) {
    throw PubsubError.validation('invalid Nostr event timestamp or kind');
  }

  for (const tag of candidate.tags) Object.freeze(tag);
  Object.freeze(candidate.tags);
  verifiedEventCopies.add(candidate);
  return Object.freeze(candidate);
}
