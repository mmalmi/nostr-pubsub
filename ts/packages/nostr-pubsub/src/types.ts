import type { Event, VerifiedEvent } from 'nostr-tools/core';
import type { Filter } from 'nostr-tools/filter';
import { verifiedSymbol, verifyEvent } from 'nostr-tools/pure';

const verifiedEventCopies = new WeakSet<NostrVerifiedEvent>();
const NOSTR_HEX_32_BYTES = /^[0-9a-f]{64}$/;
const NOSTR_HEX_64_BYTES = /^[0-9a-f]{128}$/;

export type NostrEvent = Event;
export type NostrFilter = Filter;
export type NostrVerifiedEvent = VerifiedEvent;
export type SourceId = string;

export interface NostrEventBatchVerificationOptions {
  signal?: AbortSignal;
}

export type NostrEventBatchVerifier = (
  events: readonly NostrEvent[],
  options: Readonly<NostrEventBatchVerificationOptions>,
) => PromiseLike<readonly boolean[]>;

export interface QueryOptions {
  limit?: number;
  /** Cancels work which is no longer useful to the caller. */
  signal?: AbortSignal;
  /** Absolute Unix timestamp in milliseconds shared by every queried source. */
  deadline?: number;
}

export function validateQueryOptions(options: QueryOptions): void {
  if (
    options.limit !== undefined &&
    (!Number.isSafeInteger(options.limit) || options.limit < 0)
  ) {
    throw new RangeError('Query limit must be a non-negative safe integer');
  }
  if (options.deadline !== undefined && !Number.isFinite(options.deadline)) {
    throw new RangeError('Query deadline must be a finite Unix timestamp in milliseconds');
  }
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
  // Adjacent protocol layers can receive a defensive object created by this
  // module (for example FIPS wire decode followed by kind-policy admission).
  // The private WeakSet cannot be spoofed through the public verified symbol.
  if (verifiedEventCopies.has(event as NostrVerifiedEvent)) {
    return copyVerifiedNostrEvent(event as NostrVerifiedEvent);
  }
  const candidate = cloneNostrEvent(event);
  if (!verifyEvent(candidate)) {
    throw PubsubError.validation('invalid Nostr event id or signature');
  }
  return freezeVerifiedEvent(candidate);
}

/**
 * Admit events checked by an asynchronous trust boundary such as a Web Worker.
 *
 * The verifier receives immutable defensive clones. No event is admitted unless
 * the verifier returns one `true` boolean per event.
 */
export async function verifyNostrEventsWith(
  events: readonly NostrEvent[],
  verifier: NostrEventBatchVerifier,
  options: NostrEventBatchVerificationOptions = {},
): Promise<NostrVerifiedEvent[]> {
  if (!Array.isArray(events) || typeof verifier !== 'function') {
    throw PubsubError.validation('invalid external Nostr event verifier input');
  }
  throwIfVerificationAborted(options.signal);

  const candidates = Object.freeze(events.map((event) =>
    freezeUnverifiedEvent(cloneNostrEvent(event))
  ));
  const verifierOptions = Object.freeze({ signal: options.signal });
  const result = await awaitExternalVerification(
    Promise.resolve(verifier(candidates, verifierOptions)),
    options.signal,
  );
  throwIfVerificationAborted(options.signal);

  if (
    !Array.isArray(result) ||
    result.length !== candidates.length ||
    result.some((valid) => typeof valid !== 'boolean')
  ) {
    throw PubsubError.validation('external Nostr event verifier returned malformed results');
  }
  const invalidIndex = result.findIndex((valid) => !valid);
  if (invalidIndex !== -1) {
    throw PubsubError.validation(
      `invalid Nostr event id or signature at batch index ${invalidIndex}`,
    );
  }

  return candidates.map((event) => {
    const candidate = cloneNostrEvent(event) as NostrVerifiedEvent;
    candidate[verifiedSymbol] = true;
    return freezeVerifiedEvent(candidate);
  });
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
    if (typeof event !== 'object' || event === null) throw new TypeError('invalid event');
    const id = event.id;
    const pubkey = event.pubkey;
    const createdAt = event.created_at;
    const kind = event.kind;
    const tags = event.tags;
    const content = event.content;
    const sig = event.sig;
    if (
      typeof id !== 'string' ||
      !NOSTR_HEX_32_BYTES.test(id) ||
      typeof pubkey !== 'string' ||
      !NOSTR_HEX_32_BYTES.test(pubkey) ||
      !Number.isSafeInteger(createdAt) ||
      createdAt < 0 ||
      !Number.isSafeInteger(kind) ||
      kind < 0 ||
      kind > 65_535 ||
      !Array.isArray(tags) ||
      tags.some(
        (tag) => !Array.isArray(tag) || tag.some((item) => typeof item !== 'string'),
      ) ||
      typeof content !== 'string' ||
      typeof sig !== 'string' ||
      !NOSTR_HEX_64_BYTES.test(sig)
    ) {
      throw new TypeError('invalid event');
    }
    return {
      id,
      pubkey,
      created_at: createdAt,
      kind,
      tags: tags.map((tag) => [...tag]),
      content,
      sig,
    };
  } catch {
    throw PubsubError.validation('invalid Nostr event structure');
  }
}

function freezeUnverifiedEvent(candidate: NostrEvent): NostrEvent {
  for (const tag of candidate.tags) Object.freeze(tag);
  Object.freeze(candidate.tags);
  return Object.freeze(candidate);
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

function awaitExternalVerification(
  verification: Promise<readonly boolean[]>,
  signal: AbortSignal | undefined,
): Promise<readonly boolean[]> {
  if (signal === undefined) return verification;
  return new Promise((resolve, reject) => {
    const abort = (): void => reject(verificationAbortError(signal.reason));
    signal.addEventListener('abort', abort, { once: true });
    if (signal.aborted) {
      signal.removeEventListener('abort', abort);
      abort();
      return;
    }
    verification.then(
      (result) => {
        signal.removeEventListener('abort', abort);
        resolve(result);
      },
      (error: unknown) => {
        signal.removeEventListener('abort', abort);
        reject(error);
      },
    );
  });
}

function throwIfVerificationAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted) throw verificationAbortError(signal.reason);
}

function verificationAbortError(reason: unknown): DOMException {
  return new DOMException(
    typeof reason === 'string' ? reason : 'Nostr event verification cancelled',
    'AbortError',
  );
}
