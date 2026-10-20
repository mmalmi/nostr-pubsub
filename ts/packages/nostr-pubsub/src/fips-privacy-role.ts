import { discoveryScope, isRelayDiscoveryFilter } from './anonymous-relay-discovery.js';
import { isPrivateRelayEvent, privateEnvelopeAllowed } from './private-relay-publisher.js';
import type { NostrEvent, NostrFilter } from './types.js';
import type { FipsPubsubWireMessage } from './wire.js';

export type FipsPrivacyRole = 'full' | 'receiver' | 'publisher' | { discovery: NostrFilter[] };

/** Guards every final serialized send, including retained cache and WANT replay. */
export class FipsPrivacyRolePolicy {
  private readonly scope?: string;
  constructor(readonly role: FipsPrivacyRole = 'full', private readonly publicAuthors: () => readonly string[] = () => []) {
    if (typeof role === 'object') this.scope = discoveryScope(role.discovery);
  }
  filters(filters: NostrFilter[]): void {
    if (this.role === 'full') return;
    if (this.role === 'publisher') throw new Error('Anonymous publisher cannot subscribe');
    if (typeof this.role === 'object') {
      if (discoveryScope(filters) !== this.scope) throw new Error('FIPS lookup crosses its public subject');
      return;
    }
    for (const filter of filters) {
      if (isRelayDiscoveryFilter(filter)) throw new Error('Public-author discovery requires an anonymous FIPS scope');
      if (filter.authors?.length && filter.kinds?.some(kind => !isPrivateRelayEvent({ kind })) && filter.authors.some(author => !this.publicAuthors().includes(author)))
        throw new Error('Authored public filter requires an explicit public service author');
      if (!filter.kinds?.length) throw new Error('Receiver filters require explicit event kinds');
      if (filter.kinds.some(kind => isPrivateRelayEvent({ kind })) && !filter.authors?.length && !filter['#p']?.length)
        throw new Error('Private receive filter requires an inbox or session key');
    }
  }
  canExport(event: NostrEvent): boolean {
    return this.role === 'full' || this.role === 'receiver' && !isPrivateRelayEvent(event) && this.publicAuthors().includes(event.pubkey);
  }
  publication(event: NostrEvent): void {
    if (!this.canExport(event)) throw new Error('Publication is forbidden on this FIPS role');
  }
  outbound(message: FipsPubsubWireMessage, inventoryEvent?: NostrEvent): void {
    if (this.role === 'full') return;
    if (message.type === 'req') { this.filters(message.filters); return; }
    if (message.type === 'close') return;
    if (this.role === 'publisher' && message.type === 'event' && message.subscriptionId === undefined && privateEnvelopeAllowed(message.event)) return;
    if (this.role !== 'publisher' && message.type === 'want') return;
    if (this.role === 'receiver' && ((message.type === 'event' && this.canExport(message.event))
      || (message.type === 'inv' && inventoryEvent?.id === message.eventId && inventoryEvent.kind === message.eventKind && this.canExport(inventoryEvent)))) return;
    throw new Error('Outbound frame is forbidden on this FIPS role');
  }
  get ignoresRequests(): boolean { return this.role !== 'full' && this.role !== 'receiver'; }
  assertPublisher(event: NostrEvent): void {
    if (this.role !== 'publisher' || !privateEnvelopeAllowed(event)) throw new Error('Requires an anonymous private publication role');
  }
}
