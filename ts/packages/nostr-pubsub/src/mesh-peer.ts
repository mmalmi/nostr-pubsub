import { PubsubError } from './types.js';

export interface MeshPeer {
  id: string;
  qualityScore?: number;
}

export function meshPeer(id: string, qualityScore?: number): MeshPeer {
  if (typeof id !== 'string') throw validation('mesh peer id must be a string');
  if (qualityScore === undefined) return { id };
  if (
    !Number.isInteger(qualityScore) ||
    qualityScore < -0x8000_0000 ||
    qualityScore > 0x7fff_ffff
  ) {
    throw validation(`mesh peer quality score must be a signed 32-bit integer: ${qualityScore}`);
  }
  return { id, qualityScore };
}

export function selectMeshPeers(
  peers: readonly MeshPeer[],
  excludedPeer: string | undefined,
  fanout: number,
  unknownPeerReserve: number,
): MeshPeer[] {
  const deduplicated = new Map<string, MeshPeer>();
  for (const candidate of peers) {
    const peer = meshPeer(candidate.id, candidate.qualityScore);
    if (peer.id === excludedPeer) continue;
    const existing = deduplicated.get(peer.id);
    if (existing === undefined || (isUnknown(existing) && !isUnknown(peer))) {
      deduplicated.set(peer.id, peer);
    }
  }

  const candidates = [...deduplicated.values()].sort(compareMeshPeers);
  const target = Math.min(boundedPositive(fanout), candidates.length);
  const unknownCount = candidates.filter(isUnknown).length;
  const requiredUnknown = Math.min(nonNegative(unknownPeerReserve), target, unknownCount);
  const selected = candidates.slice(0, target);
  const selectedIds = new Set(selected.map((peer) => peer.id));
  const replacements = candidates.filter((peer) => isUnknown(peer) && !selectedIds.has(peer.id));
  while (selected.filter(isUnknown).length < requiredUnknown) {
    const replacement = replacements.shift();
    const replaceIndex = selected.map(isUnknown).lastIndexOf(false);
    if (replacement === undefined || replaceIndex < 0) break;
    selected[replaceIndex] = replacement;
  }
  return selected;
}

function compareMeshPeers(left: MeshPeer, right: MeshPeer): number {
  const score = (right.qualityScore ?? 0) - (left.qualityScore ?? 0);
  if (score !== 0) return score;
  if (isUnknown(left) !== isUnknown(right)) return isUnknown(left) ? 1 : -1;
  return compareUtf8(left.id, right.id);
}

function compareUtf8(left: string, right: string): number {
  const encoder = new TextEncoder();
  const leftBytes = encoder.encode(left);
  const rightBytes = encoder.encode(right);
  const length = Math.min(leftBytes.length, rightBytes.length);
  for (let index = 0; index < length; index += 1) {
    if (leftBytes[index] !== rightBytes[index]) return leftBytes[index] - rightBytes[index];
  }
  return leftBytes.length - rightBytes.length;
}

function isUnknown(peer: MeshPeer): boolean {
  return peer.qualityScore === undefined;
}

function boundedPositive(value: number): number {
  if (!Number.isSafeInteger(value)) throw validation(`invalid positive integer ${value}`);
  return Math.max(1, value);
}

function nonNegative(value: number): number {
  if (!Number.isSafeInteger(value)) throw validation(`invalid non-negative integer ${value}`);
  return Math.max(0, value);
}

function validation(message: string): PubsubError {
  return PubsubError.validation(message);
}
