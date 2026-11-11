import { compare, compareIds, type Bound } from './encoding.js';

const K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
  0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
  0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);
const W = new Int32Array(64);
/** First 16 bytes of SHA-256 over one padded block (input < 56 bytes). */
function fingerprintHash(block: Uint8Array, length: number): Uint8Array {
  block.fill(0, length); block[length] = 0x80; block[62] = (length * 8) >>> 8; block[63] = (length * 8) & 255;
  for (let i = 0; i < 16; i++) W[i] = (block[i * 4] << 24) | (block[i * 4 + 1] << 16) | (block[i * 4 + 2] << 8) | block[i * 4 + 3];
  for (let i = 16; i < 64; i++) {
    const a = W[i - 15], b = W[i - 2];
    W[i] = (W[i - 16] + ((a >>> 7 | a << 25) ^ (a >>> 18 | a << 14) ^ (a >>> 3)) + W[i - 7] + ((b >>> 17 | b << 15) ^ (b >>> 19 | b << 13) ^ (b >>> 10))) | 0;
  }
  let a = 0x6a09e667, b = 0xbb67ae85 | 0, c = 0x3c6ef372, d = 0xa54ff53a | 0, e = 0x510e527f, f = 0x9b05688c | 0, g = 0x1f83d9ab, h = 0x5be0cd19;
  for (let i = 0; i < 64; i++) {
    const t1 = (h + ((e >>> 6 | e << 26) ^ (e >>> 11 | e << 21) ^ (e >>> 25 | e << 7)) + ((e & f) ^ (~e & g)) + K[i] + W[i]) | 0;
    const t2 = (((a >>> 2 | a << 30) ^ (a >>> 13 | a << 19) ^ (a >>> 22 | a << 10)) + ((a & b) ^ (a & c) ^ (b & c))) | 0;
    h = g; g = f; f = e; e = (d + t1) | 0; d = c; c = b; b = a; a = (t1 + t2) | 0;
  }
  const out = new Uint8Array(16), view = new DataView(out.buffer);
  view.setUint32(0, a + 0x6a09e667); view.setUint32(4, b + 0xbb67ae85); view.setUint32(8, c + 0x3c6ef372); view.setUint32(12, d + 0xa54ff53a);
  return out;
}

export class Storage {
  readonly items: Bound[];
  private sums: Uint32Array;
  private block = new Uint8Array(64);
  constructor(records: Bound[], since: bigint, until: bigint, maxRecords: number) {
    // Bucket by a per-snapshot salted hash of the whole ID, so crafted IDs
    // cannot pile into one bucket; full IDs settle collisions.
    const salt = globalThis.crypto.getRandomValues(new Int32Array(8));
    const buckets = new Map<number, Bound | Bound[]>();
    const items: Bound[] = [];
    for (const record of records) {
      if (record.timestamp < since || record.timestamp > until) continue;
      const id = record.id;
      let prefix = 0;
      for (let word = 0; word < 8; word++) {
        const o = word * 4;
        prefix = (prefix + Math.imul((id[o] << 24) | (id[o + 1] << 16) | (id[o + 2] << 8) | id[o + 3], salt[word] | 1)) | 0;
      }
      prefix ^= prefix >>> 15;
      const bucket = buckets.get(prefix);
      let seen: Bound | undefined;
      if (bucket === undefined) seen = undefined;
      else if (Array.isArray(bucket)) seen = bucket.find(item => compareIds(item.id, id) === 0);
      else if (compareIds(bucket.id, id) === 0) seen = bucket;
      if (seen) {
        if (seen.timestamp !== record.timestamp) throw new Error('conflicting timestamp');
        continue;
      }
      const item = { timestamp: record.timestamp, id: new Uint8Array(id) };
      if (bucket === undefined) buckets.set(prefix, item);
      else if (Array.isArray(bucket)) bucket.push(item);
      else buckets.set(prefix, [bucket, item]);
      items.push(item);
      if (items.length > maxRecords) throw new Error('reconciliation window exceeds record limit');
    }
    this.items = items.sort(compare);
    this.sums = new Uint32Array((this.items.length + 1) * 8);
    for (let i = 0; i < this.items.length; i++) {
      const id = this.items[i].id;
      let carry = 0;
      for (let word = 0; word < 8; word++) {
        const o = word * 4;
        const sum = this.sums[i * 8 + word] + ((id[o] | (id[o + 1] << 8) | (id[o + 2] << 16) | (id[o + 3] << 24)) >>> 0) + carry;
        this.sums[(i + 1) * 8 + word] = sum >>> 0;
        carry = sum > 0xffffffff ? 1 : 0;
      }
    }
  }
  lowerBound(first: number, bound: Bound): number {
    let last = this.items.length;
    while (first < last) {
      const middle = first + Math.floor((last - first) / 2);
      if (compare(this.items[middle], bound) < 0) first = middle + 1;
      else last = middle;
    }
    return first;
  }
  fingerprint(begin: number, end: number): Uint8Array {
    const block = this.block;
    let borrow = 0;
    for (let word = 0; word < 8; word++) {
      const difference = this.sums[end * 8 + word] - this.sums[begin * 8 + word] - borrow;
      const o = word * 4;
      block[o] = difference; block[o + 1] = difference >>> 8; block[o + 2] = difference >>> 16; block[o + 3] = difference >>> 24;
      borrow = difference < 0 ? 1 : 0;
    }
    // Counts stay below 2^28, so the big-endian base-128 varint is at most four bytes.
    const count = end - begin;
    let length = 32;
    if (count >= 1 << 21) block[length++] = (count >>> 21) | 128;
    if (count >= 1 << 14) block[length++] = ((count >>> 14) & 127) | 128;
    if (count >= 1 << 7) block[length++] = ((count >>> 7) & 127) | 128;
    block[length++] = count & 127;
    return fingerprintHash(block, length);
  }
}
