import { compare, key, Writer, type Bound } from './encoding.js';

export class Storage {
  readonly items: Bound[];
  private sums: Uint32Array;
  constructor(records: Bound[], since: bigint, until: bigint, maxRecords: number) {
    const unique = new Map<string, Bound>();
    for (const record of records) {
      if (record.timestamp < since || record.timestamp > until) continue;
      const id = key(record.id);
      if (unique.has(id) && unique.get(id)!.timestamp !== record.timestamp) throw new Error('conflicting timestamp');
      unique.set(id, { timestamp: record.timestamp, id: record.id.slice() });
      if (unique.size > maxRecords) throw new Error('reconciliation window exceeds record limit');
    }
    this.items = [...unique.values()].sort(compare);
    this.sums = new Uint32Array((this.items.length + 1) * 8);
    for (let i = 0; i < this.items.length; i++) {
      const id = new DataView(this.items[i].id.buffer);
      let carry = 0;
      for (let word = 0; word < 8; word++) {
        const sum = this.sums[i * 8 + word] + id.getUint32(word * 4, true) + carry;
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
  async fingerprint(begin: number, end: number): Promise<Uint8Array> {
    const sum = new Uint8Array(32);
    const view = new DataView(sum.buffer);
    let borrow = 0;
    for (let word = 0; word < 8; word++) {
      const difference = this.sums[end * 8 + word] - this.sums[begin * 8 + word] - borrow;
      view.setUint32(word * 4, difference >>> 0, true);
      borrow = difference < 0 ? 1 : 0;
    }
    const input = new Writer(); input.append(sum); input.varint(BigInt(end - begin));
    return new Uint8Array(await globalThis.crypto.subtle.digest('SHA-256', input.finish() as Uint8Array<ArrayBuffer>)).slice(0, 16);
  }
}
