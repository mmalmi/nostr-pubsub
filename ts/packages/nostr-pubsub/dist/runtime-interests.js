/** Candidate dispatch index only; the caller still applies every original filter. */
export class RuntimeInterests {
    buckets = new Map();
    memberships = new Map();
    add(value, filters) {
        const keys = new Set(filters.flatMap(candidateKeys));
        this.memberships.set(value, keys);
        for (const key of keys) {
            const bucket = this.buckets.get(key) ?? new Set();
            bucket.add(value);
            this.buckets.set(key, bucket);
        }
    }
    remove(value) {
        for (const key of this.memberships.get(value) ?? []) {
            const bucket = this.buckets.get(key);
            bucket.delete(value);
            if (!bucket.size)
                this.buckets.delete(key);
        }
        this.memberships.delete(value);
    }
    candidates(event) {
        const keys = ['*', `id:${event.id}`, `author:${event.pubkey}`, `kind:${event.kind}`];
        for (const tag of event.tags)
            if (tag[0] && tag[1])
                keys.push(`tag:${tag[0]}:${tag[1]}`);
        const result = new Set();
        for (const key of keys)
            for (const value of this.buckets.get(key) ?? [])
                result.add(value);
        return result;
    }
}
function candidateKeys(filter) {
    if (filter.ids?.length && filter.ids.every((id) => id.length === 64))
        return filter.ids.map((id) => `id:${id}`);
    const tagged = Object.entries(filter).filter(([key, value]) => key.startsWith('#') && Array.isArray(value) && value.length)
        .sort(([, a], [, b]) => a.length - b.length)[0];
    if (tagged)
        return tagged[1].map((value) => `tag:${tagged[0].slice(1)}:${value}`);
    if (filter.authors?.length && filter.authors.every((author) => author.length === 64))
        return filter.authors.map((author) => `author:${author}`);
    if (filter.kinds?.length)
        return filter.kinds.map((kind) => `kind:${kind}`);
    return ['*'];
}
//# sourceMappingURL=runtime-interests.js.map