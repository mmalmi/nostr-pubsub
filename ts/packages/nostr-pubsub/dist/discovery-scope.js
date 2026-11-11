const authorLookupKinds = new Set([0, 1059, 10002, 10050, 30078, 37368, 37195]);
/** Identify targeted public lookups before validating their scope. */
export function isRelayDiscoveryFilter(filter) {
    return !!filter.kinds?.some(kind => kind === 37368 || kind === 30078
        || authorLookupKinds.has(kind) && (filter.authors !== undefined || kind !== 1059 && filter['#p'] !== undefined));
}
/** Preserve OR semantics while keeping every lookup key in a separate scope. */
export function splitRelayDiscoveryFilters(filters) {
    const receiver = [];
    const discovery = new Map();
    for (const filter of filters) {
        if (!isRelayDiscoveryFilter(filter)) {
            receiver.push(filter);
            continue;
        }
        const scopedKind = (kind) => authorLookupKinds.has(kind) && (kind !== 1059 || filter.authors !== undefined);
        const kinds = filter.kinds.filter(scopedKind);
        const ordinary = filter.kinds.filter(kind => !scopedKind(kind));
        const authors = filter.authors;
        const devices = filter['#p'];
        if (authors !== undefined && devices !== undefined)
            throw new Error('Public lookups require one key selector');
        const selector = authors !== undefined ? 'authors' : '#p';
        const keys = authors ?? devices;
        if (!keys?.length)
            throw new Error('Discovery requires a public key scope');
        for (const key of new Set(keys)) {
            const scoped = { ...filter, kinds, [selector]: [key] };
            const scope = discoveryScope([scoped]);
            const group = discovery.get(scope);
            if (group)
                group.push(scoped);
            else
                discovery.set(scope, [scoped]);
        }
        if (ordinary.length)
            receiver.push({ ...filter, kinds: ordinary });
    }
    return { receiver, discovery: [...discovery.values()] };
}
export function discoveryScope(filters) {
    if (!filters.length)
        throw new Error('Discovery requires a public key scope');
    const scopes = filters.map(filter => {
        if (!filter.kinds?.length || filter.kinds.some(kind => !authorLookupKinds.has(kind)))
            throw new Error('Discovery only reads public profiles, relay lists, devices and invites');
        if (filter.kinds.includes(37368) && filter['#type']?.includes('group_roster'))
            throw new Error('Private group membership cannot use public discovery');
        const author = filter.authors?.length === 1 && filter['#p'] === undefined ? filter.authors[0] : undefined;
        const device = filter.authors === undefined && filter.kinds.every(kind => kind === 37368)
            && filter['#p']?.length === 1 ? filter['#p'][0] : undefined;
        const key = author ?? device;
        if (!key || !/^[0-9a-f]{64}$/.test(key))
            throw new Error('Discovery requires exactly one public author or device');
        return `${author ? 'author' : 'device'}:${key}`;
    });
    if (new Set(scopes).size !== 1)
        throw new Error('Different public keys require separate discovery connections');
    return scopes[0];
}
//# sourceMappingURL=discovery-scope.js.map