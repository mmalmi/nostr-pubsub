export function allowWithPriority(priority) {
    return { type: 'allow', priority };
}
export function throttle(priority, reason) {
    return { type: 'throttle', priority, reason };
}
export function drop(reason) {
    return { type: 'drop', reason };
}
export function decisionPriority(decision) {
    return decision.type === 'drop' ? 0 : decision.priority;
}
export function reportParts(decision) {
    switch (decision.type) {
        case 'allow':
            return { accepted: true, priority: decision.priority };
        case 'throttle':
            return { accepted: true, priority: decision.priority, reason: decision.reason };
        case 'drop':
            return { accepted: false, priority: 0, reason: decision.reason };
    }
}
//# sourceMappingURL=policy.js.map