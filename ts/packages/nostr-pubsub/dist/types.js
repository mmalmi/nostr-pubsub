export class PubsubError extends Error {
    kind;
    constructor(kind, message) {
        super(message);
        this.name = 'PubsubError';
        this.kind = kind;
    }
    static validation(message) {
        return new PubsubError('validation', message);
    }
    static storage(message) {
        return new PubsubError('storage', message);
    }
}
//# sourceMappingURL=types.js.map