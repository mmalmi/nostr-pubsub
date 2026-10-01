import { State, type ConnectionId } from '@fips/tcp';
import { describe, expect, it, vi } from 'vitest';
import { abortTcpConnectionIfPresent, closeTcpConnectionIfPresent } from '../src/fips-tcp-cleanup.js';

describe('TCP/FIPS cleanup', () => {
  it('treats a connection closed between state and abort as already cleaned up', async () => {
    const tcp = {
      state: vi.fn()
        .mockResolvedValueOnce(State.Established)
        .mockResolvedValueOnce(undefined),
      abort: vi.fn().mockRejectedValue(new Error('unknown connection')),
    };

    await expect(
      abortTcpConnectionIfPresent(tcp, 7 as ConnectionId),
    ).resolves.toBeUndefined();
    expect(tcp.abort).toHaveBeenCalledOnce();
  });

  it('preserves an abort failure while the connection remains retained', async () => {
    const tcp = {
      state: vi.fn().mockResolvedValue(State.Established),
      abort: vi.fn().mockRejectedValue(new Error('reset delivery failed')),
    };

    await expect(
      abortTcpConnectionIfPresent(tcp, 8 as ConnectionId),
    ).rejects.toThrow('reset delivery failed');
  });
  it('finishes a remote half-close released by a concurrent reset', async () => {
    const tcp = { state: vi.fn().mockResolvedValueOnce(State.CloseWait).mockResolvedValueOnce(undefined),
      close: vi.fn().mockRejectedValue(new Error('unknown connection')) };
    await expect(closeTcpConnectionIfPresent(tcp, 9 as ConnectionId, 1)).resolves.toBeUndefined();
    expect(tcp.close).toHaveBeenCalledOnce();
  });
  it('preserves close errors for retained connections', async () => {
    const tcp = { state: vi.fn().mockResolvedValue(State.CloseWait),
      close: vi.fn().mockRejectedValue(new Error('send failed')) };
    await expect(closeTcpConnectionIfPresent(tcp, 9 as ConnectionId, 1)).rejects.toThrow('send failed');
  });

});
