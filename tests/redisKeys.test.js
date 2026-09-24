import { describe, it, expect } from 'vitest';
import { queueKey, processingKey, idempotencyKey, PRIORITIES } from '../packages/shared/src/redisKeys.js';

describe('redisKeys', () => {
  it('exposes exactly 3 priority levels (FR-2.1)', () => {
    expect(PRIORITIES).toEqual(['high', 'normal', 'low']);
  });

  it('builds a distinct queue key per priority', () => {
    expect(queueKey('high')).toBe('queue:high');
    expect(queueKey('normal')).toBe('queue:normal');
    expect(queueKey('low')).toBe('queue:low');
  });

  it('rejects unknown priorities', () => {
    expect(() => queueKey('urgent')).toThrow();
  });

  it('builds a per-worker processing key', () => {
    expect(processingKey('worker-abc')).toBe('processing:worker-abc');
  });

  it('builds an idempotency key', () => {
    expect(idempotencyKey('welcome-email-user-123')).toBe('idempotency:welcome-email-user-123');
  });
});
