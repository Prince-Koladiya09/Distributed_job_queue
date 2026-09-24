import { describe, it, expect } from 'vitest';
import { computeBackoffMs, computeBackoffCeilingMs } from '../packages/shared/src/backoff.js';

describe('computeBackoffMs (FR-4.1: delay = base * 2^attempt, capped)', () => {
  it('computes the un-jittered ceiling correctly across attempts', () => {
    expect(computeBackoffCeilingMs(0, { baseMs: 1000, maxDelayMs: 300000 })).toBe(1000);
    expect(computeBackoffCeilingMs(1, { baseMs: 1000, maxDelayMs: 300000 })).toBe(2000);
    expect(computeBackoffCeilingMs(2, { baseMs: 1000, maxDelayMs: 300000 })).toBe(4000);
    expect(computeBackoffCeilingMs(3, { baseMs: 1000, maxDelayMs: 300000 })).toBe(8000);
  });

  it('caps the delay at maxDelayMs for large attempt counts', () => {
    const ceiling = computeBackoffCeilingMs(20, { baseMs: 1000, maxDelayMs: 300000 });
    expect(ceiling).toBe(300000);
  });

  it('jittered delay is within [0, ceiling]', () => {
    for (let attempt = 0; attempt < 6; attempt += 1) {
      const ceiling = computeBackoffCeilingMs(attempt, { baseMs: 1000, maxDelayMs: 300000 });
      for (let i = 0; i < 20; i += 1) {
        const val = computeBackoffMs(attempt, { baseMs: 1000, maxDelayMs: 300000, jitter: true });
        expect(val).toBeGreaterThanOrEqual(0);
        expect(val).toBeLessThanOrEqual(ceiling);
      }
    }
  });

  it('without jitter, returns exactly the ceiling', () => {
    expect(computeBackoffMs(2, { baseMs: 500, maxDelayMs: 300000, jitter: false })).toBe(2000);
  });

  it('throws for a negative attempt', () => {
    expect(() => computeBackoffMs(-1)).toThrow();
  });
});
