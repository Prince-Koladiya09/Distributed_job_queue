import { describe, it, expect, beforeEach } from 'vitest';
import { claimNextJob } from '../packages/worker/src/claim.js';

/**
 * A minimal in-memory fake of the subset of the ioredis API that
 * claim.js uses, so we can unit-test priority ordering and atomic
 * claim semantics without a real Redis instance (a full round-trip
 * against real Redis/Postgres is covered by the Testcontainers-based
 * integration suite described in SRS 9.2 / README "Testing").
 */
function createFakeRedis() {
  const lists = new Map(); // key -> array (front = index 0 = "left")
  const hashes = new Map(); // key -> Map

  function getList(key) {
    if (!lists.has(key)) lists.set(key, []);
    return lists.get(key);
  }
  function getHash(key) {
    if (!hashes.has(key)) hashes.set(key, new Map());
    return hashes.get(key);
  }

  return {
    _lists: lists,
    async lpush(key, val) {
      getList(key).unshift(val);
    },
    async lmove(src, dst, fromSide, toSide) {
      const s = getList(src);
      if (s.length === 0) return null;
      const val = fromSide === 'RIGHT' ? s.pop() : s.shift();
      const d = getList(dst);
      if (toSide === 'LEFT') d.unshift(val);
      else d.push(val);
      return val;
    },
    async blmove(src, dst, fromSide, toSide /*, timeoutSeconds */) {
      // For tests, behave like a non-blocking lmove (timeout not simulated).
      return this.lmove(src, dst, fromSide, toSide);
    },
    async hset(key, field, val) {
      getHash(key).set(field, val);
    },
    async hget(key, field) {
      return getHash(key).get(field) ?? null;
    },
  };
}

describe('claimNextJob priority ordering', () => {
  let redis;
  beforeEach(() => {
    redis = createFakeRedis();
  });

  it('claims from the high-priority queue before normal or low', async () => {
    await redis.lpush('queue:low', 'job-low');
    await redis.lpush('queue:normal', 'job-normal');
    await redis.lpush('queue:high', 'job-high');

    const claimed = await claimNextJob(redis, 'worker-1', { blockSeconds: 0 });
    expect(claimed.id).toBe('job-high');
    expect(claimed.priority).toBe('high');
  });

  it('falls back to normal when high is empty', async () => {
    await redis.lpush('queue:low', 'job-low');
    await redis.lpush('queue:normal', 'job-normal');

    const claimed = await claimNextJob(redis, 'worker-1', { blockSeconds: 0 });
    expect(claimed.id).toBe('job-normal');
  });

  it('moves the claimed job into the worker processing list (atomicity)', async () => {
    await redis.lpush('queue:high', 'job-1');
    await claimNextJob(redis, 'worker-1', { blockSeconds: 0 });
    expect(redis._lists.get('queue:high') || []).toEqual([]);
    expect(redis._lists.get('processing:worker-1')).toContain('job-1');
  });

  it('returns null when every queue is empty', async () => {
    const claimed = await claimNextJob(redis, 'worker-1', { blockSeconds: 0 });
    expect(claimed).toBeNull();
  });
});
