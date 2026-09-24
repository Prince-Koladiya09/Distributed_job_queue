import { queueKey, processingKey, JOB_DATA_KEY, HEARTBEATS_KEY, PRIORITIES } from '@jobqueue/shared';

/**
 * FR-3.1: Workers shall atomically claim a job such that no two
 * workers process the same job, using an atomic primitive (LMOVE).
 *
 * We poll high -> normal -> low (section 5.3) using a short blocking
 * BLMOVE on each list in turn so a worker doesn't busy-loop when all
 * queues are empty, while still respecting priority ordering (a
 * single BLMOVE across multiple keys isn't atomic-safe for priority
 * ordering in Redis, so we do bounded-timeout attempts per priority).
 */
export async function claimNextJob(redis, workerId, { blockSeconds = 1 } = {}) {
  for (const priority of PRIORITIES) {
    const isLast = priority === PRIORITIES[PRIORITIES.length - 1];
    // Non-blocking check on higher-priority queues first (so we never
    // block on "low" while a "high" job is waiting); only the final,
    // lowest-priority check blocks briefly to avoid busy-looping when
    // every queue is empty.
    const id = isLast
      ? await redis.blmove(queueKey(priority), processingKey(workerId), 'RIGHT', 'LEFT', blockSeconds)
      : await redis.lmove(queueKey(priority), processingKey(workerId), 'RIGHT', 'LEFT');
    if (id) {
      await redis.hset(HEARTBEATS_KEY, id, Date.now());
      const raw = await redis.hget(JOB_DATA_KEY, id);
      const envelope = raw ? JSON.parse(raw) : { id };
      return { id, priority, envelope };
    }
  }
  return null;
}

/** Remove a job from the worker's processing list once it's finished. */
export async function releaseJob(redis, workerId, jobId) {
  await redis.lrem(processingKey(workerId), 0, jobId);
  await redis.hdel(HEARTBEATS_KEY, jobId);
}

/** Refresh the heartbeat while a job is still executing (used by process.js). */
export async function heartbeat(redis, jobId) {
  await redis.hset(HEARTBEATS_KEY, jobId, Date.now());
}
