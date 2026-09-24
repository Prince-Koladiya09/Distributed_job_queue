/**
 * Central definition of every Redis key/naming pattern used across the
 * system, so the API server, workers, scheduler and reaper never drift
 * out of sync with each other (SRS 5.3 Queue Design Detail).
 */

export const PRIORITIES = ['high', 'normal', 'low'];

/** FIFO list per priority level. Producers LPUSH, workers RPOP/LMOVE. */
export function queueKey(priority) {
  if (!PRIORITIES.includes(priority)) {
    throw new Error(`Unknown priority "${priority}"`);
  }
  return `queue:${priority}`;
}

/** Sorted set of delayed jobs, scored by epoch-ms execution time. */
export const DELAYED_KEY = 'queue:delayed';

/** Per-worker "in flight" list. Jobs LMOVE'd here while being processed. */
export function processingKey(workerId) {
  return `processing:${workerId}`;
}

/** Hash of jobId -> last heartbeat epoch-ms, used by the reaper. */
export const HEARTBEATS_KEY = 'jobs:heartbeats';

/** Hash of jobId -> the raw job JSON envelope currently queued/processing. */
export const JOB_DATA_KEY = 'jobs:data';

/** Pub/Sub channel workers/API publish state-change events to. */
export const EVENTS_CHANNEL = 'jobs:events';

/** Idempotency dedup key -> existing jobId, with a TTL. */
export function idempotencyKey(key) {
  return `idempotency:${key}`;
}

/** Distributed lock keys (SET NX PX), section 5.3. */
export const SCHEDULER_LOCK_KEY = 'lock:scheduler';
export function reaperLockKey(workerId) {
  return `lock:reaper:${workerId}`;
}

/** Registry of known worker processing-list keys, so the reaper can scan
 * all of them without doing a (blocking, O(N)) KEYS scan in production. */
export const WORKERS_SET_KEY = 'workers:active';

/** Rolling counters for dashboard throughput (jobs/min), one per minute
 * bucket, expired automatically. */
export function throughputBucketKey(epochMinute) {
  return `metrics:throughput:${epochMinute}`;
}
