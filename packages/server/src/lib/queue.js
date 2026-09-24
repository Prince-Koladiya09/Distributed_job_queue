import { v4 as uuidv4 } from 'uuid';
import {
  queueKey,
  DELAYED_KEY,
  JOB_DATA_KEY,
  EVENTS_CHANNEL,
  idempotencyKey as idemKey,
} from '@jobqueue/shared';
import { redis } from '../redis.js';
import { pool } from '../db.js';
import { config } from '../config.js';

/**
 * FR-1 / FR-2: validate, dedupe, persist (queued) to Postgres BEFORE
 * pushing a reference onto the Redis queue, then push.
 *
 * Postgres is the source of truth for history (2.5); Redis is the
 * source of truth for live queue state. We write Postgres first so
 * that even if the Redis push fails, the job is not lost -- it is
 * recoverable, though for v1 recovery of that narrow gap is a manual
 * "requeue" operator action. See README "Known gaps".
 */
export async function enqueueJob({
  type,
  payload = {},
  priority = 'normal',
  delay = 0,
  maxRetries,
  idempotencyKey,
}) {
  maxRetries = maxRetries ?? config.maxRetriesDefault;

  // FR-1.3: idempotency dedup within TTL window.
  if (idempotencyKey) {
    const existingId = await redis.get(idemKey(idempotencyKey));
    if (existingId) {
      const existing = await getJobById(existingId);
      if (existing) {
        return { job: existing, deduped: true };
      }
    }
  }

  const id = uuidv4();
  const now = new Date();
  const scheduledFor = delay > 0 ? new Date(now.getTime() + delay) : null;
  const status = scheduledFor ? 'delayed' : 'queued';

  const { rows } = await pool.query(
    `INSERT INTO jobs (id, type, payload, priority, status, attempt, max_retries,
                        idempotency_key, scheduled_for, created_at, updated_at)
     VALUES ($1,$2,$3,$4,$5,0,$6,$7,$8,$9,$9)
     RETURNING *`,
    [id, type, payload, priority, status, maxRetries, idempotencyKey || null, scheduledFor, now]
  );
  const job = rows[0];

  await logEvent(id, status, 'Job created');

  if (idempotencyKey) {
    await redis.set(idemKey(idempotencyKey), id, 'EX', config.idempotencyTtlSeconds);
  }

  const envelope = toEnvelope(job);
  await redis.hset(JOB_DATA_KEY, id, JSON.stringify(envelope));

  if (scheduledFor) {
    // FR-2.2 delayed jobs: sorted set scored by execution timestamp.
    await redis.zadd(DELAYED_KEY, scheduledFor.getTime(), id);
  } else {
    // FR-2.1: 3 priority levels, distinct Redis lists.
    await redis.lpush(queueKey(priority), id);
  }

  await publishEvent({ jobId: id, status, message: 'Job created' });

  return { job, deduped: false };
}

export function toEnvelope(job) {
  return {
    id: job.id,
    type: job.type,
    payload: job.payload,
    priority: job.priority,
    attempt: job.attempt,
    maxRetries: job.max_retries,
  };
}

export async function getJobById(id) {
  const { rows } = await pool.query('SELECT * FROM jobs WHERE id = $1', [id]);
  return rows[0] || null;
}

export async function getJobEvents(id) {
  const { rows } = await pool.query(
    'SELECT * FROM job_events WHERE job_id = $1 ORDER BY created_at ASC',
    [id]
  );
  return rows;
}

export async function listJobs({ status, type, page = 1, pageSize = 25 }) {
  const conditions = [];
  const params = [];
  if (status) {
    params.push(status);
    conditions.push(`status = $${params.length}`);
  }
  if (type) {
    params.push(type);
    conditions.push(`type = $${params.length}`);
  }
  const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
  const offset = (Math.max(1, page) - 1) * pageSize;

  params.push(pageSize, offset);
  const { rows } = await pool.query(
    `SELECT * FROM jobs ${where} ORDER BY created_at DESC LIMIT $${params.length - 1} OFFSET $${params.length}`,
    params
  );
  const countParams = params.slice(0, params.length - 2);
  const { rows: countRows } = await pool.query(
    `SELECT COUNT(*)::int AS count FROM jobs ${where}`,
    countParams
  );
  return { jobs: rows, total: countRows[0].count, page, pageSize };
}

/** FR-4.3: manually re-queue a DLQ/failed job. */
export async function retryJob(id) {
  const job = await getJobById(id);
  if (!job) return null;
  if (!['dead', 'failed'].includes(job.status)) {
    const err = new Error(`Job ${id} is not in a retryable state (status=${job.status})`);
    err.code = 'INVALID_STATE';
    throw err;
  }

  const { rows } = await pool.query(
    `UPDATE jobs SET status = 'queued', error = NULL, worker_id = NULL
     WHERE id = $1 RETURNING *`,
    [id]
  );
  const updated = rows[0];
  await logEvent(id, 'queued', 'Manually re-queued by operator');

  const envelope = toEnvelope(updated);
  await redis.hset(JOB_DATA_KEY, id, JSON.stringify(envelope));
  await redis.lpush(queueKey(updated.priority), id);
  await publishEvent({ jobId: id, status: 'queued', message: 'Manually re-queued by operator' });

  return updated;
}

/** DELETE /jobs/:id -- cancel a queued (not-yet-started) job. */
export async function cancelJob(id) {
  const job = await getJobById(id);
  if (!job) return null;
  if (!['queued', 'delayed'].includes(job.status)) {
    const err = new Error(`Job ${id} cannot be cancelled from status=${job.status}`);
    err.code = 'INVALID_STATE';
    throw err;
  }

  await redis.lrem(queueKey(job.priority), 0, id);
  await redis.zrem(DELAYED_KEY, id);
  await redis.hdel(JOB_DATA_KEY, id);

  const { rows } = await pool.query(
    `UPDATE jobs SET status = 'failed', error = 'Cancelled by operator' WHERE id = $1 RETURNING *`,
    [id]
  );
  await logEvent(id, 'failed', 'Cancelled by operator');
  await publishEvent({ jobId: id, status: 'failed', message: 'Cancelled by operator' });
  return rows[0];
}

export async function logEvent(jobId, status, message) {
  await pool.query(
    'INSERT INTO job_events (job_id, status, message) VALUES ($1,$2,$3)',
    [jobId, status, message || null]
  );
}

export async function publishEvent(event) {
  await redis.publish(EVENTS_CHANNEL, JSON.stringify({ ...event, ts: new Date().toISOString() }));
}

/**
 * FR-2.2 promotion: move delayed jobs whose scheduled_for time has
 * passed from the ZSET into their priority list. Safe to call
 * concurrently -- ZRANGEBYSCORE + ZREM per id is idempotent (a second
 * caller's ZREM on an already-removed member is a no-op), so at worst
 * two callers both find a job in the read step but only one succeeds
 * in removing (and thus pushing) it.
 */
export async function promoteDueDelayedJobs(limit = 100) {
  const now = Date.now();
  const dueIds = await redis.zrangebyscore(DELAYED_KEY, 0, now, 'LIMIT', 0, limit);
  let promoted = 0;
  for (const id of dueIds) {
    const removed = await redis.zrem(DELAYED_KEY, id);
    if (!removed) continue; // another promoter beat us to it
    const raw = await redis.hget(JOB_DATA_KEY, id);
    if (!raw) continue;
    const envelope = JSON.parse(raw);
    await redis.lpush(queueKey(envelope.priority), id);
    await pool.query(`UPDATE jobs SET status = 'queued' WHERE id = $1`, [id]);
    await logEvent(id, 'queued', 'Delayed job promoted to active queue');
    await publishEvent({ jobId: id, status: 'queued', message: 'Delayed job promoted' });
    promoted += 1;
  }
  return promoted;
}

/** FR-6.4 / GET /queues/stats support. */
export async function getQueueStats() {
  const [high, normal, low, delayed] = await Promise.all([
    redis.llen(queueKey('high')),
    redis.llen(queueKey('normal')),
    redis.llen(queueKey('low')),
    redis.zcard(DELAYED_KEY),
  ]);

  const { rows: statusCounts } = await pool.query(
    `SELECT status, COUNT(*)::int AS count FROM jobs GROUP BY status`
  );
  const byStatus = Object.fromEntries(statusCounts.map((r) => [r.status, r.count]));

  const { rows: throughputRows } = await pool.query(
    `SELECT COUNT(*)::int AS count FROM jobs
     WHERE status = 'completed' AND completed_at > now() - interval '1 minute'`
  );

  const { rows: failRows } = await pool.query(
    `SELECT
        COUNT(*) FILTER (WHERE status IN ('failed','dead'))::int AS failed,
        COUNT(*)::int AS total
     FROM jobs WHERE created_at > now() - interval '1 hour'`
  );
  const failureRate = failRows[0].total > 0 ? failRows[0].failed / failRows[0].total : 0;

  return {
    queueDepth: { high, normal, low, delayed },
    byStatus,
    throughputPerMin: throughputRows[0].count,
    failureRateLastHour: Number(failureRate.toFixed(4)),
    dlqCount: byStatus.dead || 0,
  };
}
