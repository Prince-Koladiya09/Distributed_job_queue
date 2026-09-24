import Redis from 'ioredis';
import pg from 'pg';
import { computeBackoffMs } from '@jobqueue/shared';
import {
  createLogger,
  queueKey,
  DELAYED_KEY,
  JOB_DATA_KEY,
  HEARTBEATS_KEY,
  EVENTS_CHANNEL,
  WORKERS_SET_KEY,
  processingKey,
  reaperLockKey,
} from '@jobqueue/shared';

const logger = createLogger('reaper');

const config = {
  pollIntervalMs: Number(process.env.REAPER_POLL_INTERVAL_MS || 5000),
  orphanTimeoutMs: Number(process.env.REAPER_ORPHAN_TIMEOUT_MS || 30000),
  backoffBaseMs: Number(process.env.BACKOFF_BASE_MS || 1000),
  backoffMaxMs: Number(process.env.BACKOFF_MAX_MS || 300000),
};

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://jobqueue:jobqueue@localhost:5432/jobqueue',
});

const LOCK_TTL_MS = config.pollIntervalMs * 4;

/**
 * SRS 5.4 Failure Recovery: "A background reaper process periodically
 * scans per-worker processing lists; any job whose heartbeat has
 * expired (worker crashed) is requeued automatically, incrementing its
 * retry count."
 *
 * Multiple reaper replicas may run for HA. Each worker's processing
 * list is claimed via a short-lived per-worker lock (5.3 "Locks") so
 * two reaper instances never requeue the same orphaned job twice.
 */
async function tick() {
  const workerIds = await redis.smembers(WORKERS_SET_KEY);
  for (const workerId of workerIds) {
    const lockKey = reaperLockKey(workerId);
    const acquired = await redis.set(lockKey, '1', 'NX', 'PX', LOCK_TTL_MS);
    if (!acquired) continue; // another reaper instance is already handling this worker

    try {
      await reapWorker(workerId);
    } catch (err) {
      logger.error({ err, workerId }, 'Failed reaping worker processing list');
    }
  }
}

async function reapWorker(workerId) {
  const jobIds = await redis.lrange(processingKey(workerId), 0, -1);
  if (jobIds.length === 0) return;

  const now = Date.now();
  for (const jobId of jobIds) {
    const hb = await redis.hget(HEARTBEATS_KEY, jobId);
    const lastBeat = hb ? Number(hb) : 0;
    const age = now - lastBeat;
    if (hb && age < config.orphanTimeoutMs) continue; // still alive, skip

    await requeueOrphan(workerId, jobId, age);
  }
}

async function requeueOrphan(workerId, jobId, age) {
  const removed = await redis.lrem(processingKey(workerId), 0, jobId);
  if (!removed) return; // job finished/was reaped concurrently between LRANGE and here

  await redis.hdel(HEARTBEATS_KEY, jobId);

  const raw = await redis.hget(JOB_DATA_KEY, jobId);
  const { rows } = await pool.query('SELECT * FROM jobs WHERE id = $1', [jobId]);
  const job = rows[0];
  if (!job) {
    logger.warn({ jobId, workerId }, 'Orphaned job had no Postgres record; dropping');
    return;
  }

  const envelope = raw
    ? JSON.parse(raw)
    : { id: jobId, type: job.type, payload: job.payload, priority: job.priority, attempt: job.attempt, maxRetries: job.max_retries };

  const nextAttempt = envelope.attempt + 1;
  const message = `Reaped: worker ${workerId} heartbeat stale (${age}ms) -- likely crash mid-execution.`;

  if (nextAttempt <= envelope.maxRetries) {
    const delayMs = computeBackoffMs(envelope.attempt, {
      baseMs: config.backoffBaseMs,
      maxDelayMs: config.backoffMaxMs,
    });
    await pool.query(
      `UPDATE jobs SET status = 'delayed', attempt = $2, error = $3, worker_id = NULL,
                        scheduled_for = now() + ($4 || ' milliseconds')::interval
       WHERE id = $1`,
      [jobId, nextAttempt, message, String(delayMs)]
    );
    await logEvent(jobId, 'delayed', `${message} Retry ${nextAttempt}/${envelope.maxRetries} in ${delayMs}ms.`);
    await publish({ jobId, status: 'delayed', message: `${message} Requeued (attempt ${nextAttempt}).` });

    const updatedEnvelope = { ...envelope, attempt: nextAttempt };
    await redis.hset(JOB_DATA_KEY, jobId, JSON.stringify(updatedEnvelope));
    await redis.zadd(DELAYED_KEY, Date.now() + delayMs, jobId);
    logger.info({ jobId, workerId, nextAttempt, delayMs }, 'Orphaned job requeued with backoff');
  } else {
    await pool.query(
      `UPDATE jobs SET status = 'dead', attempt = $2, error = $3, worker_id = NULL WHERE id = $1`,
      [jobId, nextAttempt, message]
    );
    await logEvent(jobId, 'dead', `${message} Retries exhausted -- moved to DLQ.`);
    await publish({ jobId, status: 'dead', message: 'Moved to dead-letter queue after crash + retry exhaustion' });
    await redis.hdel(JOB_DATA_KEY, jobId);
    logger.warn({ jobId, workerId }, 'Orphaned job exhausted retries; moved to DLQ');
  }
}

async function logEvent(jobId, status, message) {
  await pool.query('INSERT INTO job_events (job_id, status, message) VALUES ($1,$2,$3)', [jobId, status, message]);
}

async function publish(event) {
  await redis.publish(EVENTS_CHANNEL, JSON.stringify({ ...event, ts: new Date().toISOString() }));
}

let pollTimer;
function start() {
  logger.info({ pollIntervalMs: config.pollIntervalMs, orphanTimeoutMs: config.orphanTimeoutMs }, 'Reaper starting');
  pollTimer = setInterval(() => {
    tick().catch((err) => logger.error({ err }, 'Unhandled reaper tick error'));
  }, config.pollIntervalMs);
}

async function shutdown(signal) {
  logger.info({ signal }, 'Reaper shutting down');
  clearInterval(pollTimer);
  await pool.end();
  redis.disconnect();
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

start();
