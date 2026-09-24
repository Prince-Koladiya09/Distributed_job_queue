import { computeBackoffMs, DELAYED_KEY, JOB_DATA_KEY, EVENTS_CHANNEL } from '@jobqueue/shared';
import { getHandler } from './handlers/index.js';
import { releaseJob, heartbeat } from './claim.js';

class TimeoutError extends Error {}

function withTimeout(promise, ms) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new TimeoutError(`Job timed out after ${ms}ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * Executes one claimed job end-to-end:
 *  - marks it `active` in Postgres
 *  - runs the handler with a per-job timeout (FR-3.3)
 *  - on success: marks `completed`, removes from processing list
 *  - on failure: FR-4.1 exponential backoff retry, or FR-4.2 DLQ
 *    ("dead") once maxRetries is exhausted
 */
export async function processJob({ redis, pool, workerId, claimed, config, logger }) {
  const { id, priority, envelope } = claimed;
  const heartbeatTimer = setInterval(() => {
    heartbeat(redis, id).catch(() => {});
  }, config.heartbeatIntervalMs);

  try {
    await pool.query(
      `UPDATE jobs SET status = 'active', worker_id = $2 WHERE id = $1`,
      [id, workerId]
    );
    await logEvent(pool, id, 'active', `Claimed by worker ${workerId}`);
    await publish(redis, { jobId: id, status: 'active', message: `Claimed by worker ${workerId}` });

    const handler = getHandler(envelope.type);
    const result = await withTimeout(
      handler(envelope.payload, { jobId: id, attempt: envelope.attempt, logger }),
      config.defaultTimeoutMs
    );

    await pool.query(
      `UPDATE jobs SET status = 'completed', completed_at = now(), error = NULL WHERE id = $1`,
      [id]
    );
    await logEvent(pool, id, 'completed', 'Job completed successfully');
    await publish(redis, { jobId: id, status: 'completed', message: 'Job completed successfully' });

    await redis.hdel(JOB_DATA_KEY, id);
    await releaseJob(redis, workerId, id);
    return { outcome: 'completed', result };
  } catch (err) {
    return await handleFailure({ redis, pool, workerId, claimed, config, logger, err });
  } finally {
    clearInterval(heartbeatTimer);
  }
}

async function handleFailure({ redis, pool, workerId, claimed, config, logger, err }) {
  const { id, envelope } = claimed;
  const isTimeout = err instanceof TimeoutError;
  const nextAttempt = envelope.attempt + 1;
  const willRetry = nextAttempt <= envelope.maxRetries;

  logger.warn({ jobId: id, err: err.message, isTimeout, nextAttempt, willRetry }, 'Job execution failed');

  if (willRetry) {
    // FR-4.1: delay = base * 2^attempt, capped.
    const delayMs = computeBackoffMs(envelope.attempt, {
      baseMs: config.backoffBaseMs,
      maxDelayMs: config.backoffMaxMs,
    });

    await pool.query(
      `UPDATE jobs SET status = 'delayed', attempt = $2, error = $3, scheduled_for = now() + ($4 || ' milliseconds')::interval
       WHERE id = $1`,
      [id, nextAttempt, err.message, String(delayMs)]
    );
    await logEvent(pool, id, 'delayed', `Attempt ${nextAttempt} failed: ${err.message}. Retrying in ${delayMs}ms.`);
    await publish(redis, {
      jobId: id,
      status: 'delayed',
      message: `Retry ${nextAttempt}/${envelope.maxRetries} scheduled in ${delayMs}ms`,
    });

    const updatedEnvelope = { ...envelope, attempt: nextAttempt };
    await redis.hset(JOB_DATA_KEY, id, JSON.stringify(updatedEnvelope));
    await redis.zadd(DELAYED_KEY, Date.now() + delayMs, id);
    await releaseJob(redis, workerId, id);
    return { outcome: 'retry-scheduled', delayMs };
  }

  // FR-4.2: exhausted -> DLQ.
  await pool.query(
    `UPDATE jobs SET status = 'dead', attempt = $2, error = $3 WHERE id = $1`,
    [id, nextAttempt, err.message]
  );
  await logEvent(pool, id, 'dead', `Exhausted ${envelope.maxRetries} retries. Last error: ${err.message}`);
  await publish(redis, { jobId: id, status: 'dead', message: 'Moved to dead-letter queue (retries exhausted)' });

  await redis.hdel(JOB_DATA_KEY, id);
  await releaseJob(redis, workerId, id);
  return { outcome: 'dead' };
}

async function logEvent(pool, jobId, status, message) {
  await pool.query('INSERT INTO job_events (job_id, status, message) VALUES ($1,$2,$3)', [jobId, status, message]);
}

async function publish(redis, event) {
  await redis.publish(EVENTS_CHANNEL, JSON.stringify({ ...event, ts: new Date().toISOString() }));
}

export { TimeoutError };
