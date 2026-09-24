import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import pg from 'pg';
import cronParser from 'cron-parser';
import {
  createLogger,
  queueKey,
  DELAYED_KEY,
  JOB_DATA_KEY,
  EVENTS_CHANNEL,
  SCHEDULER_LOCK_KEY,
} from '@jobqueue/shared';

const logger = createLogger('scheduler');

const config = {
  pollIntervalMs: Number(process.env.SCHEDULER_POLL_INTERVAL_MS || 1000),
  maxRetriesDefault: Number(process.env.MAX_RETRIES_DEFAULT || 3),
  lockTtlMs: Number(process.env.SCHEDULER_POLL_INTERVAL_MS || 1000) * 5,
};

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://jobqueue:jobqueue@localhost:5432/jobqueue',
});

const instanceId = randomUUID();

// Lua script: only delete the lock if it's still ours (avoids a slow
// instance deleting a lock a *different* instance has since acquired
// after the original TTL expired).
const RELEASE_LOCK_LUA = `
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
else
  return 0
end
`;

/**
 * FR-5.2: A scheduler process shall enqueue recurring jobs at their
 * configured interval without duplication (using a Redis lock to
 * prevent multiple scheduler instances double-firing). Any number of
 * scheduler replicas can run for HA; only the lock holder acts on a
 * given tick.
 */
async function tick() {
  const acquired = await redis.set(SCHEDULER_LOCK_KEY, instanceId, 'NX', 'PX', config.lockTtlMs);
  if (!acquired) return; // another instance holds the lock this tick

  try {
    const { rows: defs } = await pool.query('SELECT * FROM scheduled_jobs WHERE enabled = true');
    const now = new Date();

    for (const def of defs) {
      if (!isDue(def, now)) continue;
      await fireScheduledJob(def, now);
    }
  } catch (err) {
    logger.error({ err }, 'Scheduler tick failed');
  } finally {
    await redis.eval(RELEASE_LOCK_LUA, 1, SCHEDULER_LOCK_KEY, instanceId);
  }
}

/**
 * FR-5.1: cron-like recurring definitions. A definition is "due" if
 * its most recent scheduled fire time (per the cron expression) is
 * after last_run_at (or last_run_at is null, i.e. never run).
 */
function isDue(def, now) {
  try {
    const interval = cronParser.parseExpression(def.cron_expression, { currentDate: now });
    const prevFireTime = interval.prev().toDate();
    if (!def.last_run_at) return prevFireTime <= now;
    return prevFireTime > new Date(def.last_run_at);
  } catch (err) {
    logger.error({ err, cron: def.cron_expression, defId: def.id }, 'Invalid cron expression; skipping');
    return false;
  }
}

async function fireScheduledJob(def, now) {
  const id = randomUUID();
  const { rows } = await pool.query(
    `INSERT INTO jobs (id, type, payload, priority, status, attempt, max_retries, created_at, updated_at)
     VALUES ($1,$2,$3,$4,'queued',0,$5,$6,$6) RETURNING *`,
    [id, def.type, def.payload_template, def.priority, def.max_retries || config.maxRetriesDefault, now]
  );
  const job = rows[0];

  await pool.query('INSERT INTO job_events (job_id, status, message) VALUES ($1,$2,$3)', [
    id,
    'queued',
    `Enqueued by scheduler from recurring definition ${def.id} (${def.cron_expression})`,
  ]);
  await pool.query('UPDATE scheduled_jobs SET last_run_at = $2 WHERE id = $1', [def.id, now]);

  const envelope = {
    id,
    type: job.type,
    payload: job.payload,
    priority: job.priority,
    attempt: 0,
    maxRetries: job.max_retries,
  };
  await redis.hset(JOB_DATA_KEY, id, JSON.stringify(envelope));
  await redis.lpush(queueKey(job.priority), id);
  await redis.publish(
    EVENTS_CHANNEL,
    JSON.stringify({ jobId: id, status: 'queued', message: 'Recurring job fired', ts: now.toISOString() })
  );

  logger.info({ jobId: id, type: def.type, scheduledJobId: def.id }, 'Recurring job fired');
}

let pollTimer;
function start() {
  logger.info({ instanceId, pollIntervalMs: config.pollIntervalMs }, 'Scheduler starting');
  pollTimer = setInterval(() => {
    tick().catch((err) => logger.error({ err }, 'Unhandled scheduler tick error'));
  }, config.pollIntervalMs);
}

async function shutdown(signal) {
  logger.info({ signal }, 'Scheduler shutting down');
  clearInterval(pollTimer);
  await pool.end();
  redis.disconnect();
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));

start();
