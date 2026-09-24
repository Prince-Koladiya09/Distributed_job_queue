import { randomUUID } from 'node:crypto';
import Redis from 'ioredis';
import pg from 'pg';
import { createLogger, WORKERS_SET_KEY } from '@jobqueue/shared';
import { claimNextJob } from './claim.js';
import { processJob } from './process.js';
import { createShutdownController } from './shutdown.js';

const workerId = process.env.WORKER_ID || `worker-${randomUUID().slice(0, 8)}`;
const logger = createLogger(`worker:${workerId}`);

const config = {
  concurrency: Number(process.env.WORKER_CONCURRENCY || 4),
  defaultTimeoutMs: Number(process.env.JOB_DEFAULT_TIMEOUT_MS || 30000),
  heartbeatIntervalMs: Number(process.env.JOB_HEARTBEAT_INTERVAL_MS || 5000),
  gracePeriodMs: Number(process.env.SHUTDOWN_GRACE_PERIOD_MS || 15000),
  backoffBaseMs: Number(process.env.BACKOFF_BASE_MS || 1000),
  backoffMaxMs: Number(process.env.BACKOFF_MAX_MS || 300000),
};

const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
const pool = new pg.Pool({
  connectionString: process.env.DATABASE_URL || 'postgres://jobqueue:jobqueue@localhost:5432/jobqueue',
});

const inFlight = new Set();
const shutdownController = createShutdownController({ gracePeriodMs: config.gracePeriodMs, logger, inFlight });

async function registerWorker() {
  // Lets the reaper discover every active worker's processing list
  // (section 5.4) without a blocking KEYS scan.
  await redis.sadd(WORKERS_SET_KEY, workerId);
}

async function deregisterWorker() {
  await redis.srem(WORKERS_SET_KEY, workerId);
}

/** One lane of the concurrency pool: claim -> process -> repeat. */
async function runLane(laneId) {
  while (!shutdownController.isDraining()) {
    let claimed;
    try {
      claimed = await claimNextJob(redis, workerId, { blockSeconds: 1 });
    } catch (err) {
      logger.error({ err, laneId }, 'claimNextJob failed; backing off');
      await sleep(1000);
      continue;
    }
    if (!claimed) continue; // nothing to claim; loop (blmove already waited briefly)

    inFlight.add(claimed.id);
    try {
      const outcome = await processJob({ redis, pool, workerId, claimed, config, logger });
      logger.info({ jobId: claimed.id, laneId, outcome: outcome.outcome }, 'Job processed');
    } catch (err) {
      logger.error({ err, jobId: claimed.id, laneId }, 'Unhandled error while processing job');
    } finally {
      inFlight.delete(claimed.id);
    }
  }
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function main() {
  await registerWorker();
  logger.info({ workerId, concurrency: config.concurrency }, 'Worker starting');

  const lanes = Array.from({ length: config.concurrency }, (_, i) => runLane(i));

  async function shutdown(signal) {
    await shutdownController.drain(signal);
    await Promise.allSettled(lanes);
    await deregisterWorker();
    await pool.end();
    redis.disconnect();
    logger.info('Worker exited.');
    process.exit(0);
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));

  await Promise.allSettled(lanes);
}

main().catch((err) => {
  logger.error({ err }, 'Fatal worker error');
  process.exit(1);
});
