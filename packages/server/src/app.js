import Fastify from 'fastify';
import websocketPlugin from '@fastify/websocket';
import { apiKeyAuth } from './middleware/auth.js';
import { rateLimit } from './middleware/rateLimit.js';
import { registerWsGateway } from './ws/gateway.js';
import jobRoutes from './routes/jobs.js';
import scheduleRoutes from './routes/schedules.js';
import queueRoutes from './routes/queues.js';
import { pool } from './db.js';
import { redis } from './redis.js';

export async function buildApp() {
  const app = Fastify({
    logger: {
      level: process.env.LOG_LEVEL || 'info',
      // NFR-7: never log secrets/credentials.
      redact: ['req.headers["x-api-key"]', 'req.headers.authorization'],
    },
    trustProxy: true,
  });

  await app.register(websocketPlugin);

  // Health check -- unauthenticated, used by container orchestrators.
  app.get('/health', async (request, reply) => {
    const checks = { api: true, redis: false, postgres: false };
    try {
      await redis.ping();
      checks.redis = true;
    } catch { /* leave false */ }
    try {
      await pool.query('SELECT 1');
      checks.postgres = true;
    } catch { /* leave false */ }
    const healthy = checks.redis && checks.postgres;
    reply.code(healthy ? 200 : 503).send({ status: healthy ? 'ok' : 'degraded', checks });
  });

  // Prometheus-format metrics stub for future Grafana integration (10.4).
  app.get('/metrics', async (request, reply) => {
    const { getQueueStats } = await import('./lib/queue.js');
    const stats = await getQueueStats();
    reply.header('Content-Type', 'text/plain; version=0.0.4');
    reply.send(
      [
        `# HELP jobqueue_queue_depth Current queue depth by priority`,
        `# TYPE jobqueue_queue_depth gauge`,
        `jobqueue_queue_depth{priority="high"} ${stats.queueDepth.high}`,
        `jobqueue_queue_depth{priority="normal"} ${stats.queueDepth.normal}`,
        `jobqueue_queue_depth{priority="low"} ${stats.queueDepth.low}`,
        `jobqueue_queue_depth{priority="delayed"} ${stats.queueDepth.delayed}`,
        `# HELP jobqueue_dlq_count Jobs currently in the dead-letter queue`,
        `# TYPE jobqueue_dlq_count gauge`,
        `jobqueue_dlq_count ${stats.dlqCount}`,
        `# HELP jobqueue_throughput_per_min Completed jobs in the last minute`,
        `# TYPE jobqueue_throughput_per_min gauge`,
        `jobqueue_throughput_per_min ${stats.throughputPerMin}`,
        `# HELP jobqueue_failure_rate_last_hour Failure rate over the last hour`,
        `# TYPE jobqueue_failure_rate_last_hour gauge`,
        `jobqueue_failure_rate_last_hour ${stats.failureRateLastHour}`,
        '',
      ].join('\n')
    );
  });

  // WebSocket gateway does its own auth-free connection (dashboard is
  // assumed to sit behind the same network boundary / SSO in prod;
  // see README "Security notes" for the production hardening TODO).
  registerWsGateway(app);

  // Everything else requires an API key + rate limiting (FR-8).
  await app.register(async (authed) => {
    authed.addHook('preHandler', apiKeyAuth);
    authed.addHook('preHandler', rateLimit());

    await authed.register(jobRoutes);
    await authed.register(scheduleRoutes);
    await authed.register(queueRoutes);
  });

  app.setErrorHandler((err, request, reply) => {
    if (err.validation) {
      reply.code(400).send({
        error: 'validation_error',
        message: err.message,
        details: err.validation,
      });
      return;
    }
    request.log.error({ err }, 'Unhandled error');
    reply.code(500).send({ error: 'internal_error', message: 'Something went wrong.' });
  });

  return app;
}
