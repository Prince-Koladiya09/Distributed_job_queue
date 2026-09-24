import { buildApp } from './app.js';
import { config } from './config.js';
import { promoteDueDelayedJobs } from './lib/queue.js';
import { pool } from './db.js';
import { redis, redisSub } from './redis.js';

async function main() {
  const app = await buildApp();

  // FR-2.2 promotion poller lives in-process here for simplicity; in a
  // multi-instance deployment this is safe to run on every instance
  // since promotion (ZREM-then-push) is idempotent -- see
  // lib/queue.js#promoteDueDelayedJobs.
  const promoterInterval = setInterval(() => {
    promoteDueDelayedJobs().catch((err) => app.log.error({ err }, 'promoteDueDelayedJobs failed'));
  }, 1000);

  await app.listen({ port: config.port, host: config.host });
  app.log.info(`API server listening on ${config.host}:${config.port}`);

  async function shutdown(signal) {
    app.log.info(`Received ${signal}, shutting down gracefully...`);
    clearInterval(promoterInterval);
    try {
      await app.close();
      await pool.end();
      redis.disconnect();
      redisSub.disconnect();
      app.log.info('Shutdown complete.');
      process.exit(0);
    } catch (err) {
      app.log.error({ err }, 'Error during shutdown');
      process.exit(1);
    }
  }

  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT', () => shutdown('SIGINT'));
}

main().catch((err) => {
  // eslint-disable-next-line no-console
  console.error('Fatal startup error', err);
  process.exit(1);
});
