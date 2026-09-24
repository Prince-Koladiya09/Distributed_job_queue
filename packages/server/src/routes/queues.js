import { getQueueStats } from '../lib/queue.js';

export default async function queueRoutes(app) {
  // FR-6.4: queue depth per priority, throughput, failure rate, DLQ count.
  app.get('/queues/stats', async (request, reply) => {
    const stats = await getQueueStats();
    reply.send(stats);
  });
}
