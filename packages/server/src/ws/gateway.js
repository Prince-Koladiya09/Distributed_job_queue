import { EVENTS_CHANNEL } from '@jobqueue/shared';
import { redisSub } from '../redis.js';
import { getQueueStats } from '../lib/queue.js';

/**
 * FR-6.3: push real-time job state changes over WebSocket to connected
 * dashboard clients. We subscribe once to the shared Redis pub/sub
 * channel (which any API instance or worker can publish to) and fan
 * that single stream out to every connected WebSocket client -- this
 * is what lets the API layer scale horizontally without clients
 * missing events published via a sibling instance.
 */
export function registerWsGateway(app) {
  const clients = new Set();

  redisSub.subscribe(EVENTS_CHANNEL, (err) => {
    if (err) app.log.error({ err }, 'Failed to subscribe to events channel');
  });

  redisSub.on('message', (channel, message) => {
    if (channel !== EVENTS_CHANNEL) return;
    for (const socket of clients) {
      if (socket.readyState === socket.OPEN) {
        socket.send(message);
      }
    }
  });

  // Periodic queue-stats broadcast, independent of individual job
  // events, so the dashboard's depth/throughput panels stay live even
  // during quiet periods.
  const statsInterval = setInterval(async () => {
    if (clients.size === 0) return;
    try {
      const stats = await getQueueStats();
      const payload = JSON.stringify({ type: 'stats', ...stats, ts: new Date().toISOString() });
      for (const socket of clients) {
        if (socket.readyState === socket.OPEN) socket.send(payload);
      }
    } catch (err) {
      app.log.error({ err }, 'Failed to broadcast queue stats');
    }
  }, 3000);

  app.get('/ws', { websocket: true }, (connection) => {
    const socket = connection.socket ?? connection;
    clients.add(socket);
    socket.send(JSON.stringify({ type: 'connected', ts: new Date().toISOString() }));

    socket.on('close', () => clients.delete(socket));
    socket.on('error', () => clients.delete(socket));
  });

  app.addHook('onClose', (instance, done) => {
    clearInterval(statsInterval);
    done();
  });
}
