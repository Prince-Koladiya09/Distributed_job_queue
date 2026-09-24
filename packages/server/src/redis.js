import Redis from 'ioredis';
import { config } from './config.js';

export const redis = new Redis(config.redisUrl, {
  maxRetriesPerRequest: 3,
  enableAutoPipelining: true,
});

// A dedicated connection for pub/sub subscription (ioredis requires a
// separate connection once a client enters subscriber mode).
export const redisSub = new Redis(config.redisUrl);

redis.on('error', (err) => {
  // eslint-disable-next-line no-console
  console.error('Redis client error', err.message);
});
