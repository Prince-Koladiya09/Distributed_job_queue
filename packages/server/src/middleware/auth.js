import { config } from '../config.js';

/**
 * FR-8.1: API endpoints shall require an API key (`x-api-key` header).
 * Attaches `request.apiKey` on success so downstream middleware (rate
 * limiting) can key off of it.
 */
export function apiKeyAuth(request, reply, done) {
  const key = request.headers['x-api-key'];
  if (!key || !config.apiKeys.includes(key)) {
    reply.code(401).send({
      error: 'unauthorized',
      message: 'A valid x-api-key header is required.',
    });
    return;
  }
  request.apiKey = key;
  done();
}
