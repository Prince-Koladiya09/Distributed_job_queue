import { redis } from '../redis.js';
import { config } from '../config.js';

/**
 * FR-8.2: per-key token bucket rate limiting, returning 429 when
 * exceeded. Implemented with a Redis Lua script so the read-decrement
 * is atomic even under concurrent requests for the same key.
 *
 * Bucket capacity == rateLimitPerMin. Tokens refill continuously at
 * capacity/60000 tokens per ms, capped at capacity ("token bucket").
 */
const TOKEN_BUCKET_LUA = `
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refillPerMs = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local ttlMs = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts = tonumber(bucket[2])

if tokens == nil then
  tokens = capacity
  ts = now
end

local elapsed = math.max(0, now - ts)
tokens = math.min(capacity, tokens + elapsed * refillPerMs)

local allowed = 0
if tokens >= 1 then
  tokens = tokens - 1
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', key, ttlMs)

return { allowed, tokens }
`;

export function rateLimit(options = {}) {
  const capacity = options.perMinute || config.rateLimitPerMin;
  const refillPerMs = capacity / 60000;

  return async function rateLimitHandler(request, reply) {
    const bucketKey = `ratelimit:${request.apiKey || request.ip}`;
    const now = Date.now();
    const [allowed] = await redis.eval(
      TOKEN_BUCKET_LUA,
      1,
      bucketKey,
      capacity,
      refillPerMs,
      now,
      120000
    );
    if (!allowed) {
      reply.code(429).send({
        error: 'rate_limited',
        message: `Rate limit of ${capacity}/min exceeded for this API key.`,
      });
      return reply;
    }
  };
}
