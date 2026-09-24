export const config = {
  port: Number(process.env.PORT || 3000),
  host: process.env.HOST || '0.0.0.0',
  apiKeys: (process.env.API_KEYS || 'dev-key-123')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean),
  rateLimitPerMin: Number(process.env.RATE_LIMIT_PER_MIN || 600),
  redisUrl: process.env.REDIS_URL || 'redis://localhost:6379',
  databaseUrl: process.env.DATABASE_URL || 'postgres://jobqueue:jobqueue@localhost:5432/jobqueue',
  maxRetriesDefault: Number(process.env.MAX_RETRIES_DEFAULT || 3),
  backoffBaseMs: Number(process.env.BACKOFF_BASE_MS || 1000),
  backoffMaxMs: Number(process.env.BACKOFF_MAX_MS || 300000),
  idempotencyTtlSeconds: Number(process.env.IDEMPOTENCY_TTL_SECONDS || 86400),
  jobRetentionDays: Number(process.env.JOB_RETENTION_DAYS || 30),
};
