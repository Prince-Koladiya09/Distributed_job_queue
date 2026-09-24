import pino from 'pino';

/**
 * Structured JSON logger shared by every process (NFR-5: "All state
 * transitions shall be logged with structured JSON logs (job ID,
 * timestamp, status)"). NFR-7 requires secrets are never logged, so
 * callers must never pass credential fields into `bindings`.
 */
export function createLogger(service) {
  return pino({
    name: service,
    level: process.env.LOG_LEVEL || 'info',
    redact: {
      paths: ['*.password', '*.apiKey', '*.REDIS_URL', '*.DATABASE_URL', 'req.headers["x-api-key"]'],
      censor: '[REDACTED]',
    },
    timestamp: pino.stdTimeFunctions.isoTime,
  });
}
