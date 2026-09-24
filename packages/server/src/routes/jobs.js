import {
  enqueueJob,
  getJobById,
  getJobEvents,
  listJobs,
  retryJob,
  cancelJob,
} from '../lib/queue.js';

const enqueueSchema = {
  body: {
    type: 'object',
    required: ['type'],
    additionalProperties: false,
    properties: {
      type: { type: 'string', minLength: 1, maxLength: 255 },
      payload: { type: 'object' },
      priority: { type: 'string', enum: ['high', 'normal', 'low'] },
      delay: { type: 'integer', minimum: 0 },
      maxRetries: { type: 'integer', minimum: 0, maximum: 50 },
      idempotencyKey: { type: 'string', minLength: 1, maxLength: 255 },
    },
  },
};

const listSchema = {
  querystring: {
    type: 'object',
    properties: {
      status: { type: 'string', enum: ['queued', 'delayed', 'active', 'completed', 'failed', 'dead'] },
      type: { type: 'string' },
      page: { type: 'integer', minimum: 1, default: 1 },
      pageSize: { type: 'integer', minimum: 1, maximum: 100, default: 25 },
    },
  },
};

export default async function jobRoutes(app) {
  // FR-1.1 / FR-1.2 / FR-1.3
  app.post('/jobs', { schema: enqueueSchema }, async (request, reply) => {
    const { job, deduped } = await enqueueJob(request.body);
    reply.code(deduped ? 200 : 201).send({
      id: job.id,
      status: job.status,
      priority: job.priority,
      createdAt: job.created_at,
      deduped,
    });
  });

  // FR-6.1
  app.get('/jobs/:id', async (request, reply) => {
    const job = await getJobById(request.params.id);
    if (!job) return reply.code(404).send({ error: 'not_found' });
    const events = await getJobEvents(job.id);
    reply.send({ ...serializeJob(job), events });
  });

  // FR-6.2
  app.get('/jobs', { schema: listSchema }, async (request, reply) => {
    const { status, type, page, pageSize } = request.query;
    const result = await listJobs({ status, type, page, pageSize });
    reply.send({
      jobs: result.jobs.map(serializeJob),
      total: result.total,
      page: result.page,
      pageSize: result.pageSize,
    });
  });

  // FR-4.3
  app.post('/jobs/:id/retry', async (request, reply) => {
    try {
      const job = await retryJob(request.params.id);
      if (!job) return reply.code(404).send({ error: 'not_found' });
      reply.send(serializeJob(job));
    } catch (err) {
      if (err.code === 'INVALID_STATE') {
        return reply.code(409).send({ error: 'invalid_state', message: err.message });
      }
      throw err;
    }
  });

  // Cancel a queued (not-yet-started) job.
  app.delete('/jobs/:id', async (request, reply) => {
    try {
      const job = await cancelJob(request.params.id);
      if (!job) return reply.code(404).send({ error: 'not_found' });
      reply.send(serializeJob(job));
    } catch (err) {
      if (err.code === 'INVALID_STATE') {
        return reply.code(409).send({ error: 'invalid_state', message: err.message });
      }
      throw err;
    }
  });
}

function serializeJob(job) {
  return {
    id: job.id,
    type: job.type,
    payload: job.payload,
    priority: job.priority,
    status: job.status,
    attempt: job.attempt,
    maxRetries: job.max_retries,
    idempotencyKey: job.idempotency_key,
    scheduledFor: job.scheduled_for,
    createdAt: job.created_at,
    updatedAt: job.updated_at,
    completedAt: job.completed_at,
    error: job.error,
    workerId: job.worker_id,
  };
}
