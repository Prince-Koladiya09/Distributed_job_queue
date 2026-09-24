import { pool } from '../db.js';

const createSchema = {
  body: {
    type: 'object',
    required: ['type', 'cronExpression'],
    additionalProperties: false,
    properties: {
      type: { type: 'string', minLength: 1, maxLength: 255 },
      cronExpression: { type: 'string', minLength: 1, maxLength: 120 },
      payloadTemplate: { type: 'object' },
      priority: { type: 'string', enum: ['high', 'normal', 'low'] },
      maxRetries: { type: 'integer', minimum: 0, maximum: 50 },
      enabled: { type: 'boolean' },
    },
  },
};

export default async function scheduleRoutes(app) {
  // POST /schedules -- FR-5.1
  app.post('/schedules', { schema: createSchema }, async (request, reply) => {
    const {
      type,
      cronExpression,
      payloadTemplate = {},
      priority = 'normal',
      maxRetries = 3,
      enabled = true,
    } = request.body;

    const { rows } = await pool.query(
      `INSERT INTO scheduled_jobs (type, cron_expression, payload_template, priority, max_retries, enabled)
       VALUES ($1,$2,$3,$4,$5,$6) RETURNING *`,
      [type, cronExpression, payloadTemplate, priority, maxRetries, enabled]
    );
    reply.code(201).send(serialize(rows[0]));
  });

  // GET /schedules
  app.get('/schedules', async (request, reply) => {
    const { rows } = await pool.query('SELECT * FROM scheduled_jobs ORDER BY created_at DESC');
    reply.send(rows.map(serialize));
  });

  // PATCH /schedules/:id -- enable/disable, used by dashboard.
  app.patch('/schedules/:id', async (request, reply) => {
    const { enabled } = request.body || {};
    if (typeof enabled !== 'boolean') {
      return reply.code(400).send({ error: 'validation_error', message: '"enabled" boolean required' });
    }
    const { rows } = await pool.query(
      'UPDATE scheduled_jobs SET enabled = $1 WHERE id = $2 RETURNING *',
      [enabled, request.params.id]
    );
    if (!rows[0]) return reply.code(404).send({ error: 'not_found' });
    reply.send(serialize(rows[0]));
  });

  app.delete('/schedules/:id', async (request, reply) => {
    const { rowCount } = await pool.query('DELETE FROM scheduled_jobs WHERE id = $1', [request.params.id]);
    if (!rowCount) return reply.code(404).send({ error: 'not_found' });
    reply.code(204).send();
  });
}

function serialize(row) {
  return {
    id: row.id,
    type: row.type,
    cronExpression: row.cron_expression,
    payloadTemplate: row.payload_template,
    priority: row.priority,
    maxRetries: row.max_retries,
    enabled: row.enabled,
    lastRunAt: row.last_run_at,
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}
