/**
 * Example job handler. Handlers receive the job's payload and a
 * `context` object (jobId, attempt, logger) and either resolve
 * (success) or throw (failure -> triggers retry/backoff per FR-4).
 *
 * This one just simulates an email send. It intentionally fails on the
 * first attempt for `template === "flaky-demo"` so the retry/backoff
 * path is exercisable end-to-end in local demos.
 */
export default async function sendEmail(payload, context) {
  const { to, template } = payload;
  if (!to) {
    // A non-retryable-looking validation error still goes through the
    // normal retry path in v1 (handlers are simple functions); callers
    // that want "no retry" should set maxRetries: 0 on the job.
    throw new Error('payload.to is required for send_email jobs');
  }

  context.logger.info({ to, template, attempt: context.attempt }, 'Sending email');

  if (template === 'flaky-demo' && context.attempt < 1) {
    throw new Error('Simulated transient email provider failure');
  }

  // Simulate network latency.
  await new Promise((resolve) => setTimeout(resolve, 100 + Math.random() * 200));

  return { sent: true, to, template };
}
