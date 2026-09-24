/** Trivial handler useful for load testing (k6/Artillery scenarios). */
export default async function noop(payload, context) {
  context.logger.debug({ payload }, 'noop handler executed');
  return { ok: true };
}
