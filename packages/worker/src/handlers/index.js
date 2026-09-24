import sendEmail from './sendEmail.js';
import noop from './noop.js';

/**
 * Job "type" -> handler function. New handlers are registered here.
 * v1 is Node-only / in-process (see SRS 1.2 out-of-scope: cross-language
 * SDKs, plugin marketplace are future work per section 11).
 */
export const handlers = {
  send_email: sendEmail,
  noop,
};

export function getHandler(type) {
  const handler = handlers[type];
  if (!handler) {
    throw new Error(`No handler registered for job type "${type}"`);
  }
  return handler;
}
