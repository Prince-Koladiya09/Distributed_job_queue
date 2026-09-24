/**
 * FR-7.1: On SIGTERM, workers shall stop claiming new jobs and finish
 * in-flight jobs within a configurable grace period before exiting.
 */
export function createShutdownController({ gracePeriodMs, logger, inFlight }) {
  let draining = false;

  function isDraining() {
    return draining;
  }

  async function drain(signal) {
    if (draining) return;
    draining = true;
    logger.info({ signal, inFlightCount: inFlight.size }, 'Draining: no longer claiming new jobs');

    const deadline = Date.now() + gracePeriodMs;
    while (inFlight.size > 0 && Date.now() < deadline) {
      await new Promise((resolve) => setTimeout(resolve, 200));
    }

    if (inFlight.size > 0) {
      logger.warn(
        { remaining: inFlight.size },
        `Grace period (${gracePeriodMs}ms) elapsed with jobs still in-flight; exiting anyway. ` +
          'These jobs remain in their processing list and will be recovered by the reaper.'
      );
    } else {
      logger.info('All in-flight jobs finished cleanly.');
    }
  }

  return { isDraining, drain };
}
