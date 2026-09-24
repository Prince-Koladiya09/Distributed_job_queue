/**
 * Exponential backoff, per FR-4.1:
 *   delay = base * 2^attempt, capped at maxDelayMs.
 *
 * `attempt` is the 0-indexed retry number (0 = first retry after the
 * initial failed attempt). A small amount of "full jitter" is added by
 * default to avoid thundering-herd retries; pass jitter: false for
 * deterministic delays (used by unit tests).
 */
export function computeBackoffMs(attempt, {
  baseMs = 1000,
  maxDelayMs = 300000,
  jitter = true,
} = {}) {
  if (attempt < 0) throw new Error('attempt must be >= 0');
  const raw = baseMs * Math.pow(2, attempt);
  const capped = Math.min(raw, maxDelayMs);
  if (!jitter) return capped;
  // Full jitter: random value in [0, capped]
  return Math.floor(Math.random() * capped);
}

/** Convenience: the un-jittered delay, useful for display/estimation. */
export function computeBackoffCeilingMs(attempt, {
  baseMs = 1000,
  maxDelayMs = 300000,
} = {}) {
  if (attempt < 0) throw new Error('attempt must be >= 0');
  return Math.min(baseMs * Math.pow(2, attempt), maxDelayMs);
}
