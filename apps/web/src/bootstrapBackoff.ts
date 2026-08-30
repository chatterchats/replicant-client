export interface BootstrapBackoffOptions {
  /** Delay used for the first retry. */
  baseDelayMs?: number;
  /** Maximum delay between retries. Retries continue indefinitely at this cap. */
  maxDelayMs?: number;
  /** Fraction of the exponential delay available as positive jitter. */
  jitterRatio?: number;
  /** Random source returning a value in [0, 1). */
  random?: () => number;
}

export interface BootstrapBackoff {
  /** Number of retry delays that have been issued. */
  readonly attempt: number;
  /** Returns the next bounded delay and advances the retry attempt. */
  nextDelayMs(): number;
  /** Starts exponential backoff over from the first retry. */
  reset(): void;
}

const DEFAULT_BASE_DELAY_MS = 500;
const DEFAULT_MAX_DELAY_MS = 10_000;
const DEFAULT_JITTER_RATIO = 0.25;

function positiveFinite(value: number | undefined, fallback: number): number {
  return value !== undefined && Number.isFinite(value) && value > 0
    ? value
    : fallback;
}

function nonNegativeFinite(
  value: number | undefined,
  fallback: number,
): number {
  return value !== undefined && Number.isFinite(value) && value >= 0
    ? value
    : fallback;
}

/**
 * Creates state for retries whose delay grows exponentially, then remains at
 * a cap. Jitter is intentionally positive: the exponential delay remains a
 * lower bound while clients avoid retrying in lockstep.
 *
 * The scheduler contains no knowledge of the operation being retried. Callers
 * own the timer and call reset after a successful operation.
 */
export function createBootstrapBackoff(
  options: BootstrapBackoffOptions = {},
): BootstrapBackoff {
  const baseDelayMs = positiveFinite(
    options.baseDelayMs,
    DEFAULT_BASE_DELAY_MS,
  );
  const maxDelayMs = Math.max(
    baseDelayMs,
    positiveFinite(options.maxDelayMs, DEFAULT_MAX_DELAY_MS),
  );
  const jitterRatio = nonNegativeFinite(
    options.jitterRatio,
    DEFAULT_JITTER_RATIO,
  );
  const random = options.random ?? Math.random;
  let attempt = 0;

  return {
    get attempt() {
      return attempt;
    },
    nextDelayMs() {
      // Saturating the exponent avoids overflowing for an indefinitely
      // failing connection while preserving infinite retries at the cap.
      const exponent = Math.min(attempt, 31);
      const exponentialDelay = Math.min(
        maxDelayMs,
        baseDelayMs * 2 ** exponent,
      );
      const jitterWindow = Math.min(
        maxDelayMs - exponentialDelay,
        exponentialDelay * jitterRatio,
      );
      const randomValue = random();
      const boundedRandom = Number.isFinite(randomValue)
        ? Math.min(1, Math.max(0, randomValue))
        : 0;
      const delay = Math.floor(exponentialDelay + jitterWindow * boundedRandom);
      attempt = Math.min(attempt + 1, Number.MAX_SAFE_INTEGER);
      return Math.min(maxDelayMs, Math.max(0, delay));
    },
    reset() {
      attempt = 0;
    },
  };
}
