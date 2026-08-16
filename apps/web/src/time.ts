const UNITS: [
  limit: number,
  divisor: number,
  unit: Intl.RelativeTimeFormatUnit,
][] = [
  [60_000, 1_000, "second"],
  [3_600_000, 60_000, "minute"],
  [86_400_000, 3_600_000, "hour"],
  [604_800_000, 86_400_000, "day"],
  [2_629_800_000, 604_800_000, "week"],
  [31_557_600_000, 2_629_800_000, "month"],
];

const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });

/**
 * Formats a timestamp relative to now ("32 seconds ago").
 *
 * Operations consoles are read at a glance, where elapsed time answers the
 * actual question ("is this stuck?") faster than a wall-clock reading does.
 * Pair with an absolute value in a `title` or `dateTime` attribute.
 */
export function relativeTime(timestampMs: number, now = Date.now()): string {
  const elapsed = timestampMs - now;
  const magnitude = Math.abs(elapsed);
  for (const [limit, divisor, unit] of UNITS) {
    if (magnitude < limit)
      return formatter.format(Math.round(elapsed / divisor), unit);
  }
  return formatter.format(Math.round(elapsed / 31_557_600_000), "year");
}

/** Absolute rendering for tooltips and `dateTime` attributes. */
export function absoluteTime(timestampMs: number): string {
  return new Date(timestampMs).toLocaleString();
}
