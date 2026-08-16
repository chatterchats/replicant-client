import { describe, expect, it } from "vitest";

import { absoluteTime, relativeTime } from "./time";

describe("relativeTime", () => {
  const now = Date.UTC(2026, 7, 16, 12, 0, 0);

  it("describes recent timestamps in elapsed units", () => {
    expect(relativeTime(now - 30_000, now)).toContain("30");
    expect(relativeTime(now - 30_000, now)).toContain("second");
    expect(relativeTime(now - 5 * 60_000, now)).toContain("minute");
    expect(relativeTime(now - 3 * 3_600_000, now)).toContain("hour");
    expect(relativeTime(now - 4 * 86_400_000, now)).toContain("day");
  });

  it("handles future timestamps and long spans", () => {
    expect(relativeTime(now + 120_000, now)).toContain("minute");
    expect(relativeTime(now - 400 * 86_400_000, now)).toContain("year");
  });

  it("renders an absolute value for tooltips", () => {
    expect(absoluteTime(now)).toEqual(new Date(now).toLocaleString());
  });
});
