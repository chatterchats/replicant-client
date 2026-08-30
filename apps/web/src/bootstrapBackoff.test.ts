import { describe, expect, it } from "vitest";

import { createBootstrapBackoff } from "./bootstrapBackoff";

describe("bootstrap backoff", () => {
  it("keeps retries bounded over a 30-second window", () => {
    const backoff = createBootstrapBackoff({
      baseDelayMs: 500,
      maxDelayMs: 10_000,
      jitterRatio: 0,
      random: () => 0,
    });
    let elapsedMs = 0;
    let retries = 0;

    while (elapsedMs <= 30_000) {
      elapsedMs += backoff.nextDelayMs();
      if (elapsedMs <= 30_000) retries += 1;
    }

    expect(retries).toBe(6);
    expect(backoff.attempt).toBe(7);
  });

  it("applies exponential growth and then stays at the cap", () => {
    const backoff = createBootstrapBackoff({
      baseDelayMs: 1_000,
      maxDelayMs: 5_000,
      jitterRatio: 0.5,
      random: () => 1,
    });

    expect(backoff.nextDelayMs()).toBe(1_500);
    expect(backoff.nextDelayMs()).toBe(3_000);
    expect(backoff.nextDelayMs()).toBe(5_000);
    expect(backoff.nextDelayMs()).toBe(5_000);
    expect(backoff.nextDelayMs()).toBeLessThanOrEqual(5_000);
  });

  it("keeps jitter within the configured bounds", () => {
    const low = createBootstrapBackoff({
      baseDelayMs: 2_000,
      maxDelayMs: 20_000,
      jitterRatio: 0.25,
      random: () => 0,
    });
    const high = createBootstrapBackoff({
      baseDelayMs: 2_000,
      maxDelayMs: 20_000,
      jitterRatio: 0.25,
      random: () => 1,
    });

    expect(low.nextDelayMs()).toBe(2_000);
    expect(high.nextDelayMs()).toBe(2_500);
  });

  it("resets to the first retry after success", () => {
    const backoff = createBootstrapBackoff({
      baseDelayMs: 300,
      maxDelayMs: 10_000,
      jitterRatio: 0,
      random: () => 0,
    });

    expect(backoff.nextDelayMs()).toBe(300);
    expect(backoff.nextDelayMs()).toBe(600);
    expect(backoff.attempt).toBe(2);

    backoff.reset();

    expect(backoff.attempt).toBe(0);
    expect(backoff.nextDelayMs()).toBe(300);
  });
});
