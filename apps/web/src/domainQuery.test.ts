import { describe, expect, it, vi } from "vitest";

import { createRequestGate } from "./domainQuery";

describe("domain request gate", () => {
  it("coalesces repeated invalidations behind one trailing refresh", async () => {
    let finish: ((value: number) => void) | undefined;
    const fetcher = vi.fn(
      () =>
        new Promise<number>((resolve) => {
          finish = resolve;
        }),
    );
    const values: number[] = [];
    const gate = createRequestGate(
      fetcher,
      () => undefined,
      values.push.bind(values),
      vi.fn(),
    );

    const first = gate.run();
    void gate.run();
    void gate.run();
    expect(fetcher).toHaveBeenCalledTimes(1);
    finish?.(1);
    await first;
    await Promise.resolve();
    expect(fetcher).toHaveBeenCalledTimes(2);
    finish?.(2);
    await Promise.resolve();
    expect(values).toEqual([1, 2]);
  });

  it("can ignore invalidations while a current projection request is active", async () => {
    let finish: ((value: number) => void) | undefined;
    const fetcher = vi.fn(
      () =>
        new Promise<number>((resolve) => {
          finish = resolve;
        }),
    );
    const gate = createRequestGate(
      fetcher,
      () => undefined,
      () => undefined,
      vi.fn(),
    );

    const first = gate.run({ queueIfActive: false });
    void gate.run({ queueIfActive: false });
    void gate.run({ queueIfActive: false });
    finish?.(1);
    await first;
    await Promise.resolve();

    expect(fetcher).toHaveBeenCalledTimes(1);
  });

  it("aborts an in-flight request when disposed", () => {
    let signal: AbortSignal | undefined;
    const gate = createRequestGate(
      (nextSignal) => {
        signal = nextSignal;
        return new Promise(() => undefined);
      },
      () => undefined,
      () => undefined,
      () => undefined,
    );
    void gate.run();
    gate.abort();
    expect(signal?.aborted).toBe(true);
  });
});
