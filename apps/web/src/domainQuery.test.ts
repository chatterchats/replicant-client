import { describe, expect, it, vi } from "vitest";

import { createRequestGate, domainInvalidationKey } from "./domainQuery";

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

  it("tracks every listed slice revision independently", () => {
    const slices = ["universe", "devices", "entities"] as const;
    const initial = domainInvalidationKey(slices, {
      universe: 1,
      devices: 2,
      entities: 3,
    });
    expect(
      domainInvalidationKey(slices, {
        universe: 2,
        devices: 2,
        entities: 3,
      }),
    ).not.toBe(initial);
    expect(
      domainInvalidationKey(slices, {
        universe: 1,
        devices: 3,
        entities: 3,
      }),
    ).not.toBe(initial);
    expect(
      domainInvalidationKey(slices, {
        universe: 1,
        devices: 2,
        entities: 4,
      }),
    ).not.toBe(initial);
  });
});
