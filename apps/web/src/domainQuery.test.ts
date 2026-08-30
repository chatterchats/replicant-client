import { describe, expect, it, vi } from "vitest";

import { createQueryCache } from "./queryCache";
import { createRequestGate, domainInvalidationKey } from "./domainQuery";

type Projection = {
  metadata: { revision: number };
  value: string;
};

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function flushPromises() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("domain query cache", () => {
  it("shares one fetch between two subscribers", async () => {
    const pending = deferred<Projection>();
    const fetcher = vi.fn(() => pending.promise);
    const cache = createQueryCache();
    const firstEvents: string[] = [];
    const secondEvents: string[] = [];

    const first = cache.subscribe("entities", fetcher, "1", (event) =>
      firstEvents.push(event.type),
    );
    const second = cache.subscribe("entities", fetcher, "1", (event) =>
      secondEvents.push(event.type),
    );

    expect(fetcher).toHaveBeenCalledTimes(1);
    expect(firstEvents).toEqual(["start"]);
    expect(secondEvents).toEqual([]);

    pending.resolve({ metadata: { revision: 1 }, value: "shared" });
    await flushPromises();

    expect(firstEvents).toEqual(["start", "success"]);
    expect(secondEvents).toEqual(["success"]);
    first.unsubscribe();
    second.unsubscribe();
  });

  it("coalesces revision bursts into at most one newest follow-up", async () => {
    const first = deferred<Projection>();
    const second = deferred<Projection>();
    const fetcher = vi
      .fn<() => Promise<Projection>>()
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(second.promise);
    const cache = createQueryCache();
    const events: string[] = [];
    const subscription = cache.subscribe("entities", fetcher, "1", (event) => {
      if (event.type === "success") events.push(event.data.value);
    });

    subscription.updateRevision("2");
    subscription.updateRevision("3");
    subscription.updateRevision("4");
    expect(fetcher).toHaveBeenCalledTimes(1);

    first.resolve({ metadata: { revision: 1 }, value: "first" });
    await flushPromises();
    expect(fetcher).toHaveBeenCalledTimes(2);

    second.resolve({ metadata: { revision: 4 }, value: "newest" });
    await flushPromises();
    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(events).toEqual(["newest"]);
    subscription.unsubscribe();
  });

  it("suppresses a result from a revision older than the latest request", async () => {
    const pending = deferred<Projection>();
    const followUp = deferred<Projection>();
    const fetcher = vi
      .fn<() => Promise<Projection>>()
      .mockReturnValueOnce(pending.promise)
      .mockReturnValueOnce(followUp.promise);
    const cache = createQueryCache();
    const values: string[] = [];
    const subscription = cache.subscribe("entities", fetcher, "1", (event) => {
      if (event.type === "success") values.push(event.data.value);
    });

    subscription.updateRevision("2");
    pending.resolve({ metadata: { revision: 1 }, value: "stale" });
    await flushPromises();

    expect(values).toEqual([]);
    expect(fetcher).toHaveBeenCalledTimes(2);
    followUp.resolve({ metadata: { revision: 2 }, value: "fresh" });
    await flushPromises();
    expect(values).toEqual(["fresh"]);
    subscription.unsubscribe();
  });

  it("keeps shared work alive when one subscriber leaves", async () => {
    const pending = deferred<Projection>();
    const fetcher = vi.fn((signal: AbortSignal) => {
      void signal;
      return pending.promise;
    });
    const cache = createQueryCache();
    const first = cache.subscribe("entities", fetcher, "1", vi.fn());
    const secondEvents: string[] = [];
    const second = cache.subscribe("entities", fetcher, "1", (event) =>
      secondEvents.push(event.type),
    );

    const signal = fetcher.mock.calls[0]?.[0];
    first.unsubscribe();
    expect(signal?.aborted).toBe(false);

    pending.resolve({ metadata: { revision: 1 }, value: "survived" });
    await flushPromises();
    expect(secondEvents).toContain("success");
    second.unsubscribe();
  });

  it("cancels page-only work when its last subscriber leaves", () => {
    const pending = deferred<Projection>();
    const fetcher = vi.fn((signal: AbortSignal) => {
      void signal;
      return pending.promise;
    });
    const cache = createQueryCache();
    const subscription = cache.subscribe("entities", fetcher, "1", vi.fn());
    const signal = fetcher.mock.calls[0]?.[0];

    subscription.unsubscribe();

    expect(signal?.aborted).toBe(true);
  });

  it("emits loading immediately before the initial projection resolves", () => {
    const pending = deferred<Projection>();
    const fetcher = vi.fn(() => pending.promise);
    const cache = createQueryCache();
    const events: string[] = [];

    const subscription = cache.subscribe("entities", fetcher, "1", (event) =>
      events.push(event.type),
    );

    expect(events).toEqual(["start"]);
    expect(fetcher).toHaveBeenCalledTimes(1);
    subscription.unsubscribe();
  });
});

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
