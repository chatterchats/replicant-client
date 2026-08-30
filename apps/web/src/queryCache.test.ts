import { describe, expect, it, vi } from "vitest";

import { createQueryCache } from "./queryCache";

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

describe("query cache", () => {
  it("shares one entities fetch between two consumers", async () => {
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

  it("coalesces five revisions during one fetch into at most one newest follow-up", async () => {
    const first = deferred<Projection>();
    const followUp = deferred<Projection>();
    const fetcher = vi
      .fn<() => Promise<Projection>>()
      .mockReturnValueOnce(first.promise)
      .mockReturnValueOnce(followUp.promise);
    const cache = createQueryCache();
    const values: string[] = [];
    const subscription = cache.subscribe("entities", fetcher, "1", (event) => {
      if (event.type === "success") values.push(event.data.value);
    });

    subscription.updateRevision("2");
    subscription.updateRevision("3");
    subscription.updateRevision("4");
    subscription.updateRevision("5");
    subscription.updateRevision("6");
    expect(fetcher).toHaveBeenCalledTimes(1);

    first.resolve({ metadata: { revision: 1 }, value: "stale" });
    await flushPromises();
    expect(fetcher).toHaveBeenCalledTimes(2);

    followUp.resolve({ metadata: { revision: 6 }, value: "newest" });
    await flushPromises();

    expect(fetcher).toHaveBeenCalledTimes(2);
    expect(values).toEqual(["newest"]);
    subscription.unsubscribe();
  });

  it("discards a stale response instead of publishing it", async () => {
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

  it("keeps a shared request alive when one consumer unmounts", async () => {
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

  it("aborts page-local work when the final consumer unmounts", () => {
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

  it("recovers from a failed request on a later revision", async () => {
    const failed = deferred<Projection>();
    const recovered = deferred<Projection>();
    const fetcher = vi
      .fn<() => Promise<Projection>>()
      .mockReturnValueOnce(failed.promise)
      .mockReturnValueOnce(recovered.promise);
    const cache = createQueryCache();
    const events: string[] = [];
    const subscription = cache.subscribe("entities", fetcher, "1", (event) =>
      events.push(event.type),
    );

    failed.reject(new Error("temporary failure"));
    await flushPromises();

    expect(events).toEqual(["start", "error"]);
    subscription.updateRevision("2");
    expect(fetcher).toHaveBeenCalledTimes(2);

    recovered.resolve({ metadata: { revision: 2 }, value: "recovered" });
    await flushPromises();

    expect(events).toEqual(["start", "error", "start", "success"]);
    subscription.unsubscribe();
  });
});
