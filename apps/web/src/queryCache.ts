import { recordQueryEvent } from "./queryTelemetry";

export type QueryCacheEvent<T> =
  | { type: "start"; explicit: boolean; joined?: boolean }
  | { type: "success"; data: T; cached?: boolean }
  | { type: "error"; error: unknown }
  | { type: "coalesced" };

export interface QueryCacheSubscription {
  updateRevision(revision: string): void;
  refresh(): Promise<void>;
  unsubscribe(): void;
}

interface ProjectionValue {
  metadata: { revision: number };
}
type QueryFetcher<T> = (signal: AbortSignal) => Promise<T>;
type QueryListener<T> = (event: QueryCacheEvent<T>) => void;
type Subscriber<T> = { listener: QueryListener<T> };

type RequestState = {
  controller: AbortController;
  initialDataRevision: number;
  promise: Promise<void>;
  /** A refresh always queues another request, even when the first succeeds. */
  forceFollowUp: boolean;
  /** Whether that queued refresh should report an explicit start. */
  forceFollowUpExplicit: boolean;
  /** Newest invalidation observed while this request was active. */
  invalidatedRevision?: string;
};

type Entry = {
  key: string;
  fetcher: QueryFetcher<ProjectionValue>;
  latestRevision: string;
  data?: ProjectionValue;
  request?: RequestState;
  subscribers: Set<Subscriber<ProjectionValue>>;
};

function revisionComponents(revision: string): number[] {
  return revision.split(":").map((value) => {
    const number = Number(value);
    return Number.isFinite(number) ? number : 0;
  });
}

function revisionScore(revision: string): number {
  return revisionComponents(revision).reduce(
    (maximum, value) => Math.max(maximum, value),
    0,
  );
}

function revisionIsNewer(next: string, previous: string): boolean {
  const nextValues = revisionComponents(next);
  const previousValues = revisionComponents(previous);
  return nextValues.some(
    (value, index) => value > (previousValues[index] ?? 0),
  );
}

function mergeRevision(current: string, next: string): string {
  const currentValues = revisionComponents(current);
  const nextValues = revisionComponents(next);
  const length = Math.max(currentValues.length, nextValues.length);
  return Array.from({ length }, (_, index) =>
    String(Math.max(currentValues[index] ?? 0, nextValues[index] ?? 0)),
  ).join(":");
}

function isFresh(data: ProjectionValue | undefined, revision: string): boolean {
  return (
    data !== undefined && data.metadata.revision >= revisionScore(revision)
  );
}

/** Shared projection requests and their newest completed snapshots. */
export interface QueryCache {
  subscribe<T extends { metadata: { revision: number } }>(
    key: string,
    fetcher: QueryFetcher<T>,
    revision: string,
    listener: QueryListener<T>,
  ): QueryCacheSubscription;
  invalidate(key: string, revision: string): void;
  seed(key: string, data: ProjectionValue, revision?: string): void;
  clear(): void;
}

export function createQueryCache(): QueryCache {
  const entries = new Map<string, Entry>();

  const notify = (entry: Entry, event: QueryCacheEvent<ProjectionValue>) => {
    for (const subscriber of [...entry.subscribers]) subscriber.listener(event);
  };

  const run = (
    entry: Entry,
    explicit: boolean,
    force: boolean,
  ): Promise<void> => {
    const current = entry.request;
    if (current) {
      if (force) {
        current.forceFollowUp = true;
        current.forceFollowUpExplicit ||= explicit;
        if (explicit) {
          return current.promise.then(() => {
            const followUp = entry.request;
            return followUp ? followUp.promise : Promise.resolve();
          });
        }
      }
      return current.promise;
    }
    if (!force && isFresh(entry.data, entry.latestRevision))
      return Promise.resolve();

    const controller = new AbortController();
    const request: RequestState = {
      controller,
      initialDataRevision: entry.data?.metadata.revision ?? -Infinity,
      promise: Promise.resolve(),
      forceFollowUp: false,
      forceFollowUpExplicit: false,
    };
    entry.request = request;
    notify(entry, { type: "start", explicit });
    if (entry.request !== request || controller.signal.aborted) {
      request.promise = Promise.resolve();
      return request.promise;
    }

    let fetchPromise: Promise<ProjectionValue>;
    try {
      // Promise.resolve turns a synchronous throw into the same settled path
      // as an asynchronous rejection, so failures cannot strand entry.request.
      fetchPromise = Promise.resolve(entry.fetcher(controller.signal));
    } catch (error: unknown) {
      fetchPromise = Promise.reject(
        error instanceof Error ? error : new Error(String(error)),
      );
    }

    request.promise = fetchPromise
      .then((data) => {
        if (controller.signal.aborted || entry.request !== request) return;

        const requestedRevision = revisionScore(entry.latestRevision);
        const cachedRevision = entry.data?.metadata.revision ?? -Infinity;
        if (
          data.metadata.revision < requestedRevision ||
          data.metadata.revision < cachedRevision
        ) {
          recordQueryEvent("stale_discarded", { query: entry.key });
          return;
        }
        entry.data = data;
        notify(entry, { type: "success", data });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted || entry.request !== request) return;
        const cacheAdvanced =
          (entry.data?.metadata.revision ?? -Infinity) >
          request.initialDataRevision;
        if (
          request.invalidatedRevision !== undefined ||
          (cacheAdvanced && isFresh(entry.data, entry.latestRevision))
        )
          return;
        notify(entry, { type: "error", error });
      })
      .finally(() => {
        if (entry.request !== request) return;
        entry.request = undefined;

        const hasSubscribers = entry.subscribers.size > 0;
        const invalidationNeedsFollowUp =
          request.invalidatedRevision !== undefined &&
          !isFresh(entry.data, entry.latestRevision);
        const needsFollowUp =
          hasSubscribers &&
          (request.forceFollowUp || invalidationNeedsFollowUp);
        if (invalidationNeedsFollowUp)
          recordQueryEvent("auto_refetch", { query: entry.key });
        if (needsFollowUp)
          void run(entry, request.forceFollowUpExplicit, request.forceFollowUp);
      });
    return request.promise;
  };

  const update = (entry: Entry, revision: string) => {
    if (!revisionIsNewer(revision, entry.latestRevision)) return;
    entry.latestRevision = mergeRevision(entry.latestRevision, revision);
    if (entry.request) {
      entry.request.invalidatedRevision = entry.latestRevision;
      recordQueryEvent("coalesced_invalidation", { query: entry.key });
      notify(entry, { type: "coalesced" });
      return;
    }
    if (entry.subscribers.size > 0) {
      recordQueryEvent("auto_refetch", { query: entry.key });
      void run(entry, false, false);
    }
  };

  return {
    subscribe<T extends { metadata: { revision: number } }>(
      key: string,
      fetcher: QueryFetcher<T>,
      revision: string,
      listener: QueryListener<T>,
    ) {
      let entry = entries.get(key);
      if (!entry) {
        entry = {
          key,
          fetcher,
          latestRevision: revision,
          subscribers: new Set(),
        };
        entries.set(key, entry);
      } else {
        entry.fetcher = fetcher;
      }

      const subscriber: Subscriber<ProjectionValue> = {
        listener: (event) => {
          listener(event as QueryCacheEvent<T>);
        },
      };
      entry.subscribers.add(subscriber);

      if (entry.data !== undefined) {
        recordQueryEvent("cache_hit", { query: key });
        subscriber.listener({
          type: "success",
          data: entry.data,
          cached: true,
        });
      }
      if (entry.request) recordQueryEvent("joined_request", { query: key });

      update(entry, revision);
      if (!entry.request && !isFresh(entry.data, entry.latestRevision))
        void run(entry, false, false);

      let active = true;
      return {
        updateRevision(nextRevision: string) {
          if (active) update(entry, nextRevision);
        },
        refresh() {
          if (!active) return Promise.resolve();
          entry.fetcher = fetcher;
          return run(entry, true, true);
        },
        unsubscribe() {
          if (!active) return;
          active = false;
          entry.subscribers.delete(subscriber);
          if (entry.subscribers.size !== 0 || !entry.request) return;

          recordQueryEvent("cancelled_request", { query: key });
          const request = entry.request;
          // Detach before aborting. A new subscriber must not join work that
          // has already been abandoned, and the old promise cannot mutate a
          // newer request because of the identity check in its callbacks.
          entry.request = undefined;
          request.controller.abort();
        },
      } satisfies QueryCacheSubscription;
    },
    invalidate(key, revision) {
      const entry = entries.get(key);
      if (entry) update(entry, revision);
    },
    seed(
      key: string,
      data: ProjectionValue,
      revision = String(data.metadata.revision),
    ) {
      let entry = entries.get(key);
      if (!entry) {
        entry = {
          key,
          fetcher: () => Promise.resolve(data),
          latestRevision: revision,
          subscribers: new Set(),
        };
        entries.set(key, entry);
      }
      if (
        entry.data === undefined ||
        data.metadata.revision >= entry.data.metadata.revision
      ) {
        entry.data = data;
      }
      if (revisionIsNewer(revision, entry.latestRevision)) {
        entry.latestRevision = mergeRevision(entry.latestRevision, revision);
        if (entry.request && !isFresh(entry.data, entry.latestRevision))
          entry.request.invalidatedRevision = entry.latestRevision;
      }
    },
    clear() {
      for (const entry of entries.values()) entry.request?.controller.abort();
      entries.clear();
    },
  };
}

export const sharedQueryCache = createQueryCache();
