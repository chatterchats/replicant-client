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
  targetRevision: string;
  promise: Promise<void>;
  forceFollowUp: boolean;
};

type Entry = {
  key: string;
  fetcher: QueryFetcher<ProjectionValue>;
  latestRevision: string;
  data?: ProjectionValue;
  request?: RequestState;
  subscribers: Set<Subscriber<ProjectionValue>>;
};

function revisionScore(revision: string): number {
  return revision
    .split(":")
    .reduce((maximum, value) => Math.max(maximum, Number(value) || 0), 0);
}

function revisionIsNewer(next: string, previous: string): boolean {
  if (next === previous) return false;
  return revisionScore(next) >= revisionScore(previous);
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
    if (entry.request) {
      if (force) entry.request.forceFollowUp = true;
      return entry.request.promise;
    }
    if (!force && isFresh(entry.data, entry.latestRevision))
      return Promise.resolve();

    const controller = new AbortController();
    const request: RequestState = {
      controller,
      targetRevision: entry.latestRevision,
      promise: Promise.resolve(),
      forceFollowUp: false,
    };
    let staleResult = false;
    entry.request = request;
    notify(entry, { type: "start", explicit });
    request.promise = entry
      .fetcher(controller.signal)
      .then((data) => {
        if (controller.signal.aborted || entry.request !== request) return;
        if (data.metadata.revision < revisionScore(entry.latestRevision)) {
          staleResult = true;
          // Keep the previous snapshot visible, but ensure the newest revision
          // gets one trailing request after this stale response settles.
          notify(entry, { type: "coalesced" });
          return;
        }
        entry.data = data;
        notify(entry, { type: "success", data });
      })
      .catch((error: unknown) => {
        if (controller.signal.aborted || entry.request !== request) return;
        if (revisionIsNewer(entry.latestRevision, request.targetRevision)) {
          staleResult = true;
          notify(entry, { type: "coalesced" });
          return;
        }
        notify(entry, { type: "error", error });
      })
      .finally(() => {
        if (entry.request !== request) return;
        entry.request = undefined;
        const needsFollowUp =
          entry.subscribers.size > 0 && (request.forceFollowUp || staleResult);
        if (needsFollowUp) void run(entry, false, true);
      });
    return request.promise;
  };

  const update = (entry: Entry, revision: string) => {
    if (!revisionIsNewer(revision, entry.latestRevision)) return;
    entry.latestRevision = revision;
    if (entry.request) {
      recordQueryEvent("coalesced_invalidation", { query: entry.key });
      notify(entry, { type: "coalesced" });
      return;
    }
    void run(entry, false, false);
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
      if (entry.request) {
        recordQueryEvent("joined_request", { query: key });
      }
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
          if (entry.subscribers.size === 0 && entry.request) {
            recordQueryEvent("cancelled_request", { query: key });
            const request = entry.request;
            entry.request = undefined;
            request.controller.abort();
          }
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
      if (revisionIsNewer(revision, entry.latestRevision))
        entry.latestRevision = revision;
    },
    clear() {
      for (const entry of entries.values()) entry.request?.controller.abort();
      entries.clear();
    },
  };
}

export const sharedQueryCache = createQueryCache();
