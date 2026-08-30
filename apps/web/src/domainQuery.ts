import { useCallback, useEffect, useRef, useState } from "react";

import { useDaemonState } from "./daemon";
import { sharedQueryCache } from "./queryCache";
import type { QueryCacheSubscription } from "./queryCache";
import type { DomainSlice, SnapshotMetadata } from "./protocol";
import { recordWebEvent } from "./telemetry";

export type DomainQueryStatus = "loading" | "error" | "empty" | "loaded";

export interface DomainQueryResult<T> {
  data: T | undefined;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  metadata: SnapshotMetadata | null;
  refresh: () => Promise<void>;
}

interface RequestRunOptions {
  queueIfActive?: boolean;
}
interface RequestGate {
  run: (options?: RequestRunOptions) => Promise<void>;
  abort: () => void;
}
const AUTO_INVALIDATION_DELAY_MS = 1_500;

export function createRequestGate<T>(
  fetcher: (signal: AbortSignal) => Promise<T>,
  onStart: () => void,
  onSuccess: (value: T) => void,
  onError: (error: unknown) => void,
): RequestGate {
  let active: Promise<void> | undefined;
  let controller: AbortController | undefined;
  let queued = false;
  let disposed = false;
  const run = (options: RequestRunOptions = {}): Promise<void> => {
    if (disposed) return Promise.resolve();
    if (active) {
      if (options.queueIfActive ?? true) queued = true;
      return active;
    }
    controller = new AbortController();
    const current = controller;
    onStart();
    active = fetcher(current.signal)
      .then((value) => {
        if (!current.signal.aborted) onSuccess(value);
      })
      .catch((error: unknown) => {
        if (!current.signal.aborted) onError(error);
      })
      .finally(() => {
        active = undefined;
        controller = undefined;
        if (queued && !disposed) {
          queued = false;
          void run();
        }
      });
    return active;
  };
  return {
    run,
    abort() {
      disposed = true;
      queued = false;
      controller?.abort();
    },
  };
}

export function domainInvalidationKey(
  slice: DomainSlice | readonly DomainSlice[] | undefined,
  invalidated: Partial<Record<DomainSlice, number>>,
): string {
  const slices: readonly DomainSlice[] =
    slice === undefined ? [] : typeof slice === "string" ? [slice] : slice;
  return slices.map((item) => invalidated[item]).join(":");
}

function sliceName(
  slice: DomainSlice | readonly DomainSlice[] | undefined,
): string {
  return slice === undefined
    ? "manual"
    : typeof slice === "string"
      ? slice
      : slice.join(",");
}

let nextHookQueryIdentity = 1;

function nextDefaultQueryKey(
  slice: DomainSlice | readonly DomainSlice[] | undefined,
): string {
  const identity = nextHookQueryIdentity++;
  return `${sliceName(slice)}:hook-${String(identity)}`;
}

export function useDomainQuery<T extends { metadata: SnapshotMetadata }>({
  slice,
  queryKey,
  fetcher,
  isEmpty,
}: {
  slice?: DomainSlice | readonly DomainSlice[];
  queryKey?: string;
  fetcher: (signal: AbortSignal) => Promise<T>;
  isEmpty: (value: T) => boolean;
}): DomainQueryResult<T> {
  const daemon = useDaemonState();
  const invalidated = daemon.invalidated;
  const queryEnabled =
    daemon.connection === "connected" &&
    daemon.revision !== null &&
    daemon.error === null;
  const invalidation = domainInvalidationKey(slice, invalidated);
  const sliceLabel = sliceName(slice);
  const fetcherRef = useRef(fetcher);
  const isEmptyRef = useRef(isEmpty);
  const defaultKeyRef = useRef<string | null>(null);
  fetcherRef.current = fetcher;
  isEmptyRef.current = isEmpty;
  defaultKeyRef.current ??= nextDefaultQueryKey(slice);
  const cacheKey = queryKey ?? defaultKeyRef.current;
  const [result, setResult] = useState<Omit<DomainQueryResult<T>, "refresh">>({
    data: undefined,
    status: "loading",
    error: null,
    refreshing: false,
    metadata: null,
  });
  const subscriptionRef = useRef<QueryCacheSubscription | null>(null);
  const automaticTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const invalidationRef = useRef(invalidation);
  invalidationRef.current = invalidation;
  const hasAutomaticallyRequestedRef = useRef(false);

  useEffect(() => {
    if (!queryEnabled) return;
    const subscription = sharedQueryCache.subscribe(
      cacheKey,
      async (signal) => {
        const requestStarted = performance.now();
        try {
          const value = await fetcherRef.current(signal);
          recordWebEvent(
            "debug",
            "frontend.domain_query",
            "frontend domain projection loaded",
            {
              slice: sliceLabel,
              elapsed_ms: Math.round(performance.now() - requestStarted),
              revision: value.metadata.revision,
            },
          );
          return value;
        } catch (error) {
          if (!signal.aborted) {
            recordWebEvent(
              "error",
              "frontend.domain_query_failed",
              "frontend domain projection failed",
              {
                slice: sliceLabel,
                elapsed_ms: Math.round(performance.now() - requestStarted),
                error: String(error).slice(0, 500),
              },
            );
          }
          throw error;
        }
      },
      invalidationRef.current,
      (event) => {
        if (event.type === "start") {
          setResult((current) => ({
            ...current,
            status: current.data === undefined ? "loading" : current.status,
            error: null,
            refreshing: event.explicit && current.data !== undefined,
          }));
        } else if (event.type === "success") {
          setResult({
            data: event.data,
            status: isEmptyRef.current(event.data) ? "empty" : "loaded",
            error: null,
            refreshing: false,
            metadata: event.data.metadata,
          });
        } else if (event.type === "error") {
          setResult((current) => ({
            ...current,
            status: current.data === undefined ? "error" : current.status,
            error: String(event.error),
            refreshing: false,
          }));
        }
      },
    );
    subscriptionRef.current = subscription;
    return () => {
      subscription.unsubscribe();
      if (subscriptionRef.current === subscription)
        subscriptionRef.current = null;
    };
  }, [cacheKey, queryEnabled, sliceLabel]);

  useEffect(() => {
    if (!queryEnabled) return;
    if (automaticTimerRef.current !== null) return;
    const delay = hasAutomaticallyRequestedRef.current
      ? AUTO_INVALIDATION_DELAY_MS
      : 0;
    automaticTimerRef.current = setTimeout(() => {
      automaticTimerRef.current = null;
      hasAutomaticallyRequestedRef.current = true;
      subscriptionRef.current?.updateRevision(invalidation);
    }, delay);
    return () => {
      if (automaticTimerRef.current !== null) {
        clearTimeout(automaticTimerRef.current);
        automaticTimerRef.current = null;
      }
    };
  }, [invalidation, queryEnabled]);

  const refresh = useCallback(() => {
    if (!queryEnabled) return Promise.resolve();
    if (automaticTimerRef.current !== null) {
      clearTimeout(automaticTimerRef.current);
      automaticTimerRef.current = null;
    }
    hasAutomaticallyRequestedRef.current = true;
    return subscriptionRef.current?.refresh() ?? Promise.resolve();
  }, [queryEnabled]);
  return { ...result, refresh };
}
