import { useCallback, useEffect, useRef, useState } from "react";

import { useDaemonState } from "./daemon";
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
  /** Whether another request should run after the active request completes. */
  queueIfActive?: boolean;
}

interface RequestGate {
  run: (options?: RequestRunOptions) => Promise<void>;
  abort: () => void;
}

/** Coalesce noisy managed-state invalidations without making pages feel stale. */
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

export function useDomainQuery<T extends { metadata: SnapshotMetadata }>({
  slice,
  fetcher,
  isEmpty,
}: {
  /** Omit for data with no corresponding live invalidation slice; use `refresh()` instead. */
  slice?: DomainSlice | readonly DomainSlice[];
  fetcher: (signal: AbortSignal) => Promise<T>;
  isEmpty: (value: T) => boolean;
}): DomainQueryResult<T> {
  const invalidated = useDaemonState().invalidated;
  const invalidation = domainInvalidationKey(slice, invalidated);
  const sliceLabel =
    slice === undefined
      ? "manual"
      : typeof slice === "string"
        ? slice
        : slice.join(",");
  const fetcherRef = useRef(fetcher);
  const isEmptyRef = useRef(isEmpty);
  fetcherRef.current = fetcher;
  isEmptyRef.current = isEmpty;
  const [result, setResult] = useState<Omit<DomainQueryResult<T>, "refresh">>({
    data: undefined,
    status: "loading",
    error: null,
    refreshing: false,
    metadata: null,
  });
  const gateRef = useRef<RequestGate | null>(null);
  const explicitRefreshRef = useRef(false);
  const automaticTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const hasAutomaticallyRequestedRef = useRef(false);
  if (!gateRef.current) {
    gateRef.current = createRequestGate(
      async (signal) => {
        const started = performance.now();
        try {
          const value = await fetcherRef.current(signal);
          recordWebEvent(
            "info",
            "frontend.domain_query",
            "frontend domain projection loaded",
            {
              slice: sliceLabel,
              elapsed_ms: Math.round(performance.now() - started),
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
                elapsed_ms: Math.round(performance.now() - started),
                error: String(error).slice(0, 500),
              },
            );
          }
          throw error;
        }
      },
      () => {
        const explicit = explicitRefreshRef.current;
        explicitRefreshRef.current = false;
        setResult((current) => ({
          ...current,
          status: current.data === undefined ? "loading" : current.status,
          error: null,
          // Background invalidation refreshes keep useful stale data visible
          // without turning every page's Refresh button into a strobe.
          refreshing: explicit && current.data !== undefined,
        }));
      },
      (data) => {
        setResult({
          data,
          status: isEmptyRef.current(data) ? "empty" : "loaded",
          error: null,
          refreshing: false,
          metadata: data.metadata,
        });
      },
      (error) => {
        setResult((current) => ({
          ...current,
          status: current.data === undefined ? "error" : current.status,
          error: String(error),
          refreshing: false,
        }));
      },
    );
  }
  const gate = gateRef.current;
  useEffect(
    () => () => {
      if (automaticTimerRef.current !== null)
        clearTimeout(automaticTimerRef.current);
      gate.abort();
    },
    [gate],
  );
  useEffect(() => {
    if (automaticTimerRef.current !== null) return;
    // The first projection fetch must remain immediate. Only subsequent live
    // invalidations are debounced/coalesced.
    const delay = hasAutomaticallyRequestedRef.current
      ? AUTO_INVALIDATION_DELAY_MS
      : 0;
    automaticTimerRef.current = setTimeout(() => {
      automaticTimerRef.current = null;
      hasAutomaticallyRequestedRef.current = true;
      // An in-flight projection is already sampling current managed state, so
      // do not queue another fetch merely because more revisions arrived while
      // it was running. A later invalidation will schedule the next refresh.
      void gate.run({ queueIfActive: false });
    }, delay);
  }, [gate, invalidation]);
  const refresh = useCallback(() => {
    // A manual refresh supersedes any pending background invalidation fetch.
    if (automaticTimerRef.current !== null) {
      clearTimeout(automaticTimerRef.current);
      automaticTimerRef.current = null;
    }
    hasAutomaticallyRequestedRef.current = true;
    explicitRefreshRef.current = true;
    return gate.run();
  }, [gate]);
  return { ...result, refresh };
}
