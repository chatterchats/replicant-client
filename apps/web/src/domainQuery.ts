import { useCallback, useEffect, useRef, useState } from "react";

import { useDaemonState } from "./daemon";
import type { DomainSlice, SnapshotMetadata } from "./protocol";

export type DomainQueryStatus = "loading" | "error" | "empty" | "loaded";

export interface DomainQueryResult<T> {
  data: T | undefined;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  metadata: SnapshotMetadata | null;
  refresh: () => Promise<void>;
}

interface RequestGate {
  run: () => Promise<void>;
  abort: () => void;
}

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

  const run = (): Promise<void> => {
    if (disposed) return Promise.resolve();
    if (active) {
      queued = true;
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

export function useDomainQuery<T extends { metadata: SnapshotMetadata }>({
  slice,
  fetcher,
  isEmpty,
}: {
  /** Omit for data with no corresponding live invalidation slice; use `refresh()` instead. */
  slice?: DomainSlice;
  fetcher: (signal: AbortSignal) => Promise<T>;
  isEmpty: (value: T) => boolean;
}): DomainQueryResult<T> {
  const invalidated = useDaemonState().invalidated;
  const invalidation = slice ? invalidated[slice] : undefined;
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
  if (!gateRef.current) {
    gateRef.current = createRequestGate(
      (signal) => fetcherRef.current(signal),
      () => {
        setResult((current) => ({
          ...current,
          status: current.data === undefined ? "loading" : current.status,
          error: null,
          refreshing: current.data !== undefined,
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
      gate.abort();
    },
    [gate],
  );
  useEffect(() => {
    void gate.run();
  }, [gate, invalidation]);
  const refresh = useCallback(() => gate.run(), [gate]);
  return { ...result, refresh };
}
