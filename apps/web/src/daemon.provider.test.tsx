// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { daemonApi as DaemonApi } from "./api";
import { daemonApi } from "./api";

import { DaemonProvider, useDaemonState } from "./daemon";
import { useDomainQuery } from "./domainQuery";
import { sharedQueryCache } from "./queryCache";
import type {
  DaemonHealth,
  EntityIndexSnapshot,
  RuntimeSnapshot,
} from "./protocol";

const health: DaemonHealth = {
  status: "healthy",
  daemon_version: "test",
  detail: null,
};

function snapshot(revision: number): RuntimeSnapshot {
  return {
    metadata: { revision, generated_at_ms: revision },
    sync: {
      phase: "ready",
      revision,
      last_event_at_ms: null,
      detail: null,
    },
    automation: {
      automatic_triggers_enabled: true,
      workflows_paused: false,
    },
    workflows: [],
    requirements: [],
    notifications: [],
    refreshes: [],
    slice_revisions: { entities: revision, overview: revision },
  };
}

function entities(revision: number, label: string): EntityIndexSnapshot {
  return {
    metadata: { revision, generated_at_ms: revision },
    entities: [
      {
        entity: { kind: "replicant", id: "R-1" },
        label,
        secondary_label: null,
        system: "SOL",
        location: "EARTH",
        entity_type: null,
        status: "idle",
      },
    ],
  };
}

type ApiModule = {
  daemonApi: typeof DaemonApi;
  daemonToken: () => string | undefined;
  daemonUrl: (path: string, origin?: string) => string;
};

const mockHealth = vi.hoisted(() => vi.fn());
const mockSnapshot = vi.hoisted(() => vi.fn());
const mockEntities = vi.hoisted(() => vi.fn());
const mockOverview = vi.hoisted(() => vi.fn());

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<ApiModule>();
  return {
    ...actual,
    daemonApi: {
      health: mockHealth,
      snapshot: mockSnapshot,
      entities: mockEntities,
      overview: mockOverview,
    } satisfies Partial<typeof actual.daemonApi>,
  };
});

type SocketListener = (event: Event) => void;

class ReconnectWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  static instances: ReconnectWebSocket[] = [];

  readyState = ReconnectWebSocket.CONNECTING;
  private readonly listeners = new Map<string, Set<SocketListener>>();

  constructor(public readonly url: string) {
    ReconnectWebSocket.instances.push(this);
  }

  addEventListener(type: string, listener: SocketListener) {
    const listeners = this.listeners.get(type) ?? new Set<SocketListener>();
    listeners.add(listener);
    this.listeners.set(type, listeners);
  }

  close() {
    if (this.readyState === ReconnectWebSocket.CLOSED) return;
    this.readyState = ReconnectWebSocket.CLOSED;
  }

  open() {
    this.readyState = ReconnectWebSocket.OPEN;
    this.emit("open", new Event("open"));
  }

  disconnect() {
    this.readyState = ReconnectWebSocket.CLOSED;
    this.emit(
      "close",
      new CloseEvent("close", { code: 1006, reason: "test disconnect" }),
    );
  }

  private emit(type: string, event: Event) {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }
}

function StateProbe() {
  const state = useDaemonState();
  return (
    <output data-label={state.entities["replicant:R-1"]?.label ?? "missing"}>
      {state.connection}:{state.revision ?? "none"}
    </output>
  );
}

function OverviewProbe() {
  const query = useDomainQuery({
    slice: "overview",
    queryKey: "startup-overview",
    fetcher: (signal) => daemonApi.overview(signal),
    isEmpty: () => false,
  });
  return <output data-overview-status={query.status} />;
}

async function settle() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

describe("DaemonProvider reconnect bootstrap", () => {
  let root: Root;
  let container: HTMLDivElement;
  let daemonAvailable: boolean;

  beforeEach(() => {
    vi.useFakeTimers();
    vi.spyOn(Math, "random").mockReturnValue(0);
    sharedQueryCache.clear();
    ReconnectWebSocket.instances = [];
    daemonAvailable = true;
    mockHealth.mockReset().mockImplementation(() => {
      if (!daemonAvailable)
        return Promise.reject(new Error("daemon unavailable"));
      return Promise.resolve(health);
    });
    mockSnapshot
      .mockReset()
      .mockResolvedValueOnce(snapshot(1))
      .mockResolvedValueOnce(snapshot(2));
    mockEntities
      .mockReset()
      .mockResolvedValueOnce(entities(1, "before"))
      .mockResolvedValueOnce(entities(2, "after"));
    mockOverview
      .mockReset()
      .mockResolvedValueOnce({
        metadata: { revision: 1, generated_at_ms: 1 },
      })
      .mockResolvedValueOnce({
        metadata: { revision: 2, generated_at_ms: 2 },
      });
    vi.stubGlobal("WebSocket", ReconnectWebSocket);
    container = document.createElement("div");
    document.body.appendChild(container);
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    container.remove();
    sharedQueryCache.clear();
    vi.restoreAllMocks();
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  async function mountProvider() {
    await act(async () => {
      root = createRoot(container);
      root.render(
        <DaemonProvider>
          <StateProbe />
          <OverviewProbe />
        </DaemonProvider>,
      );
      await settle();
    });
  }

  async function advance(ms: number) {
    await act(async () => {
      await vi.advanceTimersByTimeAsync(ms);
      await settle();
    });
  }

  it("keeps startup health-only and bounds probes while the daemon is unavailable", async () => {
    daemonAvailable = false;
    await mountProvider();
    await advance(30_000);

    expect(mockHealth.mock.calls.length).toBeGreaterThan(1);
    expect(mockHealth.mock.calls.length).toBeLessThanOrEqual(12);
    expect(ReconnectWebSocket.instances).toHaveLength(0);
    expect(mockSnapshot).not.toHaveBeenCalled();
    expect(mockEntities).not.toHaveBeenCalled();
    expect(mockOverview).not.toHaveBeenCalled();
    expect(container.querySelector("output")?.dataset.label).toBe("missing");
  });

  it("performs one bootstrap when health recovers after unavailable startup", async () => {
    daemonAvailable = false;
    await mountProvider();
    await advance(30_000);

    daemonAvailable = true;
    await advance(30_000);

    expect(ReconnectWebSocket.instances).toHaveLength(1);
    await act(async () => {
      ReconnectWebSocket.instances[0]?.open();
      await settle();
    });
    expect(mockSnapshot).toHaveBeenCalledTimes(1);
    expect(mockEntities).toHaveBeenCalledTimes(1);
    expect(mockOverview).toHaveBeenCalledTimes(1);
    expect(container.querySelector("output")?.dataset.label).toBe("before");
  });
  it("uses health-only backoff after socket loss and performs one controlled resync", async () => {
    await mountProvider();
    expect(ReconnectWebSocket.instances).toHaveLength(1);
    expect(mockHealth).toHaveBeenCalledTimes(1);
    await act(async () => {
      ReconnectWebSocket.instances[0]?.open();
      await settle();
    });
    expect(mockSnapshot).toHaveBeenCalledTimes(1);
    expect(mockEntities).toHaveBeenCalledTimes(1);

    daemonAvailable = false;
    await act(async () => {
      ReconnectWebSocket.instances[0]?.disconnect();
      await settle();
    });
    await advance(30_000);

    expect(ReconnectWebSocket.instances).toHaveLength(1);
    expect(mockSnapshot).toHaveBeenCalledTimes(1);
    expect(mockEntities).toHaveBeenCalledTimes(1);
    expect(mockOverview).toHaveBeenCalledTimes(1);
    expect(container.querySelector("output")?.dataset.label).toBe("before");

    daemonAvailable = true;
    await advance(30_000);

    expect(ReconnectWebSocket.instances).toHaveLength(2);
    await act(async () => {
      ReconnectWebSocket.instances[1]?.open();
      await settle();
    });
    expect(mockHealth.mock.calls.length).toBeGreaterThan(1);
    expect(mockSnapshot).toHaveBeenCalledTimes(2);
    expect(mockEntities).toHaveBeenCalledTimes(2);
    expect(mockOverview).toHaveBeenCalledTimes(2);
    expect(container.querySelector("output")?.dataset.label).toBe("after");
  });
});
