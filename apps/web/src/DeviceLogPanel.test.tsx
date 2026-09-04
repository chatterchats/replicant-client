/** @vitest-environment jsdom */
import { act } from "react";
import { createRoot } from "react-dom/client";
import { afterEach, describe, expect, it, vi } from "vitest";

import { daemonApi } from "./api";
import { DeviceLogButton } from "./DeviceLogPanel";
import type { DeviceLogsSnapshot } from "./protocol";

type Deferred<T> = {
  promise: Promise<T>;
  resolve: (value: T) => void;
  reject: (error: unknown) => void;
};
function deferred<T>(): Deferred<T> {
  let resolve!: (value: T) => void;
  let reject!: (error: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function snapshot(
  device: string,
  message = `${device} event`,
): DeviceLogsSnapshot {
  return {
    metadata: { revision: 1, generated_at_ms: 1 },
    device: { kind: "device", id: device },
    events: [
      {
        id: 1,
        created_at: "2026-09-03T12:00:00Z",
        device_code: device,
        device_type: "mining_drone",
        event_type: "test.event",
        message,
        payload: {},
      },
    ],
    next_cursor: null,
  };
}

type Request = {
  device: string;
  signal: AbortSignal | undefined;
  deferred: Deferred<DeviceLogsSnapshot>;
};

function installDeviceLogMock(requests: Request[]) {
  return vi
    .spyOn(daemonApi, "deviceLogs")
    .mockImplementation((device, signal) => {
      const pending = deferred<DeviceLogsSnapshot>();
      requests.push({ device, signal, deferred: pending });
      return pending.promise;
    });
}
function requestAt(requests: Request[], index: number): Request {
  const request = requests[index];
  if (!request) throw new Error(`Missing device-log request ${String(index)}`);
  return request;
}

function render(device = "D-1") {
  const container = document.createElement("div");
  document.body.appendChild(container);
  const root = createRoot(container);
  act(() => {
    root.render(<DeviceLogButton device={device} />);
  });
  return { container, root };
}

async function openLogs(container: HTMLElement) {
  await act(async () => {
    const button = [...container.querySelectorAll("button")].find(
      (candidate) => candidate.textContent === "Device log",
    );
    button?.click();
    await Promise.resolve();
  });
}

async function settle<T>(pending: Deferred<T>, value: T) {
  await act(async () => {
    pending.resolve(value);
    await pending.promise;
    await Promise.resolve();
  });
}

afterEach(() => {
  vi.restoreAllMocks();
  document.body.replaceChildren();
});

describe("DeviceLogButton", () => {
  it("fetches once when the dialog opens", async () => {
    const requests: Request[] = [];
    installDeviceLogMock(requests);
    const { container, root } = render();

    expect(requests).toHaveLength(0);
    await openLogs(container);
    expect(requests).toHaveLength(1);
    expect(requests[0]?.device).toBe("D-1");

    await settle(requestAt(requests, 0).deferred, snapshot("D-1"));
    expect(container.textContent).toContain("D-1 event");
    root.unmount();
  });

  it("does not refetch for an unrelated rerender", async () => {
    const requests: Request[] = [];
    installDeviceLogMock(requests);
    const { container, root } = render();
    await openLogs(container);
    await settle(requestAt(requests, 0).deferred, snapshot("D-1"));

    await act(async () => {
      root.render(<DeviceLogButton device="D-1" />);
      await Promise.resolve();
    });
    expect(requests).toHaveLength(1);
    root.unmount();
  });

  it("fetches exactly once for an explicit Refresh", async () => {
    const requests: Request[] = [];
    installDeviceLogMock(requests);
    const { container, root } = render();
    await openLogs(container);
    await settle(
      requestAt(requests, 0).deferred,
      snapshot("D-1", "first event"),
    );

    await act(async () => {
      const button = [...container.querySelectorAll("button")].find(
        (candidate) => candidate.textContent === "Refresh",
      );
      button?.click();
      await Promise.resolve();
    });
    expect(requests).toHaveLength(2);
    expect(requests[1]?.device).toBe("D-1");
    await settle(
      requestAt(requests, 1).deferred,
      snapshot("D-1", "refreshed event"),
    );
    expect(container.textContent).toContain("refreshed event");
    root.unmount();
  });

  it("aborts the old request and fetches when the selected device changes", async () => {
    const requests: Request[] = [];
    installDeviceLogMock(requests);
    const { container, root } = render("D-1");
    await openLogs(container);

    await act(async () => {
      root.render(<DeviceLogButton device="D-2" />);
      await Promise.resolve();
    });
    expect(requests).toHaveLength(2);
    expect(requests[0]?.signal?.aborted).toBe(true);
    expect(requests[1]?.device).toBe("D-2");

    await settle(
      requestAt(requests, 0).deferred,
      snapshot("D-1", "stale event"),
    );
    await settle(requestAt(requests, 1).deferred, snapshot("D-2", "new event"));
    expect(container.textContent).toContain("new event");
    expect(container.textContent).not.toContain("stale event");
    root.unmount();
  });

  it("aborts an in-flight request when the dialog unmounts", async () => {
    const requests: Request[] = [];
    installDeviceLogMock(requests);
    const { container, root } = render();
    await openLogs(container);
    expect(requests).toHaveLength(1);

    await act(async () => {
      root.unmount();
      await Promise.resolve();
    });
    expect(requests[0]?.signal?.aborted).toBe(true);
  });
});
