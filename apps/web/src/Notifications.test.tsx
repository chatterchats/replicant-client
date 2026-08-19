// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { formatNotificationsForClipboard } from "./notificationClipboard";
import { NotificationToasts } from "./Notifications";
import type { Notification } from "./protocol";

const historical: Notification = {
  id: "history",
  level: "warning",
  title: "Historical warning",
  message: "Already present in the daemon snapshot",
  created_at_ms: 1,
};

const fresh: Notification = {
  id: "fresh",
  level: "error",
  title: "Fresh failure",
  message: "Arrived after the initial snapshot",
  created_at_ms: 2,
};

describe("NotificationToasts", () => {
  let root: Root;
  let container: HTMLDivElement;

  beforeEach(() => {
    container = document.createElement("div");
    document.body.appendChild(container);
    root = createRoot(container);
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    container.remove();
  });

  it("does not replay snapshot notifications as new toasts", () => {
    const onSelect = vi.fn();
    const onDismiss = vi.fn();
    act(() => {
      root.render(
        <NotificationToasts
          notifications={[]}
          ready={false}
          onSelect={onSelect}
          onDismiss={onDismiss}
        />,
      );
    });
    act(() => {
      root.render(
        <NotificationToasts
          notifications={[historical]}
          ready
          onSelect={onSelect}
          onDismiss={onDismiss}
        />,
      );
    });
    expect(container.querySelectorAll(".toast")).toHaveLength(0);

    act(() => {
      root.render(
        <NotificationToasts
          notifications={[historical, fresh]}
          ready
          onSelect={onSelect}
          onDismiss={onDismiss}
        />,
      );
    });
    expect(container.querySelectorAll(".toast")).toHaveLength(1);
    expect(container.textContent).toContain("Fresh failure");
    expect(container.textContent).not.toContain("Historical warning");
  });
});

describe("formatNotificationsForClipboard", () => {
  it("copies all current notifications newest first", () => {
    expect(formatNotificationsForClipboard([historical, fresh])).toBe(
      "[1970-01-01T00:00:00.002Z] Error — Fresh failure\n" +
        "Arrived after the initial snapshot\n\n" +
        "[1970-01-01T00:00:00.001Z] Warning — Historical warning\n" +
        "Already present in the daemon snapshot",
    );
  });
});
