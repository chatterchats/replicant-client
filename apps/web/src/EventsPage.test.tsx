/** @vitest-environment jsdom */
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { EventsContent, eventCommands } from "./EventsPage";
import type { DescriptorCatalog, EventsSnapshot } from "./protocol";

const descriptors: DescriptorCatalog = {
  reports: [
    {
      kind: "event.plan",
      display_name: "Plan event",
      aliases: [],
      description: "Plan fulfillment",
      category: "events",
      operation_class: "report",
      risk: "none",
      applicable_to: ["location"],
      parameters: [],
    },
  ],
  actions: [],
  workflows: [],
};

const snapshot: EventsSnapshot = {
  metadata: { revision: 9, generated_at_ms: 10 },
  events: [
    {
      designation: "SOL-1-EVT-1",
      title: "First contact",
      event_type: "future_type",
      category: "future_category",
      tier: 2,
      system: "SOL",
      location: "SOL-1",
      description: "A newly discovered signal.",
      criteria: [
        {
          name: "supply",
          complete: false,
          requirements: [
            {
              kind: "resource",
              item: "iron",
              required: 10,
              completed: 4,
              remaining: 6,
            },
          ],
        },
      ],
      rewards: {
        resources: [{ item: "water", quantity: 3 }],
        devices: [],
        xp: 5,
        civilisation_points: null,
        completion_achievement: null,
      },
      status: "active",
      discovered_at: null,
      completed_at: null,
    },
  ],
};

const props = {
  refreshing: false,
  refresh: vi.fn(),
  descriptors,
  onSelectEvent: vi.fn(),
  onOpenGalaxy: vi.fn(),
  onOpenSystem: vi.fn(),
  onRunCommand: vi.fn(),
};

describe("EventsContent", () => {
  it("renders structured progress, rewards, navigation, and descriptor actions", () => {
    const html = renderToStaticMarkup(
      <EventsContent {...props} data={snapshot} status="loaded" error={null} />,
    );
    expect(html).toContain("First contact");
    expect(html).toContain("4/10");
    expect(html).toContain("3 water");
    expect(html).toContain("Galaxy");
    expect(html).toContain("System");
    expect(eventCommands(descriptors)[0]?.descriptor.kind).toBe("event.plan");
  });

  it("inspects the full event, not just its location", () => {
    const onSelectEvent = vi.fn();
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    act(() => {
      root.render(
        <EventsContent
          {...props}
          onSelectEvent={onSelectEvent}
          data={snapshot}
          status="loaded"
          error={null}
        />,
      );
    });
    const inspect = Array.from(container.querySelectorAll("button")).find(
      (button) => button.textContent === "Inspect",
    );
    act(() => {
      inspect?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    });
    expect(onSelectEvent).toHaveBeenCalledWith(snapshot.events[0]);
    act(() => {
      root.unmount();
    });
    container.remove();
  });

  it("distinguishes loading, empty, and error states", () => {
    expect(
      renderToStaticMarkup(
        <EventsContent {...props} status="loading" error={null} />,
      ),
    ).toContain("Loading Galaxy Events");
    expect(
      renderToStaticMarkup(
        <EventsContent
          {...props}
          data={{ ...snapshot, events: [] }}
          status="empty"
          error={null}
        />,
      ),
    ).toContain("No discovered events");
    expect(
      renderToStaticMarkup(
        <EventsContent {...props} status="error" error="offline" />,
      ),
    ).toContain("Galaxy Events unavailable");
  });
});
