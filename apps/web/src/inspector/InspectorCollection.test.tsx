/* eslint-disable @typescript-eslint/require-await, @typescript-eslint/restrict-template-expressions */
/** @vitest-environment jsdom */
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { EntityCollectionSummary } from "../protocol";
import { InspectorCollection } from "./InspectorCollection";

const items = (count: number) =>
  Array.from({ length: count }, (_, index) => ({
    entity: { kind: "location" as const, id: `L-${index}` },
    label: `Location ${index}`,
    secondary_label: "planet",
    system: "SOL",
    location: `L-${index}`,
    entity_type: "planet",
    status: index % 2 ? "scanned" : null,
  }));
const grouped = (total: number): EntityCollectionSummary => ({
  total,
  items: [],
  groups: [
    {
      entity_kind: "device",
      entity_type: "mining_drone",
      count: total,
      statuses: [{ status: "idle", count: total }],
    },
  ],
});

describe("InspectorCollection", () => {
  it("keeps eight children inline", () => {
    const html = renderToStaticMarkup(
      <InspectorCollection
        collection={{ total: 8, items: items(8), groups: [] }}
      />,
    );
    expect(html.match(/<li/g)).toHaveLength(8);
    expect(html).not.toContain("<details");
  });

  it("renders nine and 393 item projections only as counted groups", async () => {
    for (const total of [9, 393]) {
      const html = renderToStaticMarkup(
        <InspectorCollection collection={grouped(total)} />,
      );
      expect(html).toContain("<details");
      expect(html).toContain(total.toString());
      expect(html).not.toContain("Location 0");
    }
    const container = document.createElement("div");
    const root = createRoot(container);
    await act(async () => {
      root.render(<InspectorCollection collection={grouped(393)} />);
    });
    const input = container.querySelector("input");
    await act(async () => {
      if (input) {
        Object.getOwnPropertyDescriptor(
          HTMLInputElement.prototype,
          "value",
        )?.set?.call(input, "planet");
        input.dispatchEvent(new Event("input", { bubbles: true }));
      }
    });
    expect(container.querySelectorAll("details")).toHaveLength(0);
    await act(async () => {
      root.unmount();
    });
  });
});
