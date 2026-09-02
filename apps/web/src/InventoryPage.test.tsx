import {
  Children,
  isValidElement,
  type ReactElement,
  type ReactNode,
} from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import {
  filterInventoryLocations,
  filterInventoryResources,
  InventoryContent,
  InventoryViewTabs,
} from "./InventoryPage";
import type { InventorySnapshot } from "./protocol";

const snapshot: InventorySnapshot = {
  metadata: { revision: 7, generated_at_ms: 10 },
  total_quantity: 17,
  locations: [
    {
      owner_kind: "location",
      owner: "EARTH",
      system: "SOL",
      location: "EARTH",
      total_quantity: 12,
      resources: [
        { resource: "conductive", quantity: 2 },
        { resource: "silicates", quantity: 10 },
      ],
    },
    {
      owner_kind: "replicant",
      owner: "R-1",
      system: "VEGA",
      location: "VEGA-2",
      total_quantity: 5,
      resources: [{ resource: "silicates", quantity: 5 }],
    },
  ],
  resources: [
    {
      resource: "conductive",
      total_quantity: 2,
      distribution: [
        {
          owner_kind: "location",
          owner: "EARTH",
          system: "SOL",
          location: "EARTH",
          quantity: 2,
        },
      ],
    },
    {
      resource: "silicates",
      total_quantity: 15,
      distribution: [
        {
          owner_kind: "location",
          owner: "EARTH",
          system: "SOL",
          location: "EARTH",
          quantity: 10,
        },
        {
          owner_kind: "replicant",
          owner: "R-1",
          system: "VEGA",
          location: "VEGA-2",
          quantity: 5,
        },
      ],
    },
  ],
};

const content = (
  overrides: Partial<Parameters<typeof InventoryContent>[0]> = {},
) =>
  renderToStaticMarkup(
    <InventoryContent
      data={snapshot}
      status="loaded"
      error={null}
      refreshing={false}
      refresh={vi.fn()}
      onSelectEntity={vi.fn()}
      onOpenSystem={vi.fn()}
      {...overrides}
    />,
  );

describe("inventory explorer", () => {
  it("filters locations and resources and sorts quantity deterministically", () => {
    expect(
      filterInventoryLocations([...snapshot.locations].reverse(), "").map(
        (row) => row.owner,
      ),
    ).toEqual(["EARTH", "R-1"]);
    expect(
      filterInventoryLocations(snapshot.locations, "vega").map(
        (row) => row.owner,
      ),
    ).toEqual(["R-1"]);
    expect(
      filterInventoryLocations(snapshot.locations, "conductive").map(
        (row) => row.owner,
      ),
    ).toEqual(["EARTH"]);
    expect(
      filterInventoryResources(snapshot.resources, "", true).map(
        (row) => row.resource,
      ),
    ).toEqual(["silicates", "conductive"]);
    expect(
      filterInventoryResources(snapshot.resources, "sil", false).map(
        (row) => row.resource,
      ),
    ).toEqual(["silicates"]);
  });

  it("switches between location and resource tabs", () => {
    const onChange = vi.fn();
    const tabs = InventoryViewTabs({
      mode: "location",
      onChange,
    }) as ReactElement<{
      children: ReactNode;
    }>;
    const buttons = Children.toArray(tabs.props.children).filter(
      (
        child,
      ): child is ReactElement<{ onClick: () => void; children: ReactNode }> =>
        isValidElement(child),
    );
    buttons[1]?.props.onClick();
    expect(onChange).toHaveBeenCalledWith("resource");
    expect(renderToStaticMarkup(tabs)).toContain('aria-selected="true"');
  });

  it("renders loaded, loading, error, and empty states distinctly", () => {
    expect(content()).toContain("By Location");
    expect(content({ data: undefined, status: "loading" })).toContain(
      "Loading inventory",
    );
    expect(
      content({ data: undefined, status: "error", error: "offline" }),
    ).toContain("Inventory unavailable");
    expect(
      content({
        data: { ...snapshot, total_quantity: 0, locations: [], resources: [] },
        status: "empty",
      }),
    ).toContain("No positive managed inventory");
  });
});
