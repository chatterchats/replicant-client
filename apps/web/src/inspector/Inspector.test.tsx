/* eslint-disable @typescript-eslint/require-await */
/** @vitest-environment jsdom */
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import type { DescriptorCatalog } from "../protocol";
import { Inspector, InspectorView } from "./Inspector";

const descriptors: DescriptorCatalog = {
  reports: [],
  actions: [],
  workflows: [],
};
const callbacks = {
  onClose: vi.fn(),
  onClear: vi.fn(),
  onOpenGalaxy: vi.fn(),
  onOpenSystem: vi.fn(),
  onOpenWorkflow: vi.fn(),
  onRunCommand: vi.fn(),
  onOperationFinished: vi.fn(),
};

function render(
  kind: "device" | "system" | "location" | "workflow" | "event" | "resource",
  value: unknown,
) {
  return renderToStaticMarkup(
    <InspectorView
      props={{
        entity: { kind, id: "TEST" },
        value,
        descriptors,
        entities: {},
        activity: [],
        ...callbacks,
      }}
    />,
  );
}

describe("Inspector extraction", () => {
  it("renders one shell and dispatches every existing value shape", () => {
    const values = [
      [
        "device",
        {
          entity: { kind: "device", id: "D-1" },
          ownership: "public",
          device_type: null,
          status: null,
          owner: null,
          owner_name: null,
          system: null,
          region: null,
          location: null,
          available_commands: [],
          tags: [],
          attached_to: null,
          stowed_in: null,
          controller: null,
          linked_device: null,
          attached_devices: [],
          controlled_devices: [],
          stowed_devices: [],
          attach_capacity: null,
          cargo_capacity: null,
          cargo_used: null,
          operational_capacity_percent: null,
          active_directive: null,
          directive_status: null,
          travel_destination: null,
          claim: null,
        },
      ],
      [
        "workflow",
        {
          id: "TEST",
          kind: "survey",
          status: "running",
          revision: 1,
          updated_at_ms: 0,
          current_step: null,
        },
      ],
      [
        "event",
        {
          designation: "TEST",
          title: "Event",
          system: "SOL",
          location: "SOL-1",
          status: null,
          event_type: null,
          category: null,
          tier: null,
          description: null,
          criteria: [],
          rewards: { resources: [], devices: [], xp: null },
        },
      ],
      [
        "system",
        {
          id: "TEST",
          name: "Sol",
          exploration: "explored",
          spectral_type: "G",
          position: { x: 0, y: 0, z: 0 },
          has_hub: false,
          has_relay: false,
          has_megastructure: false,
          has_life: false,
        },
      ],
      [
        "location",
        {
          id: "TEST",
          label: "Earth",
          location: "TEST",
          kind: "planet",
          position: { x: 0, y: 0 },
          entity: { kind: "location", id: "TEST" },
          parent: null,
          in_habitable_zone: false,
        },
      ],
      [
        "resource",
        {
          entity: { kind: "inventory", id: "TEST" },
          label: "Iron",
          secondary_label: null,
          system: null,
          location: null,
          entity_type: null,
          status: null,
        },
      ],
    ] as const;
    for (const [kind, value] of values) {
      const html = render(kind, value);
      expect(
        html.match(/aria-label="Selected entity inspector"/g),
      ).toHaveLength(1);
    }
  });

  it("shows the absent value message and wires close and clear", async () => {
    callbacks.onClose.mockClear();
    callbacks.onClear.mockClear();
    const container = document.createElement("div");
    const root = createRoot(container);
    await act(async () => {
      root.render(
        <Inspector
          entity={{ kind: "resource", id: "missing" }}
          value={undefined}
          descriptors={descriptors}
          entities={{}}
          activity={[]}
          {...callbacks}
        />,
      );
    });
    expect(container.textContent).toContain(
      "This entity is not present in the current daemon projection.",
    );
    await act(async () => {
      container
        .querySelector<HTMLButtonElement>(
          'button[aria-label="Close inspector"]',
        )
        ?.click();
      container.querySelector<HTMLButtonElement>(".clear-selection")?.click();
    });
    expect(callbacks.onClose).toHaveBeenCalledOnce();
    expect(callbacks.onClear).toHaveBeenCalledOnce();
    await act(async () => {
      root.unmount();
    });
  });
});
