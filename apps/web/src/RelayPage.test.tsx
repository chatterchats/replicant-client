import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { RelayContent, relayCommands } from "./RelayPage";
import type { DescriptorCatalog, RelaySnapshot } from "./protocol";

const descriptors: DescriptorCatalog = {
  reports: [],
  actions: [],
  workflows: [
    {
      kind: "relay.expansion",
      display_name: "Relay expansion",
      aliases: [],
      description: "Expand relays",
      category: "relay",
      operation_class: "workflow",
      risk: "elevated",
      applicable_to: ["system"],
      parameters: [],
      supported_triggers: [],
    },
  ],
};

const relay = {
  entity: { kind: "device" as const, id: "RELAY-1" },
  device_type: "ftl_relay",
  status: "active",
  ownership: "owned",
  owner: "R-1",
  owner_name: "Ada",
  system: "SOL",
  location: "SOL-1",
  tags: ["network"],
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
  operational_capacity_percent: 100,
  active_directive: null,
  directive_status: null,
  travel_destination: null,
  claim: null,
};

const snapshot: RelaySnapshot = {
  metadata: { revision: 7, generated_at_ms: 10 },
  relays: [relay],
  staged_relays: [],
  connected_systems: 2,
  relay_edges: [{ from: "SOL", to: "VEGA" }],
  expansions: [
    {
      workflow: {
        id: "WF-RELAY",
        kind: "relay.expansion",
        status: "running",
        current_step: "deploying",
        revision: 2,
        updated_at_ms: 10,
      },
      replicant: "R-1",
      hub: "SOL-1",
      targets: ["VEGA"],
      phase: "deploying",
      completed_stops: 1,
      total_stops: 2,
      next_system: "VEGA",
      pending_relays: 0,
    },
  ],
};

describe("RelayContent", () => {
  it("renders coverage, route progress, Galaxy and workflow links", () => {
    const html = renderToStaticMarkup(
      <RelayContent
        data={snapshot}
        status="loaded"
        error={null}
        refreshing={false}
        refresh={vi.fn()}
        descriptors={descriptors}
        onSelectEntity={vi.fn()}
        onOpenGalaxy={vi.fn()}
        onSelectWorkflow={vi.fn()}
        onRunCommand={vi.fn()}
      />,
    );
    expect(html).toContain("RELAY-1");
    expect(html).toContain("1 / 2");
    expect(html).toContain("Show on Galaxy");
    expect(html).toContain("Open workflow");
    expect(relayCommands(descriptors)[0]?.operationClass).toBe("workflow");
  });

  it("distinguishes an empty projection from failure", () => {
    const html = renderToStaticMarkup(
      <RelayContent
        data={{ ...snapshot, relays: [], expansions: [], relay_edges: [] }}
        status="empty"
        error={null}
        refreshing={false}
        refresh={vi.fn()}
        descriptors={descriptors}
        onSelectEntity={vi.fn()}
        onOpenGalaxy={vi.fn()}
        onSelectWorkflow={vi.fn()}
        onRunCommand={vi.fn()}
      />,
    );
    expect(html).toContain("No owned relay devices discovered");
  });
});
