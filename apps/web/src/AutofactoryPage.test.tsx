import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { AutofactoryContent } from "./AutofactoryPage";
import type { AutofactorySnapshot, DeviceSummary } from "./protocol";

const device: DeviceSummary = {
  entity: { kind: "device", id: "FACTORY-1" },
  device_type: "autofactory",
  status: "active",
  ownership: "owned",
  owner: "R-1",
  owner_name: "Ada",
  system: "SOL",
  region: "solzone",
  location: "SOL-HUB",
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
  operational_capacity_percent: 100,
  active_directive: null,
  directive_status: null,
  travel_destination: null,
  claim: {
    workflow_id: "WF-1",
    workflow_kind: "relay.expansion",
    workflow_status: "running",
  },
};

const snapshot: AutofactorySnapshot = {
  metadata: { revision: 7, generated_at_ms: 10 },
  utilization: {
    total: 1,
    busy: 1,
    available: 0,
    unavailable: 0,
    queued_units: 2,
    utilization_percent: 100,
  },
  factories: [
    {
      device,
      availability: "busy",
      queue_capacity: 4,
      queued_units: 2,
      current_job: {
        device_type: "ftl_relay",
        quantity: 1,
        eta_seconds: 120,
        tags: [],
      },
      queued_jobs: [
        { device_type: "ftl_beacon", quantity: 2, eta_seconds: null, tags: [] },
      ],
    },
  ],
};

describe("AutofactoryContent", () => {
  it("renders aggregate, current, queued, and workflow state", () => {
    const html = renderToStaticMarkup(
      <AutofactoryContent
        data={snapshot}
        status="loaded"
        error={null}
        refreshing={false}
        refresh={vi.fn()}
        descriptors={{ reports: [], actions: [], workflows: [] }}
        onSelectDevice={vi.fn()}
        onSelectEntity={vi.fn()}
        onOpenSystem={vi.fn()}
        onSelectWorkflow={vi.fn()}
        onRunCommand={vi.fn()}
      />,
    );
    expect(html).toContain("100%");
    expect(html).toContain("ftl_relay");
    expect(html).toContain("ftl_beacon");
    expect(html).toContain("relay.expansion");
  });

  it("distinguishes request errors from an empty projection", () => {
    const html = renderToStaticMarkup(
      <AutofactoryContent
        status="error"
        error="offline"
        refreshing={false}
        refresh={vi.fn()}
        descriptors={{ reports: [], actions: [], workflows: [] }}
        onSelectDevice={vi.fn()}
        onSelectEntity={vi.fn()}
        onOpenSystem={vi.fn()}
        onSelectWorkflow={vi.fn()}
        onRunCommand={vi.fn()}
      />,
    );
    expect(html).toContain("Autofactories unavailable");
    expect(html).toContain("offline");
  });
});
