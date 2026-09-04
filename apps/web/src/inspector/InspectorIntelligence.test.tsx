import { describe, expect, it } from "vitest";

import type {
  EntityInspectorSnapshot,
  RequirementSummary,
  WorkflowIntelligenceSnapshot,
} from "../protocol";
import { associatedWorkflowIds } from "./InspectorIntelligence";

const requirement = (
  overrides: Partial<RequirementSummary> = {},
): RequirementSummary => ({
  id: "REQ-1",
  name: "Keep structural available",
  target: "available structural",
  scope: "location HUB-1",
  desired: 100,
  actual: 50,
  in_progress: 25,
  missing: 25,
  workflow_id: "WF-REQ",
  status: "running",
  ...overrides,
});

const intelligence: WorkflowIntelligenceSnapshot = {
  metadata: { revision: 1, generated_at_ms: 1 },
  reservations: [],
  targets: [
    {
      workflow_id: "WF-EVENT",
      kind: "event",
      key: "EVT-42",
      system: "THYFFAWFF",
      location: "THYFFAWFF-3-L4",
      active: true,
      created_at_ms: 1,
      updated_at_ms: 2,
    },
  ],
};

describe("associatedWorkflowIds", () => {
  it("associates exact desired-state scopes and resources", () => {
    const requirements = [requirement()];
    expect(
      associatedWorkflowIds(
        { kind: "location", id: "HUB-1" },
        undefined,
        requirements,
      ),
    ).toEqual(["WF-REQ"]);
    expect(
      associatedWorkflowIds(
        { kind: "resource", id: "structural" },
        undefined,
        requirements,
      ),
    ).toEqual(["WF-REQ"]);
    expect(
      associatedWorkflowIds(
        { kind: "location", id: "HUB-10" },
        undefined,
        requirements,
      ),
    ).toEqual([]);
  });

  it("uses exact structured event targets for workflow ownership", () => {
    expect(
      associatedWorkflowIds(
        { kind: "event", id: "EVT-42" },
        undefined,
        [],
        intelligence,
      ),
    ).toEqual(["WF-EVENT"]);
    expect(
      associatedWorkflowIds(
        { kind: "event", id: "EVT-420" },
        undefined,
        [],
        intelligence,
      ),
    ).toEqual([]);
  });

  it("uses authoritative device claims without guessing from labels", () => {
    const snapshot = {
      detail: {
        kind: "device",
        detail: {
          device: {
            claim: {
              workflow_id: "WF-DEVICE",
              workflow_kind: "scan.tour",
              workflow_status: "running",
            },
          },
        },
      },
    } as unknown as EntityInspectorSnapshot;
    expect(
      associatedWorkflowIds({ kind: "device", id: "DEVICE-1" }, snapshot, []),
    ).toEqual(["WF-DEVICE"]);
  });
});
