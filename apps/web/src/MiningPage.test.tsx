import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { MiningContent, miningCommands } from "./MiningPage";
import type { DescriptorCatalog, MiningSnapshot } from "./protocol";

const descriptors: DescriptorCatalog = {
  reports: [],
  actions: [
    {
      kind: "mining.plan",
      display_name: "Plan mining expansion",
      aliases: [],
      description: "Plan expansion",
      category: "mining",
      operation_class: "action",
      risk: "elevated",
      applicable_to: ["system"],
      parameters: [],
    },
  ],
  workflows: [],
};

const snapshot: MiningSnapshot = {
  metadata: { revision: 8, generated_at_ms: 10 },
  workflows: [
    {
      id: "WF-MINING",
      kind: "mining.expansion",
      status: "running",
      current_step: "deploying",
      revision: 3,
      updated_at_ms: 10,
    },
  ],
  installations: [
    {
      id: "SOL/SOL-BELT",
      system: "SOL",
      location: "SOL-BELT",
      controller: null,
      miners: [],
      survey_controller: null,
      survey_drones: [],
      maintenance_device: null,
      missing: ["mining controller", "4 adopted mining drones"],
      status: "partial",
    },
  ],
};

describe("MiningContent", () => {
  it("renders partial installations, workflow links, and descriptor starts", () => {
    const run = vi.fn();
    const openWorkflow = vi.fn();
    const element = (
      <MiningContent
        data={snapshot}
        status="loaded"
        error={null}
        refreshing={false}
        refresh={vi.fn()}
        descriptors={descriptors}
        onSelectEntity={vi.fn()}
        onOpenGalaxy={vi.fn()}
        onSelectWorkflow={openWorkflow}
        onRunCommand={run}
      />
    );
    const html = renderToStaticMarkup(element);
    expect(html).toContain("partial");
    expect(html).toContain("mining controller");
    expect(html).toContain("Plan mining expansion");
    expect(html).toContain("mining.expansion · running");
    const command = miningCommands(descriptors)[0];
    expect(command?.operationClass).toBe("action");
    if (command) run(command);
    expect(run).toHaveBeenCalledWith(command);
  });
});
