import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { BootstrapContent, bootstrapCommands } from "./BootstrapPage";
import type { BootstrapSnapshot, DescriptorCatalog } from "./protocol";

const descriptors: DescriptorCatalog = {
  reports: [],
  workflows: [],
  actions: [
    {
      kind: "bootstrap.run",
      display_name: "Run regional bootstrap",
      aliases: [],
      description: "Resume bootstrap",
      category: "bootstrap",
      operation_class: "action",
      risk: "elevated",
      applicable_to: ["system"],
      parameters: [],
    },
  ],
};

const snapshot: BootstrapSnapshot = {
  metadata: { revision: 9, generated_at_ms: 10 },
  missions: [
    {
      mission_id: "BOOT-ACTIVE",
      execution_id: "EXEC-1",
      region: "beta",
      source_hub: "SOL-1",
      target_system: "VEGA",
      target_location: "VEGA-ENTRY",
      phase: "staged_at_source",
      reserved_devices: 10,
      loaded_devices: 8,
      capital_system: null,
      selected_sites: 0,
      warnings: [],
      completed: false,
      updated_at_ms: 10,
    },
    {
      mission_id: "BOOT-DONE",
      execution_id: "EXEC-2",
      region: "gamma",
      source_hub: "SOL-1",
      target_system: "SIRIUS",
      target_location: "SIRIUS-ENTRY",
      phase: "completed",
      reserved_devices: 12,
      loaded_devices: 12,
      capital_system: "SIRIUS",
      selected_sites: 5,
      warnings: [],
      completed: true,
      updated_at_ms: 20,
    },
  ],
};

describe("BootstrapContent", () => {
  it("renders active and completed progress with Galaxy and history links", () => {
    const html = renderToStaticMarkup(
      <BootstrapContent
        data={snapshot}
        status="loaded"
        error={null}
        refreshing={false}
        refresh={vi.fn()}
        descriptors={descriptors}
        onOpenGalaxy={vi.fn()}
        onOpenHistory={vi.fn()}
        onRunCommand={vi.fn()}
      />,
    );
    expect(html).toContain("BOOT-ACTIVE");
    expect(html).toContain("Active / resumable");
    expect(html).toContain("BOOT-DONE");
    expect(html).toContain("Completed");
    expect(html).toContain("Show on Galaxy");
    expect(html).toContain("Open history");
    expect(bootstrapCommands(descriptors)[0]?.operationClass).toBe("action");
  });
});
