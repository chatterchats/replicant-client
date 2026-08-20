import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { SurveyContent, surveyCommands } from "./SurveyPage";
import type { DescriptorCatalog, SurveySnapshot } from "./protocol";

const descriptors: DescriptorCatalog = {
  reports: [],
  actions: [
    {
      kind: "survey.belt_search",
      display_name: "Fast belt search",
      aliases: ["belt_search", "belt-search"],
      description: "Search systems for asteroid belts",
      category: "survey",
      operation_class: "action",
      risk: "elevated",
      applicable_to: ["system", "replicant"],
      parameters: [],
    },
  ],
  workflows: [
    {
      kind: "survey.route",
      display_name: "Survey route",
      aliases: [],
      description: "Survey systems",
      category: "survey",
      operation_class: "workflow",
      risk: "elevated",
      applicable_to: ["system"],
      parameters: [],
      supported_triggers: [],
    },
  ],
};

const snapshot: SurveySnapshot = {
  metadata: { revision: 7, generated_at_ms: 10 },
  fleet: [],
  missions: [
    {
      workflow: {
        id: "WF-SURVEY",
        kind: "survey.route",
        status: "running",
        current_step: "traveling",
        revision: 2,
        updated_at_ms: 10,
      },
      replicant: "R-1",
      vessel: "SHIP-1",
      center: "SOL",
      phase: "traveling",
      completed_systems: 2,
      total_systems: 5,
      next_system: "VEGA",
      controller: "SC-1",
      drones: ["SD-1", "SD-2", "SD-3"],
    },
  ],
};

describe("SurveyContent", () => {
  it("renders workflow route progress and delegates descriptor starts", () => {
    const run = vi.fn();
    const openWorkflow = vi.fn();
    const element = (
      <SurveyContent
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
    expect(html).toContain("2 / 5");
    expect(html).toContain("VEGA");
    expect(html).toContain("Survey route");
    expect(html).toContain("Fast belt search");
    expect(html).toContain("Open workflow");
    const commands = surveyCommands(descriptors);
    expect(commands.map((command) => command.descriptor.kind)).toEqual([
      "survey.belt_search",
      "survey.route",
    ]);
    const command = commands.find(
      (candidate) => candidate.descriptor.kind === "survey.belt_search",
    );
    expect(command?.operationClass).toBe("action");
    if (command) run(command);
    expect(run).toHaveBeenCalledWith(command);
  });
});
