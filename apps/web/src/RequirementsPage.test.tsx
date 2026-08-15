import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";

import { RequirementsPage } from "./RequirementsPage";

describe("RequirementsPage", () => {
  it("shows desired, actual, in-progress, and missing state", () => {
    const html = renderToStaticMarkup(
      <RequirementsPage
        requirements={[
          {
            id: "relay-sol",
            name: "SOL relay coverage",
            target: "relay infrastructure",
            scope: "system SOL",
            desired: 3,
            actual: 1,
            in_progress: 1,
            missing: 1,
            workflow_id: "workflow-1",
            status: "running",
          },
        ]}
        onSelectWorkflow={() => undefined}
      />,
    );
    expect(html).toContain("Desired</dt><dd>3");
    expect(html).toContain("Actual</dt><dd>1");
    expect(html).toContain("In progress</dt><dd>1");
    expect(html).toContain("Missing</dt><dd>1");
  });
});
