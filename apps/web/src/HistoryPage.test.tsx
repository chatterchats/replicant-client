import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { ResultView } from "./HistoryPage";

describe("ResultView", () => {
  it("offers cancellation for a running action", () => {
    const html = renderToStaticMarkup(
      <ResultView
        execution={{
          id: "ACTION-1",
          operation_class: "action",
          kind: "survey.belt_search",
          status: "running",
          summary: { succeeded: 0, skipped: 0, failed: 0 },
          started_at_ms: 1,
          finished_at_ms: 1,
          result: null,
          error: null,
          links: [],
        }}
        onSelectEntity={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    expect(html).toContain("Cancel action");
  });
});
