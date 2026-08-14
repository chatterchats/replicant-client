import { describe, expect, it } from "vitest";

import type { FiniteExecution } from "./protocol";
import {
  executionExport,
  resultLogExcerpt,
  resultSections,
} from "./resultPresentation";

const execution: FiniteExecution = {
  id: "execution-1",
  operation_class: "action",
  kind: "tag_devices",
  status: "succeeded",
  summary: { succeeded: 1, skipped: 1, failed: 0 },
  started_at_ms: 10,
  finished_at_ms: 20,
  result: {
    changed_devices: 1,
    report: {
      events: [{ kind: "succeeded", subject: "D-1", detail: "tagged" }],
    },
  },
  error: null,
  links: [{ kind: "device", id: "D-1" }],
};

describe("result presentation", () => {
  it("builds structured summary and event sections", () => {
    const sections = resultSections(execution.result);
    expect(sections.map((section) => section.title)).toEqual([
      "Summary",
      "report · events",
    ]);
    expect(sections[0]?.rows).toEqual([
      { field: "changed devices", value: "1" },
    ]);
  });

  it("exports structured data and bounded log excerpts", () => {
    expect(JSON.parse(executionExport(execution))).toEqual(execution);
    expect(resultLogExcerpt(execution.result)).toBe("[succeeded] D-1: tagged");
  });
});
