/** @vitest-environment jsdom */
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";
import { InspectorFields } from "./InspectorFields";
import { InspectorShell } from "./InspectorShell";

const summary = {
  entity: { kind: "location" as const, id: "SOL-1" },
  label: "SOL-1",
  secondary_label: null,
  system: "SOL",
  location: "SOL-1",
  entity_type: "planet",
  status: null,
};

describe("InspectorShell", () => {
  it("omits empty sections, preserves false and zero, and renders provenance", () => {
    const html = renderToStaticMarkup(
      <InspectorShell
        summary={summary}
        body={
          <InspectorFields
            fields={[
              { label: "Magnetic field", value: false },
              { label: "Gravity", value: 0 },
              { label: "Unknown", value: null },
            ]}
          />
        }
        provenance={{
          observed_at_ms: 0,
          stale: true,
          reachability: "out_of_range",
          source_operation: "GET /v1/locations/{designation}",
        }}
        onClose={vi.fn()}
        onClear={vi.fn()}
      />,
    );
    expect(html).toContain("Magnetic field");
    expect(html).toContain("No");
    expect(html).toContain("Gravity");
    expect(html).toContain(">0<");
    expect(html).not.toContain("Unknown");
    expect(html).not.toContain("Relations");
    expect(html).toContain("Stale");
    expect(html).toContain("out_of_range");
    expect(html).toContain("GET /v1/locations/{designation}");
    expect(html).toContain('aria-label="Copy entity ID"');
  });
  it("renders inspector-local back and forward controls", () => {
    const html = renderToStaticMarkup(
      <InspectorShell
        summary={summary}
        body={<p>Detail</p>}
        navigation={{
          canGoBack: true,
          canGoForward: false,
          backLabel: "SOL",
          forwardLabel: null,
          position: 1,
          total: 2,
          entries: [
            { index: 0, label: "SOL" },
            { index: 1, label: "SOL-1" },
          ],
          onBack: vi.fn(),
          onForward: vi.fn(),
          onJump: vi.fn(),
        }}
        onClose={vi.fn()}
        onClear={vi.fn()}
      />,
    );
    expect(html).toContain('aria-label="Back to SOL"');
    expect(html).toContain('aria-label="Forward in inspector"');
    expect(html).toContain("disabled");
    expect(html).toContain('aria-label="Inspector history"');
    expect(html).toContain("1. SOL");
  });
});
