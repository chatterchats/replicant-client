import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vitest";
import type { EntitySummary, SystemInspectorSummary } from "../protocol";
import { SystemInspector } from "./SystemInspector";
const summary: EntitySummary = {
  entity: { kind: "system", id: "SOL" },
  label: "Sol",
  secondary_label: null,
  system: "SOL",
  location: null,
  entity_type: "G",
  status: "explored",
};
const detail: SystemInspectorSummary = {
  name: "Sol",
  spectral_type: "G",
  region: "Core",
  entry_point: "SOL-1",
  position: { x: 0, y: 1, z: 2 },
  explored: true,
  has_hub: false,
  has_ward: false,
  has_life: true,
  tags: [],
  stellar: {},
  asteroid_belt: {},
  outer_system: {},
  mining_bonus_percent: null,
  shop_count: null,
  active_event_count: null,
  object_count: 54,
  children: {
    total: 54,
    items: [],
    groups: [
      {
        entity_kind: "location",
        entity_type: "planet",
        count: 54,
        statuses: [{ status: "scanned", count: 54 }],
      },
    ],
  },
};
describe("SystemInspector", () => {
  it("renders typed fields and bounded 54-body summaries", () => {
    const html = renderToStaticMarkup(
      <SystemInspector summary={summary} detail={detail} />,
    );
    expect(html).toContain("Spectral type");
    expect(html).toContain("0.00, 1.00, 2.00 LY");
    expect(html).toContain("54");
    expect(html).toContain("<details");
    expect(html).not.toContain("Location 0");
  });

  it("renders structured star, belt, and outer-system facts without object dumps", () => {
    const html = renderToStaticMarkup(
      <SystemInspector
        summary={summary}
        detail={{
          ...detail,
          stellar: {
            age_my: 6300.09,
            color: "orange",
            habitable_zone: { inner_au: 0.58, outer_au: 1.01 },
          },
          asteroid_belt: {
            present: true,
            belts: [{ designation: "SOL-BELT-1", density: "dense" }],
          },
          outer_system: {
            kuiper: { designation: "SOL-KUIPER", distance_au: 29.18 },
            oort: { designation: "SOL-OORT", distance_au: 3497.63 },
          },
        }}
      />,
    );
    expect(html).toContain("Habitable Zone");
    expect(html).toContain("Inner AU");
    expect(html).toContain("SOL-BELT-1");
    expect(html).toContain("SOL-KUIPER");
    expect(html).toContain("Distance AU");
    expect(html).not.toContain("[object Object]");
    expect(html).not.toContain("{&quot;designation&quot;");
  });
});
