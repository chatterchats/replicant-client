/** @vitest-environment jsdom */
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import type { DeviceRuntimeInspectorSummary } from "../protocol";
import { DeviceActivityPanel } from "./DeviceActivityPanel";

const runtime: DeviceRuntimeInspectorSummary = {
  created_at: null,
  short_description: null,
  description: null,
  printing: {
    device_type: "survey_drone",
    progress_percent: 42.4,
    eta_seconds: 125,
    completes_at: "2026-09-04T18:00:00Z",
  },
  mining: {
    resource_type: "iron",
    belt: "SOL-BELT-1",
    density: "rich",
    pending_quantity: 12,
  },
  prospect: null,
  repair: {
    target_device_code: "RELAY-1",
    progress_percent: 75,
    eta_seconds: 30,
  },
  scan: {
    target: "SOL",
    progress_percent: 25,
  },
  waiting_for: { reason: "awaiting cargo", device_code: "CARRIER-1" },
  print_queue: [
    {
      device_type: "mining_drone",
      quantity: 2,
      status: "queued",
      progress_percent: 15,
    },
  ],
  queue_size: 5,
  taxi_mode: null,
  tracking_site_id: null,
  beacon_only: null,
  welcome_message: null,
  repair_paid_pct: null,
};

describe("DeviceActivityPanel", () => {
  it("renders structured progress, ETA, queue, and repair navigation", () => {
    const html = renderToStaticMarkup(
      <DeviceActivityPanel runtime={runtime} onNavigate={vi.fn()} />,
    );
    expect(html).toContain("Printing survey_drone");
    expect(html).toContain("42%");
    expect(html).toContain("2m 5s");
    expect(html).toContain("Mining iron");
    expect(html).toContain("Scanning SOL");
    expect(html).toContain("RELAY-1");
    expect(html).toContain("Print queue");
    expect(html).toContain("mining_drone");
    expect(html).toContain("Waiting");
    expect(html).toContain("awaiting cargo");
    expect(html).toContain("CARRIER-1");
    expect(html).toContain("15%");
    expect(html).toContain("SOL-BELT-1");
  });
});
