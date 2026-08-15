import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { TradeContent, tradeCommands } from "./TradePage";
import type { DescriptorCatalog, TradeSnapshot } from "./protocol";

const descriptors: DescriptorCatalog = {
  reports: [],
  actions: [
    {
      kind: "trade.execute",
      display_name: "Execute trade",
      aliases: [],
      description: "Execute an existing trade",
      category: "trade",
      operation_class: "action",
      risk: "elevated",
      applicable_to: ["device"],
      parameters: [],
    },
  ],
  workflows: [],
};

const snapshot: TradeSnapshot = {
  metadata: { revision: 9, generated_at_ms: 10 },
  viewer: { kind: "replicant", id: "R-1" },
  controllers: [
    {
      entity: { kind: "device", id: "TC-1" },
      shop_name: "Exchange",
      description: null,
      is_local: true,
      owner_name: "Ada",
      owner_replicant: "R-1",
      system: "SOL",
      location: "SOL-1",
      total_stock: 2,
      trade_count: 1,
      trades: [
        {
          trade_code: "TRD-1",
          name: null,
          current_stock: 1,
          initial_stock: null,
          requested: [{ kind: "resource", item: "iron", quantity: 4 }],
          offered: [{ kind: "device", item: "probe", quantity: 1 }],
          created_at: null,
        },
      ],
      workflow: null,
    },
  ],
};

const props = {
  refreshing: false,
  refresh: vi.fn(),
  descriptors,
  onSelectEntity: vi.fn(),
  onOpenSystem: vi.fn(),
  onSelectWorkflow: vi.fn(),
  onRunCommand: vi.fn(),
};

describe("TradeContent", () => {
  it("renders grouped exchanges and descriptor-driven actions", () => {
    const html = renderToStaticMarkup(
      <TradeContent {...props} data={snapshot} status="loaded" error={null} />,
    );
    expect(html).toContain("Exchange");
    expect(html).toContain("4 iron");
    expect(html).toContain("1 probe");
    expect(html).toContain("Inspect");
    expect(tradeCommands(descriptors)[0]?.descriptor.kind).toBe(
      "trade.execute",
    );
  });

  it("distinguishes loading, empty, and error states", () => {
    expect(
      renderToStaticMarkup(
        <TradeContent {...props} status="loading" error={null} />,
      ),
    ).toContain("Loading Trade");
    expect(
      renderToStaticMarkup(
        <TradeContent
          {...props}
          data={{ ...snapshot, controllers: [] }}
          status="empty"
          error={null}
        />,
      ),
    ).toContain("No visible trade controllers");
    expect(
      renderToStaticMarkup(
        <TradeContent {...props} status="error" error="offline" />,
      ),
    ).toContain("Trade unavailable");
  });
});
