/** @vitest-environment jsdom */
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import type { DescriptorCommand } from "./CommandPalette";
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
  workflows: [
    {
      kind: "trade.fulfillment",
      display_name: "Execute provisioned trade",
      aliases: [],
      description: "Provision, execute, and return from a trade",
      category: "trade",
      operation_class: "workflow",
      risk: "elevated",
      applicable_to: ["device", "replicant", "location"],
      parameters: [],
      supported_triggers: [],
    },
  ],
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
      trade_details_status: "available",
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
    expect(html).toContain("Buy");
    expect(html).toContain("Provision &amp; Buy");
    expect(
      tradeCommands(descriptors).some(
        (command) => command.descriptor.kind === "trade.fulfillment",
      ),
    ).toBe(true);
  });

  it("binds direct and provisioned buys to the selected trade row", () => {
    const onRunCommand = vi.fn<(command: DescriptorCommand) => void>();
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    act(() => {
      root.render(
        <TradeContent
          {...props}
          onRunCommand={onRunCommand}
          data={snapshot}
          status="loaded"
          error={null}
        />,
      );
    });

    const button = (label: string) =>
      Array.from(container.querySelectorAll("button")).find(
        (candidate) => candidate.textContent === label,
      );

    act(() => {
      button("Buy")?.click();
    });
    const directBuy = onRunCommand.mock.calls.at(-1)?.[0];
    expect(directBuy?.descriptor.kind).toBe("trade.execute");
    expect(directBuy?.initialParameters).toEqual({
      controller: "TC-1",
      trade_code: "TRD-1",
    });

    act(() => {
      button("Provision & Buy")?.click();
    });
    const provisionedBuy = onRunCommand.mock.calls.at(-1)?.[0];
    expect(provisionedBuy?.descriptor.kind).toBe("trade.fulfillment");
    expect(provisionedBuy?.initialParameters).toEqual({
      controller: "TC-1",
      trade_code: "TRD-1",
      shop_location: "SOL-1",
    });

    act(() => {
      root.unmount();
    });
    container.remove();
  });

  it("keeps out-of-comms shops visible without failing the page", () => {
    const data: TradeSnapshot = {
      ...snapshot,
      controllers: snapshot.controllers.map((controller) => ({
        ...controller,
        trade_details_status: "out_of_comms",
        trades: [],
      })),
    };
    const html = renderToStaticMarkup(
      <TradeContent {...props} data={data} status="loaded" error={null} />,
    );
    expect(html).toContain("Exchange");
    expect(html).toContain("Trade details are out of comms");
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
