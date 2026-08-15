import { Children, isValidElement, type ReactNode } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { OverviewContent } from "./OverviewPage";
import type { OverviewSnapshot } from "./protocol";

const empty: OverviewSnapshot = {
  metadata: { revision: 1, generated_at_ms: 10 },
  health: { status: "healthy", daemon_version: "test", detail: null },
  sync: { phase: "ready", revision: 1, last_event_at_ms: null, detail: null },
  automation: { automatic_triggers_enabled: true, workflows_paused: false },
  replicants: [],
  active_travel: [],
  active_workflows: [],
  workflow_counts: [],
  attention_workflows: [],
  notifications: [],
  recent_activity: [],
};

const props = {
  error: null,
  refreshing: false,
  refresh: () => Promise.resolve(),
  onNavigate: () => undefined,
  onSelectEntity: () => undefined,
  onSelectWorkflow: () => undefined,
  onOpenSystem: () => undefined,
};

function nodeText(node: ReactNode): string {
  if (typeof node === "string" || typeof node === "number") return String(node);
  if (!isValidElement<{ children?: ReactNode }>(node)) return "";
  return Children.toArray(node.props.children).map(nodeText).join("");
}

function click(node: ReactNode, label: string): boolean {
  if (!isValidElement<{ children?: ReactNode; onClick?: () => void }>(node))
    return false;
  if (
    typeof node.props.onClick === "function" &&
    nodeText(node).includes(label)
  ) {
    node.props.onClick();
    return true;
  }
  return Children.toArray(node.props.children).some((child) =>
    click(child, label),
  );
}

describe("OverviewContent", () => {
  it("renders loaded and empty operations state", () => {
    const loaded = renderToStaticMarkup(
      <OverviewContent
        {...props}
        data={{
          ...empty,
          replicants: [
            {
              entity: { kind: "replicant", id: "R-1" },
              name: "Ada",
              system: "SOL",
              location: "EARTH",
              status: "idle",
            },
          ],
        }}
        status="loaded"
      />,
    );
    expect(loaded).toContain("Ada");
    expect(loaded).toContain("Health / Automation");

    const blank = renderToStaticMarkup(
      <OverviewContent {...props} data={empty} status="empty" />,
    );
    expect(blank).toContain("No owned replicants, workflows");
  });

  it("renders request errors", () => {
    const html = renderToStaticMarkup(
      <OverviewContent {...props} error="daemon offline" status="error" />,
    );
    expect(html).toContain("Overview unavailable");
    expect(html).toContain("daemon offline");
  });

  it("uses the shell navigation callback", () => {
    const onNavigate = vi.fn();
    const page = OverviewContent({
      ...props,
      data: empty,
      status: "loaded",
      onNavigate,
    });
    expect(click(page, "Open automation center")).toBe(true);
    expect(onNavigate).toHaveBeenCalledWith("Automations");
  });
});
