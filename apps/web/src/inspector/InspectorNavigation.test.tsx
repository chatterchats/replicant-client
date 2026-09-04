/* eslint-disable @typescript-eslint/require-await */
/** @vitest-environment jsdom */
import { act } from "react";
import { createRoot } from "react-dom/client";
import { describe, expect, it, vi } from "vitest";

import type { InspectorProps } from "./Inspector";
import { Inspector } from "./Inspector";

function props(
  id: string,
  onSelectEntity: InspectorProps["onSelectEntity"],
): InspectorProps {
  return {
    entity: { kind: "inventory", id },
    value: { label: id },
    descriptors: { reports: [], actions: [], workflows: [] },
    entities: {},
    activity: [],
    onClose: vi.fn(),
    onClear: vi.fn(),
    onOpenGalaxy: vi.fn(),
    onOpenSystem: vi.fn(),
    onOpenWorkflow: vi.fn(),
    onSelectEntity,
    onRunCommand: vi.fn(),
    onOperationFinished: vi.fn(),
  };
}

describe("Inspector navigation history", () => {
  it("keeps local back/forward history and supports direct history jumps", async () => {
    const container = document.createElement("div");
    const root = createRoot(container);
    const onSelectEntity = vi.fn();

    await act(async () => {
      root.render(<Inspector {...props("FIRST", onSelectEntity)} />);
    });
    await act(async () => {
      root.render(<Inspector {...props("SECOND", onSelectEntity)} />);
    });
    await act(async () => {
      root.render(<Inspector {...props("THIRD", onSelectEntity)} />);
    });

    const history = container.querySelector<HTMLSelectElement>(
      'select[aria-label="Inspector history"]',
    );
    expect(history?.options).toHaveLength(3);
    expect(history?.value).toBe("2");

    const back = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Back to inventory SECOND"]',
    );
    expect(back?.disabled).toBe(false);

    await act(async () => {
      back?.click();
    });
    expect(onSelectEntity).toHaveBeenCalledWith({
      kind: "inventory",
      id: "SECOND",
    });

    const forward = container.querySelector<HTMLButtonElement>(
      'button[aria-label="Forward to inventory THIRD"]',
    );
    expect(forward?.disabled).toBe(false);

    if (!history) throw new Error("history selector not rendered");
    await act(async () => {
      history.value = "0";
      history.dispatchEvent(new Event("change", { bubbles: true }));
    });
    expect(onSelectEntity).toHaveBeenCalledWith({
      kind: "inventory",
      id: "FIRST",
    });

    await act(async () => {
      window.dispatchEvent(
        new KeyboardEvent("keydown", { key: "ArrowRight", altKey: true }),
      );
    });
    expect(onSelectEntity).toHaveBeenLastCalledWith({
      kind: "inventory",
      id: "SECOND",
    });

    root.unmount();
  });
});
