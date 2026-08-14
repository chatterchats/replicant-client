import { describe, expect, it } from "vitest";

import { initialShellState, shellReducer } from "./shellState";

describe("shellReducer", () => {
  it("opens the inspector for a global entity selection and clears it", () => {
    const selected = shellReducer(initialShellState, {
      type: "select",
      entity: { kind: "device", id: "D-1" },
    });
    expect(selected.selectedEntity).toEqual({ kind: "device", id: "D-1" });
    expect(selected.inspectorOpen).toBe(true);

    const cleared = shellReducer(selected, { type: "clear_selection" });
    expect(cleared.selectedEntity).toBeNull();
    expect(cleared.inspectorOpen).toBe(false);
  });

  it("toggles drawers without losing selection", () => {
    const selected = shellReducer(initialShellState, {
      type: "select",
      entity: { kind: "workflow", id: "WF-1" },
    });
    const collapsed = shellReducer(selected, { type: "toggle_inspector" });
    const activityOpen = shellReducer(collapsed, { type: "toggle_activity" });

    expect(activityOpen.inspectorOpen).toBe(false);
    expect(activityOpen.activityOpen).toBe(true);
    expect(activityOpen.selectedEntity).toEqual({
      kind: "workflow",
      id: "WF-1",
    });
  });

  it("ignores inspector toggles until an entity is selected", () => {
    expect(shellReducer(initialShellState, { type: "toggle_inspector" })).toBe(
      initialShellState,
    );
  });
});
