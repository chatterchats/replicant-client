import type { EntityKind } from "./protocol";

export type InspectableKind = EntityKind | "event" | "resource";

export interface SelectedEntity {
  kind: InspectableKind;
  id: string;
}

export interface ShellState {
  page: string;
  selectedEntity: SelectedEntity | null;
  inspectorOpen: boolean;
  activityOpen: boolean;
  paletteOpen: boolean;
}

export type ShellAction =
  | { type: "navigate"; page: string }
  | { type: "select"; entity: SelectedEntity }
  | { type: "clear_selection" }
  | { type: "toggle_inspector" }
  | { type: "toggle_activity" }
  | { type: "set_palette"; open: boolean };

export const initialShellState: ShellState = {
  page: "Overview",
  selectedEntity: null,
  inspectorOpen: false,
  activityOpen: false,
  paletteOpen: false,
};

export function shellReducer(
  state: ShellState,
  action: ShellAction,
): ShellState {
  switch (action.type) {
    case "navigate":
      return { ...state, page: action.page, paletteOpen: false };
    case "select":
      return { ...state, selectedEntity: action.entity, inspectorOpen: true };
    case "clear_selection":
      return { ...state, selectedEntity: null, inspectorOpen: false };
    case "toggle_inspector":
      return state.selectedEntity
        ? { ...state, inspectorOpen: !state.inspectorOpen }
        : state;
    case "toggle_activity":
      return { ...state, activityOpen: !state.activityOpen };
    case "set_palette":
      return { ...state, paletteOpen: action.open };
  }
}
