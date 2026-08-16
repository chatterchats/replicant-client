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
  | { type: "restore"; route: Route }
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
    case "restore":
      // Applied from the URL (initial load, back/forward). Transient chrome is
      // left as-is; only addressable state is restored.
      return {
        ...state,
        page: action.route.page,
        selectedEntity: action.route.entity,
        inspectorOpen: action.route.entity !== null,
        paletteOpen: false,
      };
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

/** Addressable shell state, mirrored into the location hash. */
export interface Route {
  page: string;
  entity: SelectedEntity | null;
}

const inspectableKinds: InspectableKind[] = [
  "system",
  "location",
  "replicant",
  "device",
  "inventory",
  "autofactory",
  "cargo",
  "operation",
  "workflow",
  "event",
  "resource",
];

/**
 * Serializes addressable shell state as a location hash.
 *
 * Pages and selections live in the URL so a refresh keeps your place, the
 * browser's back button works, and any view can be linked or bookmarked.
 */
export function routeToHash(route: Route): string {
  const page = encodeURIComponent(route.page);
  if (!route.entity) return `#/${page}`;
  const { kind, id } = route.entity;
  return `#/${page}/${encodeURIComponent(kind)}/${encodeURIComponent(id)}`;
}

/** Parses a location hash, falling back to `fallback` for anything unusable. */
export function routeFromHash(hash: string, fallback: Route): Route {
  const segments = hash
    .replace(/^#\/?/, "")
    .split("/")
    .filter(Boolean)
    .map((segment) => decodeURIComponent(segment));
  const [page, kind, id] = segments;
  if (page === undefined) return fallback;
  const entity =
    kind !== undefined &&
    id !== undefined &&
    (inspectableKinds as string[]).includes(kind)
      ? { kind: kind as InspectableKind, id }
      : null;
  return { page, entity };
}
