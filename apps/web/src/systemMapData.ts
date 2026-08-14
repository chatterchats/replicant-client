// Adapted from the approved replicant.react system-map geometry and marker model.
import {
  applicableDescriptorCommands,
  type DescriptorCommand,
} from "./CommandPalette";
import type {
  DescriptorCatalog,
  SystemMarker,
  SystemSceneSnapshot,
} from "./protocol";

export interface SystemLine {
  from: { x: number; y: number };
  to: { x: number; y: number };
  kind: "orbit" | "travel" | "workflow";
}

export function mapSystemScene(scene: SystemSceneSnapshot): SystemLine[] {
  const positions = new Map(
    scene.markers.map((marker) => [marker.id, marker.position]),
  );
  const lines: SystemLine[] = scene.markers.flatMap((marker) => {
    const parent = marker.parent ? positions.get(marker.parent) : undefined;
    return parent
      ? [{ from: parent, to: marker.position, kind: "orbit" as const }]
      : [];
  });
  for (const travel of scene.active_travel) {
    const from = positions.get(travel.from);
    const to = positions.get(travel.to);
    if (from && to) lines.push({ from, to, kind: "travel" });
  }
  const center = positions.get(scene.system);
  if (center) {
    for (const workflow of scene.workflow_markers) {
      const to = positions.get(workflow.location) ?? center;
      lines.push({ from: center, to, kind: "workflow" });
    }
  }
  return lines;
}

export function markerActions(
  catalog: DescriptorCatalog,
  marker: SystemMarker,
): DescriptorCommand[] {
  const kind = marker.entity.kind;
  return kind === "system" ||
    kind === "location" ||
    kind === "device" ||
    kind === "replicant"
    ? applicableDescriptorCommands(catalog, kind)
    : [];
}
