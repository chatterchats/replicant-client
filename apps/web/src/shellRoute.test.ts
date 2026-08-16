import { describe, expect, it } from "vitest";

import {
  routeFromHash,
  routeToHash,
  shellReducer,
  initialShellState,
} from "./shellState";

const fallback = { page: "Overview", entity: null };

describe("shell routing", () => {
  it("round-trips a page and selection through the hash", () => {
    const route = {
      page: "Devices",
      entity: { kind: "device" as const, id: "D-1" },
    };
    expect(routeToHash(route)).toBe("#/Devices/device/D-1");
    expect(routeFromHash(routeToHash(route), fallback)).toEqual(route);
  });

  it("round-trips identifiers needing encoding", () => {
    const route = {
      page: "System",
      entity: { kind: "location" as const, id: "SOL/BELT 1" },
    };
    expect(routeFromHash(routeToHash(route), fallback)).toEqual(route);
  });

  it("falls back for empty hashes and ignores unknown entity kinds", () => {
    expect(routeFromHash("", fallback)).toEqual(fallback);
    expect(routeFromHash("#/", fallback)).toEqual(fallback);
    expect(routeFromHash("#/Galaxy/nonsense/X", fallback)).toEqual({
      page: "Galaxy",
      entity: null,
    });
  });

  it("restores addressable state into the shell", () => {
    const restored = shellReducer(initialShellState, {
      type: "restore",
      route: { page: "Cargo", entity: { kind: "device", id: "D-9" } },
    });
    expect(restored.page).toBe("Cargo");
    expect(restored.selectedEntity).toEqual({ kind: "device", id: "D-9" });
    expect(restored.inspectorOpen).toBe(true);
  });
});
