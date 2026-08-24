/** @vitest-environment jsdom */
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { InspectorShell } from "./InspectorShell";

const summary = {
  entity: { kind: "device" as const, id: "D-1" },
  label: "D-1",
  secondary_label: null,
  system: null,
  location: null,
  entity_type: null,
  status: null,
};

function setViewport(width: number) {
  Object.defineProperty(window, "innerWidth", {
    value: width,
    configurable: true,
  });
  window.dispatchEvent(new Event("resize"));
}

async function mount() {
  const container = document.createElement("div");
  const root = createRoot(container);
  await act(async () => {
    root.render(
      <InspectorShell
        summary={summary}
        body={<p>Body</p>}
        onClose={vi.fn()}
        onClear={vi.fn()}
      />,
    );
  });
  return { container, root };
}

async function unmount(root: Root) {
  await act(async () => root.unmount());
}

function pointer(type: string, clientX: number) {
  const event = new MouseEvent(type, { bubbles: true, clientX });
  Object.defineProperty(event, "pointerId", { value: 1 });
  return event;
}

describe("Inspector resize", () => {
  beforeEach(() => {
    localStorage.clear();
    setViewport(1000);
  });

  it("drags from 390 to 420 and restores the persisted width", async () => {
    const first = await mount();
    const aside = first.container.querySelector<HTMLElement>(".inspector")!;
    const handle =
      first.container.querySelector<HTMLElement>("[role=separator]")!;
    expect(aside.style.getPropertyValue("--inspector-width")).toBe("390px");
    await act(async () => {
      handle.dispatchEvent(pointer("pointerdown", 100));
    });
    await act(async () => {
      handle.dispatchEvent(pointer("pointermove", 70));
      handle.dispatchEvent(pointer("pointerup", 70));
    });
    expect(aside.style.getPropertyValue("--inspector-width")).toBe("420px");
    expect(localStorage.getItem("replicant.inspector.width.v1")).toBe("420");
    await unmount(first.root);

    const restored = await mount();
    expect(
      restored.container
        .querySelector<HTMLElement>(".inspector")
        ?.style.getPropertyValue("--inspector-width"),
    ).toBe("420px");
    await unmount(restored.root);
  });

  it("clamps malformed, negative, and over-limit stored values", async () => {
    for (const [stored, expected] of [
      ["broken", "390px"],
      ["-20", "390px"],
      ["9999", "550px"],
    ] as const) {
      localStorage.setItem("replicant.inspector.width.v1", stored);
      const mounted = await mount();
      expect(
        mounted.container
          .querySelector<HTMLElement>(".inspector")
          ?.style.getPropertyValue("--inspector-width"),
      ).toBe(expected);
      await unmount(mounted.root);
    }
  });

  it("supports keyboard bounds and hides the handle on mobile", async () => {
    const mounted = await mount();
    const handle =
      mounted.container.querySelector<HTMLElement>("[role=separator]")!;
    await act(async () =>
      handle.dispatchEvent(
        new KeyboardEvent("keydown", { key: "ArrowLeft", bubbles: true }),
      ),
    );
    expect(handle.getAttribute("aria-valuenow")).toBe("406");
    await act(async () =>
      handle.dispatchEvent(
        new KeyboardEvent("keydown", { key: "Home", bubbles: true }),
      ),
    );
    expect(handle.getAttribute("aria-valuenow")).toBe("320");
    await act(async () =>
      handle.dispatchEvent(
        new KeyboardEvent("keydown", { key: "End", bubbles: true }),
      ),
    );
    expect(handle.getAttribute("aria-valuenow")).toBe("550");
    setViewport(700);
    await act(async () => undefined);
    expect(mounted.container.querySelector("[role=separator]")).toBeNull();
    await unmount(mounted.root);
  });
});
