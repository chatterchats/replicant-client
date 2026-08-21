/** @vitest-environment jsdom */
import { act } from "react";
import { createRoot } from "react-dom/client";
import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { SettingsContent } from "./SettingsPage";
import type { SettingsSnapshot } from "./protocol";

const data: SettingsSnapshot = {
  metadata: { revision: 1, generated_at_ms: 10 },
  profile: "default",
  bind_address: "127.0.0.1:8080",
  managed_database_path: "replicant-client.sqlite",
  history_database_path: "replicant-history.sqlite",
  telemetry_database_path: "replicant-telemetry.sqlite",
  runtime_database_path: "replicant-runtime.sqlite",
  log_filter: "info",
  docker: false,
  api_token_source: "environment",
  daemon_settings_require_restart: true,
};

const props = {
  error: null,
  refreshing: false,
  refresh: () => Promise.resolve(),
  automation: { automatic_triggers_enabled: true, workflows_paused: false },
  automationBusy: false,
  onControlAutomation: () => undefined,
};

describe("SettingsContent", () => {
  it("renders daemon environment, token source, and automation state", () => {
    const html = renderToStaticMarkup(
      <SettingsContent {...props} data={data} status="loaded" />,
    );
    expect(html).toContain("default");
    expect(html).toContain("127.0.0.1:8080");
    expect(html).toContain("replicant-client.sqlite");
    expect(html).toContain("Environment variable");
    expect(html).not.toMatch(/RS_API_TOKEN=.+secret/i);
    expect(html).toContain("Restart");
  });

  it("renders loading and error states", () => {
    expect(
      renderToStaticMarkup(<SettingsContent {...props} status="loading" />),
    ).toContain("Loading Settings");
    expect(
      renderToStaticMarkup(
        <SettingsContent {...props} status="error" error="daemon offline" />,
      ),
    ).toContain("Settings unavailable");
  });

  it("toggles automation safety through the existing control action", () => {
    const onControlAutomation = vi.fn();
    const container = document.createElement("div");
    document.body.appendChild(container);
    const root = createRoot(container);
    act(() => {
      root.render(
        <SettingsContent
          {...props}
          data={data}
          status="loaded"
          automation={{
            automatic_triggers_enabled: true,
            workflows_paused: true,
          }}
          onControlAutomation={onControlAutomation}
        />,
      );
    });

    const triggers =
      container.querySelector<HTMLInputElement>("#settings-triggers");
    act(() => {
      triggers?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    });
    expect(onControlAutomation).toHaveBeenCalledWith("disable_triggers");

    const workflows = container.querySelector<HTMLInputElement>(
      "#settings-workflows",
    );
    act(() => {
      workflows?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
    });
    expect(onControlAutomation).toHaveBeenCalledWith("resume_all");

    act(() => {
      root.unmount();
    });
    container.remove();
  });
});
