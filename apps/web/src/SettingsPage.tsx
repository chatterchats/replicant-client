import { daemonApi } from "./api";
import { useAutomationControl, useDaemonState } from "./daemon";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { AutomationStatus, SettingsSnapshot } from "./protocol";

const tokenSourceLabel: Record<SettingsSnapshot["api_token_source"], string> = {
  environment: "Environment variable (RS_API_TOKEN)",
  secret_file: "Secret file (RS_API_TOKEN_FILE)",
  unset: "Not configured",
};

export function SettingsPage() {
  const query = useDomainQuery({
    fetcher: (signal) => daemonApi.settings(signal),
    isEmpty: () => false,
  });
  const automation = useDaemonState().automation;
  const { busy, error, control } = useAutomationControl();
  return (
    <SettingsContent
      {...query}
      automation={automation}
      automationBusy={busy}
      automationError={error}
      onControlAutomation={control}
    />
  );
}

export function SettingsContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  automation,
  automationBusy,
  automationError,
  onControlAutomation,
}: {
  data?: SettingsSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  automation: AutomationStatus;
  automationBusy: boolean;
  automationError?: string;
  onControlAutomation: (
    action: "enable_triggers" | "disable_triggers" | "pause_all" | "resume_all",
  ) => void;
}) {
  if (!data && status === "loading")
    return <article className="page loading-state">Loading Settings…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Settings unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  const isTauri =
    typeof window !== "undefined" && window.location.protocol === "tauri:";

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Settings</p>
          <h1>Settings</h1>
          <p className="lede">
            Application and runtime configuration for this daemon profile.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}

      <div className="settings-grid">
        <section className="settings-card" aria-labelledby="settings-daemon">
          <h2 id="settings-daemon">Daemon environment</h2>
          {data?.daemon_settings_require_restart && (
            <p className="settings-note">
              These values are read from startup configuration. Restart{" "}
              <code>replicantd</code> to change them.
            </p>
          )}
          <dl>
            <dt>Profile</dt>
            <dd>{data?.profile ?? "—"}</dd>
            <dt>Local address</dt>
            <dd>{data?.bind_address ?? "—"}</dd>
            <dt>Managed database</dt>
            <dd>{data?.managed_database_path ?? "—"}</dd>
            <dt>History database</dt>
            <dd>{data?.history_database_path ?? "—"}</dd>
            <dt>Runtime database</dt>
            <dd>{data?.runtime_database_path ?? "—"}</dd>
            <dt>Log filter</dt>
            <dd>{data?.log_filter ?? "—"}</dd>
            <dt>Deployment</dt>
            <dd>
              <span className="status-chip">
                {data?.docker ? "Docker" : "Native"}
              </span>{" "}
              <span className="status-chip">
                {isTauri ? "Desktop app" : "Browser"}
              </span>
            </dd>
          </dl>
        </section>

        <section className="settings-card" aria-labelledby="settings-token">
          <h2 id="settings-token">API token</h2>
          <p className="settings-note">
            The token value is never sent to the browser.
          </p>
          <dl>
            <dt>Source</dt>
            <dd>{data ? tokenSourceLabel[data.api_token_source] : "—"}</dd>
          </dl>
        </section>

        <section
          className="settings-card"
          aria-labelledby="settings-automation"
        >
          <h2 id="settings-automation">Automation safety</h2>
          <p className="settings-note">
            Changes here apply immediately and affect every managed workflow.
          </p>
          {automationError && (
            <p className="inline-warning">{automationError}</p>
          )}
          <div className="form-grid">
            <label className="boolean-field" htmlFor="settings-triggers">
              <input
                id="settings-triggers"
                type="checkbox"
                checked={automation.automatic_triggers_enabled}
                disabled={automationBusy}
                onChange={() => {
                  onControlAutomation(
                    automation.automatic_triggers_enabled
                      ? "disable_triggers"
                      : "enable_triggers",
                  );
                }}
              />
              <span>
                <strong>Automatic triggers</strong>
                <small>Allow non-manual triggers to launch new work.</small>
              </span>
            </label>
            <label className="boolean-field" htmlFor="settings-workflows">
              <input
                id="settings-workflows"
                type="checkbox"
                checked={!automation.workflows_paused}
                disabled={automationBusy}
                onChange={() => {
                  onControlAutomation(
                    automation.workflows_paused ? "resume_all" : "pause_all",
                  );
                }}
              />
              <span>
                <strong>Workflow execution</strong>
                <small>Pause to halt every workflow executor globally.</small>
              </span>
            </label>
          </div>
        </section>
      </div>
    </article>
  );
}
