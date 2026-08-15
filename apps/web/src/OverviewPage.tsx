import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { EntityRef, OverviewSnapshot } from "./protocol";

const overviewEmpty = (overview: OverviewSnapshot) =>
  overview.replicants.length === 0 &&
  overview.active_workflows.length === 0 &&
  overview.notifications.length === 0 &&
  overview.recent_activity.length === 0;

export function OverviewPage({
  onNavigate,
  onSelectEntity,
  onSelectWorkflow,
  onOpenSystem,
}: {
  onNavigate: (page: string) => void;
  onSelectEntity: (entity: EntityRef) => void;
  onSelectWorkflow: (id: string) => void;
  onOpenSystem: (system: string) => void;
}) {
  const query = useDomainQuery({
    slice: "overview",
    fetcher: (signal) => daemonApi.overview(signal),
    isEmpty: overviewEmpty,
  });
  return (
    <OverviewContent
      {...query}
      onNavigate={onNavigate}
      onSelectEntity={onSelectEntity}
      onSelectWorkflow={onSelectWorkflow}
      onOpenSystem={onOpenSystem}
    />
  );
}

export function OverviewContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  onNavigate,
  onSelectEntity,
  onSelectWorkflow,
  onOpenSystem,
}: {
  data?: OverviewSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  onNavigate: (page: string) => void;
  onSelectEntity: (entity: EntityRef) => void;
  onSelectWorkflow: (id: string) => void;
  onOpenSystem: (system: string) => void;
}) {
  const openSystem = (system: string | null) => {
    if (system) onOpenSystem(system);
  };
  if (!data && status === "loading")
    return (
      <article className="overview-state">Loading operations overview…</article>
    );
  if (!data && status === "error")
    return (
      <article className="overview-state error">
        <h1>Overview unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Try again</button>
      </article>
    );
  if (!data) return null;

  return (
    <article className="overview-page">
      <header className="overview-header">
        <div>
          <p className="eyebrow">Operations</p>
          <h1>Overview</h1>
          <p className="lede">Current work and anything needing attention.</p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="overview-warning">Refresh failed: {error}</p>}
      {status === "empty" && (
        <section className="overview-empty">
          No owned replicants, workflows, or operational notices yet.
        </section>
      )}

      <div className="overview-grid">
        <section className="overview-panel overview-health">
          <header>
            <h2>Health / Automation</h2>
          </header>
          <dl className="overview-metrics">
            <div>
              <dt>Daemon</dt>
              <dd className={data.health.status}>{data.health.status}</dd>
            </div>
            <div>
              <dt>Managed sync</dt>
              <dd>{data.sync.phase}</dd>
            </div>
            <div>
              <dt>Workflows</dt>
              <dd>{data.automation.workflows_paused ? "paused" : "running"}</dd>
            </div>
            <div>
              <dt>Triggers</dt>
              <dd>
                {data.automation.automatic_triggers_enabled
                  ? "enabled"
                  : "disabled"}
              </dd>
            </div>
          </dl>
          <button
            className="text-button"
            onClick={() => {
              onNavigate("Automations");
            }}
          >
            Open automation center →
          </button>
        </section>

        <section className="overview-panel">
          <header>
            <h2>Replicants</h2>
            <span>{data.replicants.length}</span>
          </header>
          {data.replicants.length === 0 ? (
            <p className="muted">No owned replicants in managed state.</p>
          ) : (
            <ul className="overview-list">
              {data.replicants.map((replicant) => (
                <li key={replicant.entity.id}>
                  <button
                    onClick={() => {
                      onSelectEntity(replicant.entity);
                    }}
                  >
                    <strong>{replicant.name ?? replicant.entity.id}</strong>
                    <small>{replicant.status ?? "unknown status"}</small>
                  </button>
                  {replicant.system ? (
                    <button
                      className="location-link"
                      onClick={() => {
                        openSystem(replicant.system);
                      }}
                    >
                      {replicant.location ?? replicant.system}
                    </button>
                  ) : (
                    <span className="muted">Unknown location</span>
                  )}
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="overview-panel overview-work">
          <header>
            <h2>Active Work</h2>
            <span>{data.active_workflows.length}</span>
          </header>
          <div className="workflow-counts">
            {data.workflow_counts.map((item) => (
              <span className={item.status} key={item.status}>
                {item.status} <strong>{item.count}</strong>
              </span>
            ))}
          </div>
          {data.active_workflows.length === 0 ? (
            <p className="muted">No active workflows.</p>
          ) : (
            <ul className="overview-list">
              {data.active_workflows.slice(0, 6).map((workflow) => (
                <li key={workflow.id}>
                  <button
                    onClick={() => {
                      onSelectWorkflow(workflow.id);
                    }}
                  >
                    <strong>{workflow.kind}</strong>
                    <small>{workflow.current_step ?? workflow.status}</small>
                  </button>
                  <span className={`status-chip ${workflow.status}`}>
                    {workflow.status}
                  </span>
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="overview-panel">
          <header>
            <h2>Active Travel</h2>
            <span>{data.active_travel.length}</span>
          </header>
          {data.active_travel.length === 0 ? (
            <p className="muted">No replicants are traveling.</p>
          ) : (
            <ul className="overview-list">
              {data.active_travel.map((travel) => (
                <li key={travel.entity.id}>
                  <button
                    onClick={() => {
                      onSelectEntity(travel.entity);
                    }}
                  >
                    <strong>{travel.entity.id}</strong>
                    <small>
                      {travel.from ?? "?"} → {travel.to ?? "?"}
                    </small>
                  </button>
                  <span>
                    {travel.arrives_at
                      ? new Date(travel.arrives_at).toLocaleString()
                      : "ETA unknown"}
                  </span>
                </li>
              ))}
            </ul>
          )}
          <button
            className="text-button"
            onClick={() => {
              onNavigate("Galaxy");
            }}
          >
            Show on Galaxy →
          </button>
        </section>

        <section className="overview-panel overview-attention">
          <header>
            <h2>Attention</h2>
            <span>{data.notifications.length}</span>
          </header>
          {data.notifications.length === 0 ? (
            <p className="muted">Nothing currently needs attention.</p>
          ) : (
            <ul className="attention-list">
              {data.notifications.map((notice) => (
                <li className={notice.level} key={notice.id}>
                  <strong>{notice.title}</strong>
                  <p>{notice.message}</p>
                </li>
              ))}
            </ul>
          )}
          {data.attention_workflows.map((workflow) => (
            <button
              className="text-button"
              key={workflow.id}
              onClick={() => {
                onSelectWorkflow(workflow.id);
              }}
            >
              {workflow.kind} needs review →
            </button>
          ))}
        </section>

        <section className="overview-panel overview-activity">
          <header>
            <h2>Recent Activity</h2>
            <span>{data.recent_activity.length}</span>
          </header>
          {data.recent_activity.length === 0 ? (
            <p className="muted">No workflow activity recorded.</p>
          ) : (
            <ul className="attention-list">
              {data.recent_activity.map((item) => (
                <li className={item.level} key={item.id}>
                  <button
                    onClick={() => {
                      onSelectWorkflow(item.workflow_id);
                    }}
                  >
                    <strong>{item.step ?? "Workflow"}</strong>
                    <p>{item.message}</p>
                  </button>
                  <time>{new Date(item.occurred_at_ms).toLocaleString()}</time>
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
    </article>
  );
}
