import { useMemo } from "react";

import { daemonApi } from "./api";
import { descriptorCommands, type DescriptorCommand } from "./CommandPalette";
import { useDomainQuery } from "./domainQuery";
import type {
  DescriptorCatalog,
  EntityRef,
  SimulationRunSummary,
} from "./protocol";

function command(
  descriptors: DescriptorCatalog,
  kind: string,
  initialParameters: Record<string, unknown>,
): DescriptorCommand | undefined {
  const found = descriptorCommands(descriptors).find(
    (item) => item.descriptor.kind === kind,
  );
  return found ? { ...found, initialParameters } : undefined;
}

function outcome(run: SimulationRunSummary) {
  if (run.completed_at) return "completed";
  if (run.abandoned_at) return "abandoned";
  if (run.timed_out_at) return "timed out";
  return run.lifecycle ?? "archived";
}

function score(run: SimulationRunSummary) {
  const details = [
    run.score_seconds !== null ? `${String(run.score_seconds)}s score` : null,
    run.resources_mined !== null
      ? `${String(run.resources_mined)} mined`
      : null,
    run.devices_printed !== null
      ? `${String(run.devices_printed)} printed`
      : null,
  ].filter((value): value is string => value !== null);
  return details.join(" · ") || "—";
}

export function SimulationsPage({
  descriptors,
  onSelectEntity,
  onRunCommand,
  onOpenLeaderboards,
}: {
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onRunCommand: (command: DescriptorCommand) => void;
  onOpenLeaderboards: () => void;
}) {
  const { data, status, error, refreshing, refresh } = useDomainQuery({
    slice: "simulations",
    fetcher: (signal) => daemonApi.simulations(signal),
    isEmpty: (snapshot) =>
      snapshot.interfaces.length === 0 && snapshot.account_history.length === 0,
  });
  const start = useMemo(
    () =>
      descriptorCommands(descriptors).find(
        (item) => item.descriptor.kind === "simulation.start",
      ),
    [descriptors],
  );

  if (!data && status === "loading") {
    return (
      <article className="page loading-state">Loading Simulations…</article>
    );
  }
  if (!data && status === "error") {
    return (
      <article className="page error-state">
        <h1>Simulations unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );
  }

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Missions</p>
          <h1>Simulations</h1>
          <p className="lede">
            Datacentre scenarios, active runs, personal history, and competitive
            boards.
          </p>
        </div>
        <div className="asset-operations">
          <button onClick={onOpenLeaderboards}>Scenario leaderboards</button>
          <button disabled={refreshing} onClick={() => void refresh()}>
            {refreshing ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}

      {!data?.interfaces.length ? (
        <section className="empty-state">
          No owned replicant interfaces discovered. Travel to a datacentre and
          refresh device state.
        </section>
      ) : (
        data.interfaces.map((item) => (
          <section className="connection-card" key={item.device.entity.id}>
            <header className="page-heading">
              <div>
                <h2>{item.device.entity.id}</h2>
                <p>
                  {item.device.location ??
                    item.device.system ??
                    "Unknown datacentre"}
                </p>
              </div>
              <button
                onClick={() => {
                  onSelectEntity(item.device.entity);
                }}
              >
                Inspect
              </button>
            </header>
            {item.error && <p className="inline-warning">{item.error}</p>}
            <div className="inventory-table-wrap">
              <table className="inventory-table">
                <thead>
                  <tr>
                    <th>Scenario</th>
                    <th>Objective</th>
                    <th>Timeout</th>
                    <th>Entry cost</th>
                    <th>Action</th>
                  </tr>
                </thead>
                <tbody>
                  {item.scenarios.map((scenario) => (
                    <tr key={scenario.code}>
                      <td>
                        <strong>{scenario.name ?? scenario.code}</strong>
                        <small>{scenario.description}</small>
                      </td>
                      <td>
                        {scenario.objective_type ?? "—"}
                        {scenario.objective_target !== null
                          ? ` · ${String(scenario.objective_target)}`
                          : ""}
                      </td>
                      <td>
                        {scenario.timeout_hours !== null
                          ? `${String(scenario.timeout_hours)} h`
                          : "—"}
                      </td>
                      <td>
                        {scenario.entry_cost
                          .map(
                            (cost) =>
                              `${String(cost.quantity)} ${cost.resource}`,
                          )
                          .join(" + ") || "None"}
                      </td>
                      <td>
                        {start && (
                          <button
                            onClick={() => {
                              onRunCommand({
                                ...start,
                                initialParameters: {
                                  interface: item.device.entity.id,
                                  scenario: scenario.code,
                                },
                              });
                            }}
                          >
                            Start
                          </button>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            {item.active.map((run) => {
              const abandon = command(descriptors, "simulation.abandon", {
                interface: item.device.entity.id,
                simulation_id: run.id,
              });
              return (
                <div className="connection-card" key={run.id}>
                  <strong>
                    Run #{run.id} · {run.scenario_name ?? run.scenario_code}
                  </strong>
                  <p>
                    {run.replicant_name ?? "Unknown replicant"} ·{" "}
                    {run.started_at ?? "Unknown start"}
                  </p>
                  {run.is_mine && abandon && (
                    <button
                      onClick={() => {
                        onRunCommand(abandon);
                      }}
                    >
                      Abandon
                    </button>
                  )}
                </div>
              );
            })}
          </section>
        ))
      )}

      <section className="asset-jobs">
        <h2>Personal history</h2>
        {!data?.account_history.length ? (
          <p>No archived simulation runs.</p>
        ) : (
          <div className="inventory-table-wrap">
            <table className="inventory-table">
              <thead>
                <tr>
                  <th>Run</th>
                  <th>Scenario</th>
                  <th>Started</th>
                  <th>Finished</th>
                  <th>Outcome</th>
                  <th>Score / output</th>
                </tr>
              </thead>
              <tbody>
                {data.account_history.map((run) => (
                  <tr key={run.id}>
                    <td>#{run.id}</td>
                    <td>{run.scenario_name ?? run.scenario_code ?? "—"}</td>
                    <td>{run.started_at ?? "—"}</td>
                    <td>
                      {run.completed_at ??
                        run.abandoned_at ??
                        run.timed_out_at ??
                        "—"}
                    </td>
                    <td>{outcome(run)}</td>
                    <td>{score(run)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </article>
  );
}
