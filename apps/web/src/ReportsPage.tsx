import { useMemo, useState } from "react";

import { ParameterField, validateParameters } from "./AutomationsPage";
import { ResultView } from "./HistoryPage";
import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  EntityRef,
  FiniteExecution,
  ReportDescriptor,
  ReportsSnapshot,
} from "./protocol";

const empty = (snapshot: ReportsSnapshot) => snapshot.reports.length === 0;

export function ReportsPage({
  entities,
  onSelectEntity,
}: {
  entities: Record<string, unknown>;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const query = useDomainQuery({
    slice: "operations",
    fetcher: (signal) => daemonApi.reports(signal),
    isEmpty: empty,
  });
  return (
    <ReportsContent
      {...query}
      entities={entities}
      onSelectEntity={onSelectEntity}
    />
  );
}

function initialValues(descriptor: ReportDescriptor) {
  return Object.fromEntries(
    descriptor.parameters.map((parameter) => [
      parameter.name,
      parameter.default ?? (parameter.kind.type === "boolean" ? false : ""),
    ]),
  );
}

export function ReportsContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  entities,
  onSelectEntity,
}: {
  data?: ReportsSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  entities: Record<string, unknown>;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const [search, setSearch] = useState("");
  const [selectedKind, setSelectedKind] = useState<string>();
  const [values, setValues] = useState<Record<string, unknown>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [running, setRunning] = useState(false);
  const [runError, setRunError] = useState<string>();
  const [result, setResult] = useState<FiniteExecution>();
  const reports = useMemo(
    () =>
      (data?.reports ?? []).filter((report) =>
        [report.kind, report.display_name, report.description, report.category]
          .join(" ")
          .toLowerCase()
          .includes(search.toLowerCase()),
      ),
    [data?.reports, search],
  );
  const selected = data?.reports.find(
    (report) => report.kind === (selectedKind ?? data.reports[0]?.kind),
  );

  if (!data && status === "loading")
    return <article className="page loading-state">Loading Reports…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Reports unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  const choose = (report: ReportDescriptor) => {
    setSelectedKind(report.kind);
    setValues(initialValues(report));
    setErrors({});
    setRunError(undefined);
  };
  const run = async () => {
    if (!selected) return;
    const parameters = { ...initialValues(selected), ...values };
    const validation = validateParameters(selected, parameters);
    setErrors(validation);
    if (Object.keys(validation).length) return;
    setRunning(true);
    setRunError(undefined);
    try {
      setResult(
        await daemonApi.runOperation("report", selected.kind, parameters),
      );
      await refresh();
    } catch (reason) {
      setRunError(String(reason));
    } finally {
      setRunning(false);
    }
  };

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Reports</h1>
          <p className="lede">
            Registered read-only analysis and recent results.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <label className="inventory-search">
        Search reports
        <input
          type="search"
          value={search}
          onChange={(event) => {
            setSearch(event.target.value);
          }}
          placeholder="Name, category, or description"
        />
      </label>
      {!data?.reports.length ? (
        <section className="empty-state">No reports are registered.</section>
      ) : (
        <div className="intelligence-grid">
          <section className="asset-operations" aria-label="Report catalogue">
            {reports.map((report) => (
              <button
                className={selected?.kind === report.kind ? "active" : ""}
                key={report.kind}
                onClick={() => {
                  choose(report);
                }}
              >
                <strong>{report.display_name}</strong>
                <small>{report.category}</small>
                <span>{report.description}</span>
              </button>
            ))}
            {!reports.length && <p>No reports match the search.</p>}
          </section>
          {selected && (
            <section className="operation-form">
              <h2>{selected.display_name}</h2>
              <p>{selected.description}</p>
              <div className="form-grid">
                {selected.parameters.map((parameter) => (
                  <ParameterField
                    key={parameter.name}
                    parameter={parameter}
                    value={values[parameter.name] ?? parameter.default ?? ""}
                    entities={entities}
                    error={errors[parameter.name]}
                    onChange={(value) => {
                      setValues((current) => ({
                        ...current,
                        [parameter.name]: value,
                      }));
                    }}
                  />
                ))}
              </div>
              {runError && <p className="form-error">{runError}</p>}
              <button disabled={running} onClick={() => void run()}>
                {running ? "Running…" : "Run report"}
              </button>
            </section>
          )}
        </div>
      )}
      <section>
        <h2>Recent report executions</h2>
        {data?.executions.length ? (
          <div className="result-links">
            {data.executions.map((execution) => (
              <button
                key={execution.id}
                onClick={() => {
                  setResult(execution);
                }}
              >
                <small>{execution.status}</small> {execution.kind}
              </button>
            ))}
          </div>
        ) : (
          <p className="empty-state">No report executions yet.</p>
        )}
      </section>
      {result && (
        <ResultView execution={result} onSelectEntity={onSelectEntity} />
      )}
    </article>
  );
}
