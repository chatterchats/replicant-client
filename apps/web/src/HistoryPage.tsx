import { useEffect, useRef, useState } from "react";

import { daemonApi } from "./api";
import type { FiniteExecution, EntityRef, WorkflowSummary } from "./protocol";
import {
  executionExport,
  resultLogExcerpt,
  resultSections,
} from "./resultPresentation";

type HistoryFilter = "all" | "workflow" | "action" | "report";

export function ResultView({
  execution,
  onSelectEntity,
  onCancel,
  cancelling = false,
}: {
  execution: FiniteExecution;
  onSelectEntity: (entity: EntityRef) => void;
  onCancel?: () => void;
  cancelling?: boolean;
}) {
  const sections = resultSections(execution.result);
  const excerpt = resultLogExcerpt(execution.result);
  const copy = (value: string) => void navigator.clipboard.writeText(value);
  const exportResult = () => {
    const url = URL.createObjectURL(
      new Blob([executionExport(execution)], { type: "application/json" }),
    );
    const link = document.createElement("a");
    link.href = url;
    link.download = `${execution.operation_class}-${execution.kind}-${execution.id}.json`;
    link.click();
    URL.revokeObjectURL(url);
  };

  return (
    <aside className="result-inspector" aria-label="Execution result">
      <header>
        <div>
          <small>{execution.operation_class}</small>
          <h2>{execution.kind}</h2>
        </div>
        <span className={`result-status ${execution.status}`}>
          {execution.status}
        </span>
      </header>
      <div className="result-summary" aria-label="Result summary">
        <span>
          <strong>{execution.summary.succeeded}</strong> success
        </span>
        <span>
          <strong>{execution.summary.skipped}</strong> skipped
        </span>
        <span>
          <strong>{execution.summary.failed}</strong> failed
        </span>
      </div>
      <div className="result-tools">
        {execution.operation_class === "action" &&
        execution.status === "running" &&
        onCancel ? (
          <button disabled={cancelling} onClick={onCancel}>
            {cancelling ? "Cancelling…" : "Cancel action"}
          </button>
        ) : null}
        <button
          onClick={() => {
            copy(executionExport(execution));
          }}
        >
          Copy result
        </button>
        {excerpt ? (
          <button
            onClick={() => {
              copy(excerpt);
            }}
          >
            Copy log excerpt
          </button>
        ) : null}
        <button onClick={exportResult}>Export JSON</button>
      </div>
      {execution.error ? <p className="form-error">{execution.error}</p> : null}
      {execution.links.length ? (
        <section>
          <h3>Affected</h3>
          <div className="result-links">
            {execution.links.map((entity) => (
              <button
                key={`${entity.kind}:${entity.id}`}
                onClick={() => {
                  onSelectEntity(entity);
                }}
              >
                <small>{entity.kind}</small> {entity.id}
              </button>
            ))}
          </div>
        </section>
      ) : null}
      {sections.map((section) => (
        <section key={section.title}>
          <h3>{section.title}</h3>
          <div className="result-table-scroll">
            <table>
              <thead>
                <tr>
                  {section.columns.map((column) => (
                    <th key={column}>{column.replaceAll("_", " ")}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {section.rows.map((row, index) => (
                  <tr key={index}>
                    {section.columns.map((column) => (
                      <td key={column}>{row[column] ?? "—"}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      ))}
    </aside>
  );
}

export function HistoryPage({
  workflows,
  selectedExecution,
  onSelectWorkflow,
  onSelectEntity,
}: {
  workflows: WorkflowSummary[];
  selectedExecution?: FiniteExecution;
  onSelectWorkflow: (id: string) => void;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const [executions, setExecutions] = useState<FiniteExecution[]>([]);
  const [selected, setSelected] = useState<FiniteExecution>();
  const [filter, setFilter] = useState<HistoryFilter>("all");
  const [error, setError] = useState<string>();
  const [cancelling, setCancelling] = useState<string>();
  const mounted = useRef(true);
  const refreshController = useRef<AbortController | undefined>(undefined);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      refreshController.current?.abort();
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    daemonApi
      .history(controller.signal)
      .then((value) => {
        if (!controller.signal.aborted) setExecutions(value);
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) setError(String(reason));
      });
    return () => {
      controller.abort();
    };
  }, []);
  useEffect(() => {
    if (selectedExecution) {
      setSelected(selectedExecution);
      setExecutions((items) => [
        selectedExecution,
        ...items.filter((item) => item.id !== selectedExecution.id),
      ]);
    }
  }, [selectedExecution]);

  const finite = executions.filter(
    (execution) => filter === "all" || execution.operation_class === filter,
  );
  const rows = [
    ...(filter === "all" || filter === "workflow" ? workflows : []).map(
      (workflow) => ({
        type: "workflow" as const,
        id: workflow.id,
        kind: workflow.kind,
        status: workflow.status,
        time: workflow.updated_at_ms,
      }),
    ),
    ...finite.map((execution) => ({
      type: execution.operation_class,
      id: execution.id,
      kind: execution.kind,
      status: execution.status,
      time: execution.finished_at_ms,
      execution,
    })),
  ].sort((left, right) => right.time - left.time);

  const cancelAction = async (execution: FiniteExecution) => {
    if (!window.confirm(`Cancel ${execution.kind}?`)) return;
    setError(undefined);
    setCancelling(execution.id);
    try {
      await daemonApi.cancelAction(execution.id);
      if (!mounted.current) return;
      refreshController.current?.abort();
      const controller = new AbortController();
      refreshController.current = controller;
      const updated = await daemonApi.history(controller.signal);
      setExecutions(updated);
      setSelected(updated.find((item) => item.id === execution.id));
    } catch (reason) {
      if (mounted.current) setError(String(reason));
    } finally {
      if (mounted.current) setCancelling(undefined);
    }
  };

  return (
    <article className="history-page">
      <header>
        <div>
          <p className="eyebrow">Automation</p>
          <h1>History</h1>
        </div>
        <label>
          Show{" "}
          <select
            value={filter}
            onChange={(event) => {
              setFilter(event.target.value as HistoryFilter);
            }}
          >
            <option value="all">Everything</option>
            <option value="workflow">Workflow runs</option>
            <option value="action">Actions</option>
            <option value="report">Reports</option>
          </select>
        </label>
      </header>
      {error ? <p className="form-error">{error}</p> : null}
      <div className="history-layout">
        <section className="history-list" aria-label="Execution history">
          {rows.length ? (
            rows.map((row) => (
              <button
                key={`${row.type}:${row.id}`}
                onClick={() => {
                  if (row.type === "workflow") onSelectWorkflow(row.id);
                  else setSelected(row.execution);
                }}
              >
                <span>
                  <small>{row.type}</small>
                  <strong>{row.kind}</strong>
                </span>
                <span>
                  <small>{new Date(row.time).toLocaleString()}</small>
                  <span className={`result-status ${row.status}`}>
                    {row.status}
                  </span>
                </span>
              </button>
            ))
          ) : (
            <p className="empty-state">No matching history yet.</p>
          )}
        </section>
        {selected ? (
          <ResultView
            execution={selected}
            onSelectEntity={onSelectEntity}
            onCancel={() => void cancelAction(selected)}
            cancelling={cancelling === selected.id}
          />
        ) : (
          <aside className="result-inspector empty-state">
            Select an action or report to inspect its structured result.
          </aside>
        )}
      </div>
    </article>
  );
}
