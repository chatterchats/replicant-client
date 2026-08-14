/* eslint-disable react-refresh/only-export-components */
import { useEffect, useMemo, useState } from "react";

import { daemonApi } from "./api";
import type {
  EntityKind,
  OperationDescriptor,
  ParameterDescriptor,
  WorkflowActivity,
  WorkflowDescriptor,
  WorkflowDetail,
  WorkflowStatus,
  WorkflowSummary,
} from "./protocol";

const tabs = ["Active", "Templates", "Schedules", "History"] as const;
type Tab = (typeof tabs)[number];
type Values = Record<string, unknown>;

function stringValue(value: unknown) {
  return typeof value === "string" ||
    (typeof value === "number" && Number.isFinite(value))
    ? String(value)
    : "";
}

const activeStatuses: WorkflowStatus[] = [
  "queued",
  "running",
  "waiting",
  "paused",
  "reconciling",
];

function entityIds(entities: Record<string, unknown>, kind: EntityKind) {
  const prefix = `${kind}:`;
  return Object.keys(entities)
    .filter((key) => key.startsWith(prefix))
    .map((key) => key.slice(prefix.length));
}

function deviceTypes(entities: Record<string, unknown>) {
  const values = Object.entries(entities)
    .filter(([key]) => key.startsWith("device:"))
    .map(([, value]) => {
      if (typeof value !== "object" || value === null) return null;
      const device = value as Record<string, unknown>;
      return device.device_type ?? device.type ?? device.kind;
    })
    .filter((value): value is string => typeof value === "string");
  return [...new Set(values)].sort();
}

function optionsFor(
  parameter: ParameterDescriptor,
  entities: Record<string, unknown>,
) {
  switch (parameter.kind.type) {
    case "system":
    case "location":
    case "replicant":
    case "device":
      return entityIds(entities, parameter.kind.type);
    case "device_type":
      return deviceTypes(entities);
    case "entity":
      return entityIds(entities, parameter.kind.entity_kind);
    default:
      return [];
  }
}

export function validateParameters(
  descriptor: OperationDescriptor,
  values: Values,
) {
  const errors: Record<string, string> = {};
  for (const parameter of descriptor.parameters) {
    const value = values[parameter.name];
    const empty = value === "" || value === null || value === undefined;
    if (parameter.required && empty) {
      errors[parameter.name] = "Required";
      continue;
    }
    if (empty) continue;
    if (parameter.kind.type === "integer" && !Number.isInteger(Number(value)))
      errors[parameter.name] = "Enter a whole number";
    if (
      (parameter.kind.type === "integer" || parameter.kind.type === "number") &&
      !Number.isFinite(Number(value))
    )
      errors[parameter.name] = "Enter a number";
    const numeric = Number(value);
    if (
      parameter.validation.minimum !== null &&
      numeric < parameter.validation.minimum
    )
      errors[parameter.name] =
        `Minimum ${String(parameter.validation.minimum)}`;
    if (
      parameter.validation.maximum !== null &&
      numeric > parameter.validation.maximum
    )
      errors[parameter.name] =
        `Maximum ${String(parameter.validation.maximum)}`;
    if (
      parameter.validation.min_length !== null &&
      stringValue(value).length < parameter.validation.min_length
    )
      errors[parameter.name] =
        `Use at least ${String(parameter.validation.min_length)} characters`;
    if (
      parameter.validation.max_length !== null &&
      stringValue(value).length > parameter.validation.max_length
    )
      errors[parameter.name] =
        `Use at most ${String(parameter.validation.max_length)} characters`;
  }
  return errors;
}

export function ParameterField({
  parameter,
  value,
  entities,
  error,
  onChange,
}: {
  parameter: ParameterDescriptor;
  value: unknown;
  entities: Record<string, unknown>;
  error?: string;
  onChange: (value: unknown) => void;
}) {
  const id = `workflow-${parameter.name}`;
  const options = optionsFor(parameter, entities);
  const help = error ?? parameter.description;
  if (parameter.kind.type === "boolean") {
    return (
      <label className="boolean-field" htmlFor={id}>
        <input
          id={id}
          name={parameter.name}
          type="checkbox"
          checked={Boolean(value)}
          onChange={(event) => {
            onChange(event.target.checked);
          }}
        />
        <span>
          <strong>{parameter.label}</strong>
          <small className={error ? "field-error" : ""}>{help}</small>
        </span>
      </label>
    );
  }
  if (parameter.kind.type === "enum") {
    return (
      <label htmlFor={id}>
        {parameter.label}
        <select
          id={id}
          name={parameter.name}
          required={parameter.required}
          value={stringValue(value)}
          onChange={(event) => {
            onChange(event.target.value);
          }}
        >
          <option value="">Select…</option>
          {parameter.options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        <small className={error ? "field-error" : ""}>{help}</small>
      </label>
    );
  }
  const numeric =
    parameter.kind.type === "integer" || parameter.kind.type === "number";
  const semantic = options.length > 0;
  return (
    <label htmlFor={id}>
      {parameter.label}
      <input
        id={id}
        name={parameter.name}
        type={numeric ? "number" : "text"}
        step={
          parameter.kind.type === "integer" ? 1 : numeric ? "any" : undefined
        }
        min={parameter.validation.minimum ?? undefined}
        max={parameter.validation.maximum ?? undefined}
        minLength={parameter.validation.min_length ?? undefined}
        maxLength={parameter.validation.max_length ?? undefined}
        list={semantic ? `${id}-options` : undefined}
        required={parameter.required}
        value={stringValue(value)}
        onChange={(event) => {
          onChange(
            numeric && event.target.value !== ""
              ? event.target.valueAsNumber
              : event.target.value,
          );
        }}
      />
      {semantic ? (
        <datalist id={`${id}-options`}>
          {options.map((option) => (
            <option key={option} value={option} />
          ))}
        </datalist>
      ) : null}
      <small className={error ? "field-error" : ""}>{help}</small>
    </label>
  );
}

function WorkflowForm({
  descriptor,
  entities,
  onStarted,
}: {
  descriptor: WorkflowDescriptor;
  entities: Record<string, unknown>;
  onStarted: (workflow: WorkflowSummary) => void;
}) {
  const [values, setValues] = useState<Values>(() =>
    Object.fromEntries(
      descriptor.parameters.map((item) => [item.name, item.default ?? ""]),
    ),
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [submitting, setSubmitting] = useState(false);
  const [serverError, setServerError] = useState<string | null>(null);

  return (
    <form
      className="workflow-form"
      onSubmit={(event) => {
        event.preventDefault();
        const nextErrors = validateParameters(descriptor, values);
        setErrors(nextErrors);
        if (Object.keys(nextErrors).length) return;
        setSubmitting(true);
        setServerError(null);
        const parameters = Object.fromEntries(
          Object.entries(values).filter(([, value]) => value !== ""),
        );
        void daemonApi
          .startWorkflow(descriptor.kind, parameters)
          .then(onStarted)
          .catch((error: unknown) => {
            setServerError(String(error));
          })
          .finally(() => {
            setSubmitting(false);
          });
      }}
    >
      <header>
        <span className={`risk ${descriptor.risk}`}>
          {descriptor.risk} risk
        </span>
        <h2>{descriptor.display_name}</h2>
        <p>{descriptor.description}</p>
      </header>
      <div className="form-grid">
        {descriptor.parameters.map((parameter) => (
          <ParameterField
            key={parameter.name}
            parameter={parameter}
            value={values[parameter.name]}
            entities={entities}
            error={errors[parameter.name]}
            onChange={(value) => {
              setValues((current) => ({ ...current, [parameter.name]: value }));
            }}
          />
        ))}
      </div>
      {serverError ? <p className="form-error">{serverError}</p> : null}
      <button className="primary" disabled={submitting} type="submit">
        {submitting ? "Starting…" : "Start workflow"}
      </button>
    </form>
  );
}

function targetSummary(
  detail: WorkflowDetail | undefined,
  descriptor: WorkflowDescriptor | undefined,
) {
  if (!detail) return "Loading targets…";
  const targetNames = new Set(
    descriptor?.parameters
      .filter((item) =>
        [
          "system",
          "location",
          "replicant",
          "device",
          "device_type",
          "entity",
        ].includes(item.kind.type),
      )
      .map((item) => item.name) ?? [],
  );
  const parameters = Object.entries(detail.parameters);
  const targets = (
    targetNames.size
      ? parameters.filter(([key]) => targetNames.has(key))
      : parameters
  )
    .filter(
      ([, value]) => typeof value === "string" || typeof value === "number",
    )
    .slice(0, 4)
    .map(([key, value]) => `${key}: ${String(value)}`);
  targets.push(...detail.claims.map((claim) => `${claim.kind}: ${claim.id}`));
  return targets.join(" · ") || "No configured targets";
}

function WorkflowRow({
  workflow,
  descriptor,
  detail,
  selected,
  onSelect,
  onControl,
}: {
  workflow: WorkflowSummary;
  descriptor?: WorkflowDescriptor;
  detail?: WorkflowDetail;
  selected: boolean;
  onSelect: () => void;
  onControl: (action: "pause" | "resume" | "cancel") => void;
}) {
  const active = activeStatuses.includes(workflow.status);
  return (
    <article className={`workflow-row ${selected ? "selected" : ""}`}>
      <button className="workflow-row-main" onClick={onSelect}>
        <span>
          <small>{workflow.kind}</small>
          <strong>{descriptor?.display_name ?? workflow.kind}</strong>
        </span>
        <span className={`workflow-status ${workflow.status}`}>
          {workflow.status}
        </span>
        <span>
          <small>Current step</small>
          {workflow.current_step ?? "Not started"}
        </span>
        <span>
          <small>Targets / resources</small>
          {targetSummary(detail, descriptor)}
        </span>
        <span>
          <small>{detail?.error ? "Error" : "Wait reason"}</small>
          {detail?.error ?? detail?.wait_reason ?? "—"}
        </span>
        <time dateTime={new Date(workflow.updated_at_ms).toISOString()}>
          {new Date(workflow.updated_at_ms).toLocaleString()}
        </time>
      </button>
      {active ? (
        <div className="workflow-actions">
          {workflow.status === "paused" ? (
            <button
              onClick={() => {
                onControl("resume");
              }}
            >
              Resume
            </button>
          ) : (
            <button
              onClick={() => {
                onControl("pause");
              }}
            >
              Pause
            </button>
          )}
          <button
            className="danger"
            onClick={() => {
              onControl("cancel");
            }}
          >
            Cancel
          </button>
        </div>
      ) : null}
    </article>
  );
}

function WorkflowInspector({
  detail,
  activity,
  onClose,
}: {
  detail: WorkflowDetail;
  activity: WorkflowActivity[];
  onClose: () => void;
}) {
  return (
    <aside className="workflow-inspector" aria-label="Workflow details">
      <header className="drawer-header">
        <div>
          <small>{detail.summary.kind}</small>
          <strong>{detail.summary.id}</strong>
        </div>
        <button aria-label="Close workflow details" onClick={onClose}>
          ×
        </button>
      </header>
      <section>
        <h3>Claimed resources</h3>
        {detail.claims.length ? (
          <ul className="claims">
            {detail.claims.map((claim) => (
              <li key={`${claim.kind}:${claim.id}`}>
                <small>{claim.kind}</small> {claim.id}
              </li>
            ))}
          </ul>
        ) : (
          <p className="empty-state">No resources claimed.</p>
        )}
      </section>
      <section>
        <h3>Activity timeline</h3>
        <ol className="timeline">
          {activity.map((item) => (
            <li className={item.level} key={item.id}>
              <time dateTime={new Date(item.occurred_at_ms).toISOString()}>
                {new Date(item.occurred_at_ms).toLocaleString()}
              </time>
              <strong>{item.step ?? item.level}</strong>
              <p>{item.message}</p>
            </li>
          ))}
        </ol>
      </section>
    </aside>
  );
}

export function AutomationsPage({
  workflows,
  entities,
  selectedWorkflowId,
}: {
  workflows: WorkflowSummary[];
  entities: Record<string, unknown>;
  selectedWorkflowId?: string;
}) {
  const [tab, setTab] = useState<Tab>("Active");
  const [descriptors, setDescriptors] = useState<WorkflowDescriptor[]>([]);
  const [selectedTemplate, setSelectedTemplate] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [created, setCreated] = useState<WorkflowSummary[]>([]);
  const [details, setDetails] = useState<Record<string, WorkflowDetail>>({});
  const [activity, setActivity] = useState<WorkflowActivity[]>([]);
  const [error, setError] = useState<string | null>(null);
  const currentWorkflows = useMemo(
    () => [
      ...workflows,
      ...created.filter(
        (item) => !workflows.some((workflow) => workflow.id === item.id),
      ),
    ],
    [created, workflows],
  );
  const workflowVersion = currentWorkflows
    .map(({ id, revision }) => `${id}:${String(revision)}`)
    .join();

  useEffect(() => {
    if (!selectedWorkflowId) return;
    setTab("Active");
    setSelectedId(selectedWorkflowId);
  }, [selectedWorkflowId]);

  useEffect(() => {
    const controller = new AbortController();
    void daemonApi
      .descriptors(controller.signal)
      .then((catalog) => {
        setDescriptors(catalog.workflows);
      })
      .catch((reason: unknown) => {
        setError(String(reason));
      });
    return () => {
      controller.abort();
    };
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    void Promise.all(
      currentWorkflows.map((workflow) =>
        daemonApi.workflow(workflow.id, controller.signal),
      ),
    )
      .then((items) => {
        setDetails(
          Object.fromEntries(items.map((item) => [item.summary.id, item])),
        );
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) setError(String(reason));
      });
    return () => {
      controller.abort();
    };
  }, [currentWorkflows]);

  useEffect(() => {
    if (!selectedId) {
      setActivity([]);
      return;
    }
    const controller = new AbortController();
    void daemonApi
      .workflowActivity(selectedId, controller.signal)
      .then(setActivity)
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) setError(String(reason));
      });
    return () => {
      controller.abort();
    };
  }, [selectedId, workflowVersion]);

  const descriptorsByKind = useMemo(
    () => Object.fromEntries(descriptors.map((item) => [item.kind, item])),
    [descriptors],
  );
  const selectedDescriptor = descriptors.find(
    (item) => item.kind === selectedTemplate,
  );
  const visible = currentWorkflows.filter((workflow) =>
    tab === "Active"
      ? activeStatuses.includes(workflow.status)
      : !activeStatuses.includes(workflow.status),
  );

  const control = (
    workflow: WorkflowSummary,
    action: "pause" | "resume" | "cancel",
  ) => {
    setError(null);
    void daemonApi
      .controlWorkflow(workflow.id, action)
      .catch((reason: unknown) => {
        setError(String(reason));
      });
  };

  return (
    <div className="automation-center">
      <header className="automation-heading">
        <div>
          <p className="eyebrow">Automation</p>
          <h1>Automations</h1>
          <p className="lede">
            Durable workflows continue through frontend disconnects and daemon
            restarts.
          </p>
        </div>
        <button
          className="primary"
          onClick={() => {
            setTab("Templates");
          }}
        >
          Start workflow
        </button>
      </header>
      <nav className="automation-tabs" aria-label="Automation views">
        {tabs.map((item) => (
          <button
            className={tab === item ? "active" : ""}
            key={item}
            onClick={() => {
              setTab(item);
            }}
          >
            {item}
          </button>
        ))}
      </nav>
      {error ? (
        <p className="form-error" role="alert">
          {error}
        </p>
      ) : null}

      {tab === "Templates" ? (
        <div className="templates-layout">
          <div className="template-list">
            {descriptors.map((descriptor) => (
              <button
                key={descriptor.kind}
                onClick={() => {
                  setSelectedTemplate(descriptor.kind);
                }}
              >
                <small>{descriptor.category}</small>
                <strong>{descriptor.display_name}</strong>
                <span>{descriptor.description}</span>
              </button>
            ))}
          </div>
          {selectedDescriptor ? (
            <WorkflowForm
              key={selectedDescriptor.kind}
              descriptor={selectedDescriptor}
              entities={entities}
              onStarted={(workflow) => {
                setCreated((items) => [...items, workflow]);
                setTab("Active");
                setSelectedId(workflow.id);
              }}
            />
          ) : (
            <p className="empty-state">
              Select a workflow template to configure it.
            </p>
          )}
        </div>
      ) : tab === "Schedules" ? (
        <section className="placeholder-card">
          <h2>Schedules</h2>
          <p>
            Schedule-capable workflow descriptors will appear here when
            persisted triggers arrive.
          </p>
          <small>
            {
              descriptors.filter((item) =>
                item.supported_triggers.includes("schedule"),
              ).length
            }{" "}
            templates currently support schedules.
          </small>
        </section>
      ) : (
        <section className="workflow-list" aria-label={`${tab} workflows`}>
          {visible.length ? (
            visible.map((workflow) => (
              <WorkflowRow
                key={workflow.id}
                workflow={workflow}
                descriptor={descriptorsByKind[workflow.kind]}
                detail={details[workflow.id]}
                selected={selectedId === workflow.id}
                onSelect={() => {
                  setSelectedId(workflow.id);
                }}
                onControl={(action) => {
                  control(workflow, action);
                }}
              />
            ))
          ) : (
            <p className="empty-state">No {tab.toLowerCase()} workflows.</p>
          )}
        </section>
      )}

      {selectedId && details[selectedId] ? (
        <WorkflowInspector
          detail={details[selectedId]}
          activity={activity}
          onClose={() => {
            setSelectedId(null);
          }}
        />
      ) : null}
    </div>
  );
}
