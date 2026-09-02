/* eslint-disable react-refresh/only-export-components */
import { useEffect, useMemo, useRef, useState } from "react";

import { daemonApi } from "./api";
import { useDomainQuery } from "./domainQuery";
import type {
  AutomationTrigger,
  DirectorGoalKind,
  DirectorGoalSummary,
  DirectorRequirementKind,
  DirectorSnapshot,
  EntityKind,
  OperationDescriptor,
  ParameterDescriptor,
  TriggerCondition,
  TriggerRequest,
  WorkflowActivity,
  WorkflowDescriptor,
  WorkflowDetail,
  WorkflowStatus,
  WorkflowSummary,
} from "./protocol";

const tabs = [
  "Director",
  "Active",
  "Templates",
  "Schedules",
  "History",
] as const;
type Tab = (typeof tabs)[number];
type Values = Record<string, unknown>;
type LogisticsPayloadDraft = {
  id: number;
  kind: "resource" | "device" | "tag";
  item: string;
  quantity: number;
};

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

type SmartOption = { value: string; label: string };

function entityIds(entities: Record<string, unknown>, kind: EntityKind) {
  const prefix = `${kind}:`;
  return Object.keys(entities)
    .filter((key) => key.startsWith(prefix))
    .map((key) => key.slice(prefix.length));
}

function entityOptions(
  entities: Record<string, unknown>,
  kind: EntityKind,
): SmartOption[] {
  const prefix = `${kind}:`;
  return Object.entries(entities)
    .filter(([key]) => key.startsWith(prefix))
    .map(([key, value]) => {
      const id = key.slice(prefix.length);
      if (typeof value !== "object" || value === null)
        return { value: id, label: id };
      const summary = value as Record<string, unknown>;
      const label = typeof summary.label === "string" ? summary.label : id;
      const secondary =
        typeof summary.secondary_label === "string"
          ? summary.secondary_label
          : null;
      return {
        value: id,
        label:
          label === id
            ? secondary
              ? `${label} · ${secondary}`
              : label
            : `${label} (${id})`,
      };
    })
    .sort((left, right) => left.label.localeCompare(right.label));
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
  operationKind?: string,
  blueprintTypes: string[] = [],
): SmartOption[] {
  const entityKind = parameter.kind.type;
  if (operationKind === "device.change_owner" && parameter.name === "target") {
    return entityOptions(entities, "replicant");
  }
  if (entityKind === "replicant") return entityOptions(entities, "replicant");
  if (entityKind === "device_type") {
    const values =
      blueprintTypes.length > 0 ? blueprintTypes : deviceTypes(entities);
    return values.map((value) => ({ value, label: value }));
  }
  if (entityKind === "device") {
    let options = entityOptions(entities, "device");
    const deviceType = (id: string) => {
      const value = entities[`device:${id}`];
      if (typeof value !== "object" || value === null) return null;
      const summary = value as Record<string, unknown>;
      return typeof summary.device_type === "string"
        ? summary.device_type
        : typeof summary.entity_type === "string"
          ? summary.entity_type
          : null;
    };
    if (operationKind === "replicant.teleport" && parameter.name === "target") {
      options = options.filter(
        (option) => deviceType(option.value) === "empty_replicant_matrix",
      );
    } else if (
      operationKind === "clone.replicate" &&
      parameter.name === "target"
    ) {
      options = options.filter(
        (option) => deviceType(option.value) === "empty_replicant_matrix",
      );
    } else if (
      operationKind === "clone.replicate" &&
      parameter.name === "source"
    ) {
      options = options.filter(
        (option) => deviceType(option.value) === "replicant_matrix",
      );
    } else if (
      operationKind === "clone.stow_target" &&
      parameter.name === "matrix"
    ) {
      options = options.filter(
        (option) => deviceType(option.value) === "empty_replicant_matrix",
      );
    } else if (
      operationKind === "autofactory.print" &&
      parameter.name === "device"
    ) {
      options = options.filter(
        (option) => deviceType(option.value) === "autofactory",
      );
    }
    return options;
  }
  if (entityKind === "system" || entityKind === "location") {
    return entityOptions(entities, entityKind);
  }
  if (entityKind === "entity")
    return entityOptions(entities, parameter.kind.entity_kind);
  return [];
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
  operationKind,
  blueprintTypes = [],
}: {
  parameter: ParameterDescriptor;
  value: unknown;
  entities: Record<string, unknown>;
  error?: string;
  onChange: (value: unknown) => void;
  operationKind?: string;
  blueprintTypes?: string[];
}) {
  const id = `workflow-${parameter.name}`;
  const options = optionsFor(
    parameter,
    entities,
    operationKind,
    blueprintTypes,
  );
  const help = error ?? parameter.description;
  const allowsAllDevices =
    operationKind === "device.detach" && parameter.name === "target";
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
          <option value="">
            {allowsAllDevices ? "All devices" : "Select…"}
          </option>
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
  const restrictedDevice =
    parameter.kind.type === "device" &&
    (allowsAllDevices ||
      (operationKind === "replicant.teleport" && parameter.name === "target") ||
      (operationKind === "clone.replicate" &&
        (parameter.name === "target" || parameter.name === "source")) ||
      (operationKind === "clone.stow_target" && parameter.name === "matrix") ||
      (operationKind === "autofactory.print" && parameter.name === "device"));
  const useSelect =
    parameter.kind.type === "replicant" ||
    parameter.kind.type === "device_type" ||
    restrictedDevice ||
    (operationKind === "device.change_owner" && parameter.name === "target");
  const emptyHint =
    operationKind === "replicant.teleport" && parameter.name === "target"
      ? "No empty matrices available"
      : `No ${parameter.label.toLowerCase()} available`;
  if (useSelect) {
    return (
      <label htmlFor={id}>
        {parameter.label}
        <select
          id={id}
          name={parameter.name}
          required={parameter.required}
          disabled={options.length === 0 && !allowsAllDevices}
          value={stringValue(value)}
          onChange={(event) => {
            onChange(event.target.value);
          }}
        >
          <option value="">
            {allowsAllDevices
              ? "All devices"
              : options.length > 0
                ? "Select…"
                : emptyHint}
          </option>
          {options.map((option) => (
            <option key={option.value} value={option.value}>
              {option.label}
            </option>
          ))}
        </select>
        <small className={error ? "field-error" : ""}>
          {options.length === 0 && !allowsAllDevices ? emptyHint : help}
        </small>
      </label>
    );
  }
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
            <option
              key={option.value}
              value={option.value}
              label={option.label}
            />
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
            operationKind={descriptor.kind}
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

export function LogisticsWorkflowForm({
  descriptor,
  entities,
  onStarted,
  initialOrigin = "",
}: {
  descriptor: WorkflowDescriptor;
  entities: Record<string, unknown>;
  onStarted: (workflow: WorkflowSummary) => void;
  initialOrigin?: string;
}) {
  const [origin, setOrigin] = useState(initialOrigin);
  const [destination, setDestination] = useState("");
  const [returnTransports, setReturnTransports] = useState(false);
  const [payloads, setPayloads] = useState<LogisticsPayloadDraft[]>([
    { id: 1, kind: "resource", item: "", quantity: 1 },
  ]);
  const [nextId, setNextId] = useState(2);
  const [serverError, setServerError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const locations = entityIds(entities, "location");
  const systems = entityIds(entities, "system");
  const deviceTypeOptions = deviceTypes(entities);
  const locationOptions = [...new Set([...locations, ...systems])].sort();

  const updatePayload = (
    id: number,
    update: Partial<Omit<LogisticsPayloadDraft, "id">>,
  ) => {
    setPayloads((current) =>
      current.map((payload) =>
        payload.id === id ? { ...payload, ...update } : payload,
      ),
    );
  };

  return (
    <form
      className="workflow-form"
      onSubmit={(event) => {
        event.preventDefault();
        const activePayloads = payloads.filter(
          (payload) => payload.item.trim().length > 0,
        );
        if (!origin.trim() || !destination.trim()) {
          setServerError("Origin and destination are required.");
          return;
        }
        if (activePayloads.length === 0) {
          setServerError("Add at least one resource, device, or tag payload.");
          return;
        }
        if (
          activePayloads.some(
            (payload) => payload.kind !== "tag" && payload.quantity < 1,
          )
        ) {
          setServerError("Resource and device quantities must be at least 1.");
          return;
        }

        const resources: Record<string, number> = {};
        const devices: { device_type: string; quantity: number }[] = [];
        const deviceTags: string[] = [];
        for (const payload of activePayloads) {
          const item = payload.item.trim();
          if (payload.kind === "resource") {
            resources[item] = (resources[item] ?? 0) + payload.quantity;
          } else if (payload.kind === "device") {
            devices.push({ device_type: item, quantity: payload.quantity });
          } else {
            deviceTags.push(item);
          }
        }

        setSubmitting(true);
        setServerError(null);
        void daemonApi
          .startWorkflow(descriptor.kind, {
            origin: origin.trim(),
            destination: destination.trim(),
            resources,
            devices,
            device_tags: [...new Set(deviceTags)],
            return_transports: returnTransports,
          })
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
        <p>
          Move a mixed manifest of resources, device types, and tagged devices
          in one durable delivery.
        </p>
      </header>
      <div className="form-grid">
        <label>
          Origin
          <input
            required
            list="logistics-origin-options"
            value={origin}
            onChange={(event) => {
              setOrigin(event.target.value);
            }}
          />
        </label>
        <label>
          Destination
          <input
            required
            list="logistics-destination-options"
            value={destination}
            onChange={(event) => {
              setDestination(event.target.value);
            }}
          />
        </label>
        <datalist id="logistics-origin-options">
          {locationOptions.map((option) => (
            <option key={option} value={option} />
          ))}
        </datalist>
        <datalist id="logistics-destination-options">
          {locations.map((option) => (
            <option key={option} value={option} />
          ))}
        </datalist>
      </div>
      <fieldset className="manifest-builder">
        <legend>Payload manifest</legend>
        {payloads.map((payload) => (
          <div className="manifest-row" key={payload.id}>
            <select
              aria-label="Payload kind"
              value={payload.kind}
              onChange={(event) => {
                updatePayload(payload.id, {
                  kind: event.target.value as LogisticsPayloadDraft["kind"],
                  quantity: 1,
                });
              }}
            >
              <option value="resource">Resource</option>
              <option value="device">Device type</option>
              <option value="tag">Device tag</option>
            </select>
            <input
              aria-label="Payload item"
              list={
                payload.kind === "device"
                  ? `logistics-device-types-${String(payload.id)}`
                  : undefined
              }
              placeholder={
                payload.kind === "resource"
                  ? "resource type"
                  : payload.kind === "device"
                    ? "device type"
                    : "tag"
              }
              value={payload.item}
              onChange={(event) => {
                updatePayload(payload.id, { item: event.target.value });
              }}
            />
            {payload.kind === "device" ? (
              <datalist id={`logistics-device-types-${String(payload.id)}`}>
                {deviceTypeOptions.map((option) => (
                  <option key={option} value={option} />
                ))}
              </datalist>
            ) : null}
            {payload.kind === "tag" ? (
              <span className="manifest-tag-quantity">all matching</span>
            ) : (
              <input
                aria-label="Payload quantity"
                min={1}
                type="number"
                value={payload.quantity}
                onChange={(event) => {
                  updatePayload(payload.id, {
                    quantity: Number.isFinite(event.target.valueAsNumber)
                      ? event.target.valueAsNumber
                      : 1,
                  });
                }}
              />
            )}
            <button
              disabled={payloads.length === 1}
              type="button"
              onClick={() => {
                setPayloads((current) =>
                  current.filter((item) => item.id !== payload.id),
                );
              }}
            >
              Remove
            </button>
          </div>
        ))}
        <button
          type="button"
          onClick={() => {
            setPayloads((current) => [
              ...current,
              { id: nextId, kind: "resource", item: "", quantity: 1 },
            ]);
            setNextId((current) => current + 1);
          }}
        >
          + Add payload
        </button>
      </fieldset>
      <label className="boolean-field">
        <input
          type="checkbox"
          checked={returnTransports}
          onChange={(event) => {
            setReturnTransports(event.target.checked);
          }}
        />
        <span>
          <strong>Return transports</strong>
          <small>Send carriers back to the origin after delivery.</small>
        </span>
      </label>
      {serverError ? <p className="form-error">{serverError}</p> : null}
      <button className="primary" disabled={submitting} type="submit">
        {submitting ? "Starting…" : "Start workflow"}
      </button>
    </form>
  );
}

function TriggersView({
  descriptors,
  entities,
}: {
  descriptors: OperationDescriptor[];
  entities: Record<string, unknown>;
}) {
  const targets = descriptors.filter(
    (descriptor) => descriptor.operation_class !== "report",
  );
  const [triggers, setTriggers] = useState<AutomationTrigger[]>([]);
  const [name, setName] = useState("");
  const [kind, setKind] = useState<TriggerCondition["kind"]>("schedule");
  const [targetKey, setTargetKey] = useState("");
  const [values, setValues] = useState<Values>({});
  const [interval, setInterval] = useState(3600);
  const [eventName, setEventName] = useState("");
  const [deviceCode, setDeviceCode] = useState("");
  const [minimumRevision, setMinimumRevision] = useState(0);
  const [parentKind, setParentKind] = useState("");
  const [parentStatus, setParentStatus] = useState<WorkflowStatus>("succeeded");
  const [enabled, setEnabled] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const selected = targets.find(
    (descriptor) =>
      `${descriptor.operation_class}:${descriptor.kind}` === targetKey,
  );
  const mounted = useRef(true);
  const triggerController = useRef<AbortController | undefined>(undefined);

  const reload = () => {
    if (!mounted.current) return Promise.resolve();
    triggerController.current?.abort();
    const controller = new AbortController();
    triggerController.current = controller;
    return daemonApi
      .triggers(controller.signal)
      .then((items) => {
        if (!controller.signal.aborted) setTriggers(items);
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) setError(String(reason));
      });
  };

  useEffect(() => {
    mounted.current = true;
    void reload();
    const timer = window.setInterval(() => {
      void reload();
    }, 5000);
    return () => {
      mounted.current = false;
      triggerController.current?.abort();
      window.clearInterval(timer);
    };
  }, []);

  const requestFor = (trigger: AutomationTrigger): TriggerRequest => ({
    name: trigger.name,
    condition: trigger.condition,
    target: trigger.target,
    enabled: trigger.enabled,
  });

  const condition = (): TriggerCondition => {
    switch (kind) {
      case "manual":
        return { kind };
      case "schedule":
        return { kind, interval_seconds: interval };
      case "game_event":
        return {
          kind,
          event_name: eventName,
          device_code: deviceCode || null,
        };
      case "state_condition":
        return { kind, minimum_revision: minimumRevision };
      case "parent_workflow":
        return {
          kind,
          parent_kind: parentKind || null,
          status: parentStatus,
        };
    }
  };

  return (
    <div className="triggers-layout">
      <form
        className="workflow-form"
        onSubmit={(event) => {
          event.preventDefault();
          if (!selected) {
            setError("Select an action or workflow.");
            return;
          }
          const validation = validateParameters(selected, values);
          if (Object.keys(validation).length) {
            setError(Object.values(validation)[0] ?? "Invalid parameters.");
            return;
          }
          setError(null);
          void daemonApi
            .createTrigger({
              name,
              condition: condition(),
              target: {
                operation_class: selected.operation_class,
                kind: selected.kind,
                parameters: Object.fromEntries(
                  Object.entries(values).filter(([, value]) => value !== ""),
                ),
              },
              enabled,
            })
            .then((trigger) => {
              setTriggers((items) => [...items, trigger]);
              setName("");
              setEnabled(false);
            })
            .catch((reason: unknown) => {
              setError(String(reason));
            });
        }}
      >
        <header>
          <h2>New trigger</h2>
          <p>Automatic mutation stays off until you explicitly enable it.</p>
        </header>
        <div className="form-grid">
          <label>
            Name
            <input
              required
              maxLength={128}
              value={name}
              onChange={(event) => {
                setName(event.target.value);
              }}
            />
          </label>
          <label>
            Trigger
            <select
              value={kind}
              onChange={(event) => {
                setKind(event.target.value as TriggerCondition["kind"]);
              }}
            >
              <option value="manual">Manual</option>
              <option value="schedule">Schedule</option>
              <option value="game_event">Game event (SSE)</option>
              <option value="state_condition">State revision</option>
              <option value="parent_workflow">Parent workflow</option>
            </select>
          </label>
          <label>
            Target
            <select
              required
              value={targetKey}
              onChange={(event) => {
                const next = targets.find(
                  (descriptor) =>
                    `${descriptor.operation_class}:${descriptor.kind}` ===
                    event.target.value,
                );
                setTargetKey(event.target.value);
                setValues(
                  Object.fromEntries(
                    (next?.parameters ?? []).map((item) => [
                      item.name,
                      item.default ?? "",
                    ]),
                  ),
                );
              }}
            >
              <option value="">Select target</option>
              {targets
                .filter(
                  (descriptor) =>
                    descriptor.operation_class === "action" ||
                    descriptor.supported_triggers.includes(kind),
                )
                .map((descriptor) => (
                  <option
                    key={`${descriptor.operation_class}:${descriptor.kind}`}
                    value={`${descriptor.operation_class}:${descriptor.kind}`}
                  >
                    {descriptor.display_name}
                  </option>
                ))}
            </select>
          </label>
          {kind === "schedule" ? (
            <label>
              Interval seconds
              <input
                min={1}
                type="number"
                value={interval}
                onChange={(event) => {
                  setInterval(event.target.valueAsNumber);
                }}
              />
            </label>
          ) : null}
          {kind === "game_event" ? (
            <>
              <label>
                Event name
                <input
                  required
                  placeholder="mining.completed"
                  value={eventName}
                  onChange={(event) => {
                    setEventName(event.target.value);
                  }}
                />
              </label>
              <label>
                Device code (optional)
                <input
                  value={deviceCode}
                  onChange={(event) => {
                    setDeviceCode(event.target.value);
                  }}
                />
              </label>
            </>
          ) : null}
          {kind === "state_condition" ? (
            <label>
              Minimum managed revision
              <input
                min={0}
                type="number"
                value={minimumRevision}
                onChange={(event) => {
                  setMinimumRevision(event.target.valueAsNumber);
                }}
              />
            </label>
          ) : null}
          {kind === "parent_workflow" ? (
            <>
              <label>
                Parent kind (optional)
                <select
                  value={parentKind}
                  onChange={(event) => {
                    setParentKind(event.target.value);
                  }}
                >
                  <option value="">Any workflow</option>
                  {descriptors
                    .filter(
                      (descriptor) => descriptor.operation_class === "workflow",
                    )
                    .map((descriptor) => (
                      <option key={descriptor.kind} value={descriptor.kind}>
                        {descriptor.display_name}
                      </option>
                    ))}
                </select>
              </label>
              <label>
                Parent status
                <select
                  value={parentStatus}
                  onChange={(event) => {
                    setParentStatus(event.target.value as WorkflowStatus);
                  }}
                >
                  <option value="succeeded">Succeeded</option>
                  <option value="failed">Failed</option>
                  <option value="cancelled">Cancelled</option>
                </select>
              </label>
            </>
          ) : null}
          <label className="boolean-field">
            <input
              checked={enabled}
              type="checkbox"
              onChange={(event) => {
                setEnabled(event.target.checked);
              }}
            />
            <span>Enabled</span>
          </label>
        </div>
        {selected?.parameters.map((parameter) => (
          <ParameterField
            key={parameter.name}
            parameter={parameter}
            value={values[parameter.name]}
            entities={entities}
            onChange={(value) => {
              setValues((current) => ({
                ...current,
                [parameter.name]: value,
              }));
            }}
          />
        ))}
        {error ? <p className="form-error">{error}</p> : null}
        <button className="primary" type="submit">
          Save trigger
        </button>
      </form>
      <section className="trigger-list" aria-label="Persisted triggers">
        {triggers.length ? (
          triggers.map((trigger) => (
            <article key={trigger.id}>
              <header>
                <div>
                  <strong>{trigger.name}</strong>
                  <small>
                    {trigger.condition.kind.replaceAll("_", " ")} →{" "}
                    {trigger.target.kind}
                  </small>
                </div>
                <span
                  className={`status ${trigger.enabled ? "running" : "paused"}`}
                >
                  {trigger.enabled ? "enabled" : "disabled"}
                </span>
              </header>
              <small>
                Last fired:{" "}
                {trigger.last_fired_at_ms
                  ? new Date(trigger.last_fired_at_ms).toLocaleString()
                  : "never"}
                {trigger.next_run_at_ms
                  ? ` · Next: ${new Date(trigger.next_run_at_ms).toLocaleString()}`
                  : ""}
              </small>
              {trigger.last_error ? (
                <p className="form-error">{trigger.last_error}</p>
              ) : null}
              <div className="workflow-actions">
                <button
                  onClick={() => {
                    void daemonApi
                      .updateTrigger(trigger.id, trigger.revision, {
                        ...requestFor(trigger),
                        enabled: !trigger.enabled,
                      })
                      .then(reload);
                  }}
                >
                  {trigger.enabled ? "Disable" : "Enable"}
                </button>
                {trigger.condition.kind === "manual" ? (
                  <button
                    disabled={!trigger.enabled}
                    onClick={() =>
                      void daemonApi.fireTrigger(trigger.id).then(reload)
                    }
                  >
                    Run
                  </button>
                ) : null}
                <button
                  className="danger"
                  onClick={() =>
                    void daemonApi.deleteTrigger(trigger.id).then(reload)
                  }
                >
                  Delete
                </button>
              </div>
            </article>
          ))
        ) : (
          <p className="empty-state">No persisted triggers.</p>
        )}
      </section>
    </div>
  );
}

const workflowDisplayNames: Record<string, string> = {
  "belt.system": "Search System for Belts",
  "belt_search.campaign": "Search Region for Belts",
  "blueprint.acquire": "Acquire Blueprint",
  "event.campaign": "Complete Regional Events",
  "event.delivery": "Prepare Event",
  "event.fulfillment": "Fulfill Event",
  "event.stage": "Stage Event Requirements",
  "event.tour": "Fulfill Event",
  "exploration.frontier": "Expand FTL Network",
  "logistics.delivery": "Deliver Cargo or Devices",
  "logistics.manifest": "Deliver Manifest",
  "mining.campaign": "Expand Mining Operations",
  "mining.deploy": "Deploy Mining Operation",
  "mining.expansion": "Expand Mining Operations",
  "mining.route": "Deploy Mining Route",
  "mining.site": "Deploy Mining Site",
  "mining.stage": "Stage Mining Equipment",
  "observatory.search": "Search with Observatory",
  "region.establish": "Establish Region",
  "relay.expansion": "Expand Relay Network",
  "relay.stop": "Deploy Relay Stop",
  "replicant.provision": "Provision Regional Replicant",
  "requirement.action": "Fulfill Requirement Action",
  "requirement.fulfillment": "Fulfill Requirement",
  "salvage.recovery": "Recover Regional Salvage",
  "salvage.site": "Recover Salvage Site",
  "scan.belt": "Scan Asteroid Belt",
  "scan.system": "Scan System",
  "scan.tour": "Survey Region",
  "survey.route": "Survey Route",
  "survey.stop": "Survey System",
  "trade.fulfillment": "Execute Trade",
};

const workflowStepNames: Record<string, string> = {
  awaiting_available_resources: "Waiting for available resources",
  awaiting_blueprint_control_replicant:
    "Waiting for blueprint control Replicant",
  awaiting_delivery: "Waiting for delivery",
  awaiting_ftl_connectivity: "Waiting for FTL connectivity",
  awaiting_ftl_relay: "Waiting for an FTL relay",
  awaiting_purchase_evidence: "Confirming purchase",
  awaiting_relay_prerequisites: "Waiting for relay prerequisites",
  awaiting_trade_criteria: "Waiting for trade requirements",
  awaiting_trade_fulfillment: "Waiting for trade fulfillment",
  bootstrapping: "Bootstrapping region",
  configuring: "Configuring devices",
  decommissioning: "Decommissioning device",
  delivering: "Delivering assets",
  deploying: "Deploying infrastructure",
  executing: "Executing campaign",
  executing_trade: "Executing trade",
  exploring: "Extending FTL coverage",
  launching: "Launching devices",
  manufacturing: "Manufacturing equipment",
  manufacturing_survey_fleet: "Manufacturing survey fleet",
  planning: "Planning",
  printing_trade_criteria: "Printing trade requirements",
  printing_trade_payment: "Printing trade payment",
  prospecting: "Prospecting",
  purchasing: "Purchasing blueprint source",
  reconciling_purchase: "Confirming purchase",
  replanning_after_stale_asset: "Replanning after asset change",
  replanning_relay_coverage: "Replanning relay coverage",
  resolving: "Resolving event",
  running: "Running",
  searching_for_belts: "Searching for asteroid belts",
  staging: "Staging equipment",
  staging_survey_fleet: "Staging survey fleet",
  stowing_matrix: "Stowing replication matrix",
  travelling_to_shop: "Travelling to shop",
  waiting_for_criterion_blueprint: "Waiting for required blueprint",
  waiting_for_recovery_cleanup: "Waiting for recovery cleanup",
  waiting_for_resource_source: "Waiting for resource source",
  waiting_for_survey_fleet_claim: "Waiting for survey fleet",
  waiting_for_trade_payment: "Waiting for trade payment",
  waiting_for_trade_payment_blueprint: "Waiting for trade-payment blueprint",
  waiting_for_trade_payment_claim: "Waiting for trade-payment inventory",
  waiting_for_trade_payment_devices: "Waiting for trade-payment devices",
  waiting_for_trade_payment_resources: "Waiting for trade-payment resources",
  waiting_for_trade_return_transport: "Waiting for return transport",
  waiting_for_trade_reward_capacity: "Waiting for reward capacity",
  waiting_for_trade_reward_carrier: "Waiting for reward carrier",
  waiting_to_replan: "Waiting to replan",
};

function workflowDisplayName(
  workflow: WorkflowSummary,
  descriptor: WorkflowDescriptor | undefined,
) {
  return (
    descriptor?.display_name ??
    workflowDisplayNames[workflow.kind] ??
    workflow.kind
      .split(/[._-]+/)
      .filter(Boolean)
      .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
      .join(" ")
  );
}

function workflowStepName(step: string | null) {
  if (!step) return "Not started";
  const curated = workflowStepNames[step];
  if (curated) return curated;
  return step
    .split(/[_-]+/)
    .filter(Boolean)
    .map((word, index) => {
      const upper = word.toUpperCase();
      if (["FTL", "L4", "L5"].includes(upper)) return upper;
      return index === 0 ? word.charAt(0).toUpperCase() + word.slice(1) : word;
    })
    .join(" ");
}

type WorkflowScopeItem = {
  key: string;
  label: string;
  value: string;
};

const hiddenSummaryParameters = new Set([
  "max_concurrency",
  "max_hop_ly",
  "mission_file",
  "plan_file",
  "replace_plan",
  "return_transports",
  "travel_timeout_seconds",
  "survey_timeout_seconds",
  "wait_timeout_seconds",
  "maintenance_check_seconds",
  "maintenance_interval",
  "maintenance_resume_pct",
  "maintenance_threshold_pct",
]);

const scopePriority = [
  "region",
  "event",
  "target",
  "system",
  "systems",
  "systems_csv",
  "destination",
  "home",
  "hub",
  "origin",
  "replicant",
  "controller",
  "device_type",
];

function compactParameterValue(value: unknown) {
  if (typeof value === "string") return value.trim();
  if (typeof value === "number" || typeof value === "bigint")
    return String(value);
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (Array.isArray(value)) {
    const simple = value.filter(
      (item): item is string | number =>
        typeof item === "string" || typeof item === "number",
    );
    if (!simple.length) return null;
    const shown = simple.slice(0, 2).map(String);
    return simple.length > 2
      ? `${shown.join(", ")} +${String(simple.length - 2)}`
      : shown.join(", ");
  }
  return null;
}

function workflowScopeItems(
  detail: WorkflowDetail | undefined,
  descriptor: WorkflowDescriptor | undefined,
): WorkflowScopeItem[] {
  if (!detail) return [];
  const labels = Object.fromEntries(
    descriptor?.parameters.map((item) => [item.name, item.label]) ?? [],
  );
  const parameters = Object.entries(detail.parameters)
    .filter(([key]) => !hiddenSummaryParameters.has(key))
    .map(([key, value]) => ({
      key,
      label:
        labels[key] ??
        key
          .replace(/_csv$/, "")
          .split("_")
          .map((word) => word.charAt(0).toUpperCase() + word.slice(1))
          .join(" "),
      value: compactParameterValue(value),
    }))
    .filter(
      (item): item is WorkflowScopeItem =>
        item.value !== null && item.value.length > 0,
    )
    .sort((left, right) => {
      const leftPriority = scopePriority.indexOf(left.key);
      const rightPriority = scopePriority.indexOf(right.key);
      return (
        (leftPriority < 0 ? Number.MAX_SAFE_INTEGER : leftPriority) -
        (rightPriority < 0 ? Number.MAX_SAFE_INTEGER : rightPriority)
      );
    });

  const visible = parameters.slice(0, 4);
  if (visible.length < 4 && detail.claims.length) {
    const grouped = new Map<string, number>();
    for (const claim of detail.claims) {
      grouped.set(claim.kind, (grouped.get(claim.kind) ?? 0) + 1);
    }
    for (const [kind, count] of grouped) {
      if (visible.length >= 4) break;
      visible.push({
        key: `claim-${kind}`,
        label: "Claimed",
        value: `${String(count)} ${kind}${count === 1 ? "" : "s"}`,
      });
    }
  }
  return visible;
}

function workflowWaitReason(
  workflow: WorkflowSummary,
  detail: WorkflowDetail | undefined,
  detailError?: string,
) {
  if (detailError) return `Details unavailable: ${detailError}`;
  if (detail?.error) return detail.error;
  if (detail?.wait_reason) return detail.wait_reason;
  if (workflow.status === "waiting") {
    const step = workflow.current_step;
    if (step === "awaiting_ftl_connectivity")
      return "Waiting for FTL connectivity to the target system.";
    if (step === "awaiting_ftl_relay")
      return "Waiting for the required FTL relay to become available.";
    if (step === "awaiting_relay_prerequisites")
      return "Waiting for relay prerequisites such as inventory, a blueprint, or an available worker.";
    if (step === "awaiting_available_resources")
      return "Waiting for required resources or claimed devices to become available.";
    if (step?.startsWith("printing_"))
      return "Waiting for the current print job to finish.";
    return "Waiting for a workflow dependency or resource to become ready.";
  }
  if (workflow.status === "paused") return "Paused by operator.";
  return "No blocker — work is progressing.";
}

function WorkflowRow({
  workflow,
  descriptor,
  detail,
  detailError,
  selected,
  onSelect,
  onControl,
}: {
  workflow: WorkflowSummary;
  descriptor?: WorkflowDescriptor;
  detail?: WorkflowDetail;
  detailError?: string;
  selected: boolean;
  onSelect: () => void;
  onControl: (action: "pause" | "resume" | "cancel") => void;
}) {
  const active = activeStatuses.includes(workflow.status);
  const scopeItems = workflowScopeItems(detail, descriptor);
  const waitReason = workflowWaitReason(workflow, detail, detailError);
  return (
    <article
      className={`workflow-row ${selected ? "selected" : ""} ${detail?.parent_id ? "child" : ""}`.trim()}
    >
      <button className="workflow-row-main" onClick={onSelect}>
        <span className="workflow-identity">
          <strong>{workflowDisplayName(workflow, descriptor)}</strong>
          <small>
            {workflow.kind}
            {detail?.parent_id ? " · child workflow" : ""}
          </small>
        </span>
        <span className={`workflow-status ${workflow.status}`}>
          {workflow.status}
        </span>
        <span className="workflow-step">
          <small>Current step</small>
          <span>{workflowStepName(workflow.current_step)}</span>
        </span>
        <span className="workflow-scope">
          <small>Scope</small>
          {detail ? (
            scopeItems.length ? (
              <span className="workflow-scope-items">
                {scopeItems.map((item) => (
                  <span className="workflow-scope-item" key={item.key}>
                    <span>{item.label}</span>
                    <strong>{item.value}</strong>
                  </span>
                ))}
              </span>
            ) : (
              <span className="workflow-muted">No specific target</span>
            )
          ) : (
            <span className="workflow-muted">
              {detailError ? "Details unavailable" : "Loading…"}
            </span>
          )}
        </span>
        <span className={`workflow-wait ${detail?.error ? "error" : ""}`}>
          <small>{detail?.error ? "Error" : "Status detail"}</small>
          <span>{waitReason}</span>
        </span>
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

function workflowParameterValue(value: unknown) {
  if (value === null || value === undefined) return "—";
  if (typeof value === "string") return value;
  if (
    typeof value === "number" ||
    typeof value === "bigint" ||
    typeof value === "boolean" ||
    typeof value === "symbol"
  ) {
    return value.toString();
  }
  if (typeof value === "function") return value.name || "function";
  try {
    return JSON.stringify(value);
  } catch {
    return "Unserializable value";
  }
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
  const orderedActivity = [...activity].sort(
    (left, right) => right.occurred_at_ms - left.occurred_at_ms,
  );
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
      <section className="workflow-inspector-summary">
        <h3>Run status</h3>
        <dl>
          <dt>Status</dt>
          <dd>
            <span className={`workflow-status ${detail.summary.status}`}>
              {detail.summary.status}
            </span>
          </dd>
          <dt>Current step</dt>
          <dd>{detail.summary.current_step ?? "Not started"}</dd>
          <dt>Created</dt>
          <dd>{new Date(detail.created_at_ms).toLocaleString()}</dd>
          {detail.finished_at_ms !== null ? (
            <>
              <dt>Finished</dt>
              <dd>{new Date(detail.finished_at_ms).toLocaleString()}</dd>
            </>
          ) : null}
        </dl>
        {detail.error ? (
          <div className="workflow-failure" role="alert">
            <strong>Failure reason</strong>
            <p>{detail.error}</p>
          </div>
        ) : detail.wait_reason ? (
          <div className="workflow-wait-reason">
            <strong>Waiting on</strong>
            <p>{detail.wait_reason}</p>
          </div>
        ) : null}
      </section>
      {detail.parent_id ? (
        <section>
          <h3>Orchestration</h3>
          <p>
            <small>Parent workflow</small> {detail.parent_id}
          </p>
        </section>
      ) : null}
      <section>
        <h3>Parameters / targets</h3>
        {Object.keys(detail.parameters).length ? (
          <dl className="workflow-parameters">
            {Object.entries(detail.parameters).map(([key, value]) => (
              <div key={key}>
                <dt>{key.replaceAll("_", " ")}</dt>
                <dd>{workflowParameterValue(value)}</dd>
              </div>
            ))}
          </dl>
        ) : (
          <p className="empty-state">No configured parameters.</p>
        )}
      </section>
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
        <h3>Recent activity</h3>
        {orderedActivity.length ? (
          <ol className="timeline">
            {orderedActivity.map((item) => (
              <li className={item.level} key={item.id}>
                <time dateTime={new Date(item.occurred_at_ms).toISOString()}>
                  {new Date(item.occurred_at_ms).toLocaleString()}
                </time>
                <strong>{item.step ?? item.level}</strong>
                <p>{item.message}</p>
              </li>
            ))}
          </ol>
        ) : (
          <p className="empty-state">No workflow activity recorded.</p>
        )}
      </section>
    </aside>
  );
}

const goalLabels: Record<DirectorGoalKind, string> = {
  establish_regions: "Establish Regions",
  expand_star_catalogue: "Expand Star Catalogue",
  enhance_star_catalogue: "Enhance Star Catalogue",
  discover_belts: "Discover Belts",
  expand_mining_ops: "Expand Mining Ops",
  salvage_recovery: "Salvage Recovery",
  event_completion: "Event Completion",
  asteroid_diversion: "Asteroid Diversion",
  blueprint_acquisition: "Blueprint Acquisition",
  maintain_system_hubs: "Maintain System Hubs",
  expand_ftl_network: "Expand FTL Network",
  stranded_device_recovery: "Stranded Device Recovery",
  unserviced_resources: "Unserviced Resources",
  establish_beacons: "Establish Beacons",
};

const requirementLabels: Record<DirectorRequirementKind, string> = {
  blueprint: "Blueprint",
  logistics: "Logistics",
  worker_capacity: "Worker Capacity",
  connectivity: "Connectivity",
};

function goalProgress(goal: DirectorGoalSummary) {
  if (goal.progress_total === 0) return null;
  return `${String(goal.progress_current)} / ${String(goal.progress_total)}`;
}

function DirectorView({
  onOpenWorkflow,
}: {
  onOpenWorkflow: (workflowId: string) => void;
}) {
  const query = useDomainQuery({
    slice: "director",
    fetcher: (signal) => daemonApi.director(signal),
    isEmpty: () => false,
  });
  const [mutating, setMutating] = useState(false);
  const [mutationError, setMutationError] = useState<string | null>(null);
  const data = query.data;

  const mutate = async (operation: () => Promise<DirectorSnapshot>) => {
    setMutating(true);
    setMutationError(null);
    try {
      await operation();
      await query.refresh();
    } catch (reason) {
      setMutationError(String(reason));
    } finally {
      setMutating(false);
    }
  };

  if (!data) {
    return (
      <p className="empty-state">
        {query.error ?? "Loading Automation Director…"}
      </p>
    );
  }

  const globalGoals = data.goals.filter((goal) => goal.region === null);
  const regionalGoals = data.goals.filter((goal) => goal.region !== null);
  const grouped = regionalGoals.reduce<Record<string, DirectorGoalSummary[]>>(
    (items, goal) => {
      const region = goal.region ?? "unknown";
      (items[region] ??= []).push(goal);
      return items;
    },
    {},
  );
  const miningPolicies = new Map(
    data.mining_policies.map((policy) => [policy.region, policy]),
  );
  const replicantLabels = new Map(
    data.replicants.map((replicant) => [
      replicant.code,
      replicant.name ? `${replicant.name} (${replicant.code})` : replicant.code,
    ]),
  );
  const activeRequirements = data.requirements.filter(
    (requirement) => requirement.status !== "satisfied",
  );

  return (
    <section className="director-view">
      <div className="director-toolbar">
        <div>
          <span className="eyebrow">Empire control plane</span>
          <h2>Automation Director</h2>
          <p>
            Standing goals create regional batch campaigns. Replicants keep
            their regional assignment, and workforce scaling is grow-only: the
            Director can add workers but never deletes or retires them.
          </p>
        </div>
        <div className="director-mode" aria-label="Director mode">
          {(["off", "advisory", "automatic"] as const).map((mode) => (
            <button
              key={mode}
              className={data.mode === mode ? "active" : ""}
              disabled={mutating}
              onClick={() => void mutate(() => daemonApi.setDirectorMode(mode))}
            >
              {mode}
            </button>
          ))}
          <button
            disabled={mutating}
            onClick={() => void mutate(() => daemonApi.reconcileDirector())}
          >
            Reconcile now
          </button>
        </div>
      </div>

      {mutationError || query.error ? (
        <p className="form-error">{mutationError ?? query.error}</p>
      ) : null}

      <div className="director-metrics">
        <div>
          <span>Replicants</span>
          <strong>{data.workforce.total}</strong>
        </div>
        <div>
          <span>Busy</span>
          <strong>{data.workforce.busy}</strong>
        </div>
        <div>
          <span>Idle reserve</span>
          <strong>{Math.round(data.workforce.idle_ratio * 100)}%</strong>
        </div>
        <div>
          <span>Worker-blocked</span>
          <strong>{data.workforce.pending_worker_demand}</strong>
        </div>
      </div>
      {data.workforce.scale_reason ? (
        <p
          className={
            data.workforce.scale_up_recommended
              ? "director-callout warning"
              : "director-callout"
          }
        >
          {data.workforce.scale_reason}
        </p>
      ) : null}

      {activeRequirements.length ? (
        <section className="director-policy">
          <h3>Shared requirements</h3>
          <div className="director-goal-grid">
            {activeRequirements.map((requirement) => (
              <article
                className={`director-goal ${requirement.status}`}
                key={requirement.id}
              >
                <header>
                  <div>
                    <span>{requirement.region ?? "Global"}</span>
                    <h3>{requirementLabels[requirement.kind]}</h3>
                  </div>
                  <span>priority {requirement.priority}</span>
                </header>
                <p>{requirement.target}</p>
                {requirement.requesters.map((requester) => (
                  <p key={requester.goal_id}>
                    <b>{requester.goal_id}:</b> {requester.reason}
                  </p>
                ))}
              </article>
            ))}
          </div>
        </section>
      ) : null}

      <div className="director-goal-grid">
        {globalGoals.map((goal) => (
          <article className={`director-goal ${goal.status}`} key={goal.id}>
            <header>
              <div>
                <span>Global goal</span>
                <h3>{goalLabels[goal.kind]}</h3>
              </div>
              <label className="director-toggle">
                <input
                  type="checkbox"
                  checked={goal.enabled}
                  disabled={mutating}
                  aria-label={`${goalLabels[goal.kind]} global goal`}
                  onChange={(event) =>
                    void mutate(() =>
                      daemonApi.setDirectorGoal(
                        goal.kind,
                        null,
                        event.target.checked,
                      ),
                    )
                  }
                />
                {goal.enabled ? "enabled" : "disabled"}
              </label>
            </header>
            <p>{goal.objective}</p>
            {goalProgress(goal) ? <strong>{goalProgress(goal)}</strong> : null}
            {goal.blocker ? (
              <p>
                <b>Blocked:</b> {goal.blocker}
              </p>
            ) : null}
            {goal.next_action ? (
              <p>
                <b>Next:</b> {goal.next_action}
              </p>
            ) : null}
            {goal.active_workflows.length ? (
              <p>
                <b>Active:</b>{" "}
                {goal.active_workflows.map((workflowId) => (
                  <button
                    className="text-button"
                    key={workflowId}
                    onClick={() => {
                      onOpenWorkflow(workflowId);
                    }}
                  >
                    {workflowId}
                  </button>
                ))}
              </p>
            ) : null}
          </article>
        ))}
      </div>

      <section className="director-regions">
        <h3>Regional campaigns</h3>
        {data.regions.map((region) => (
          <article className="director-region" key={region.region}>
            <header>
              <div>
                <span>{region.status}</span>
                <h4>{region.region}</h4>
                <small>
                  {region.hub_system ?? "No foothold"} · {region.known_systems}{" "}
                  known systems
                </small>
              </div>
              <div className="director-region-workers">
                {region.replicants.length
                  ? region.replicants
                      .map((code) => replicantLabels.get(code) ?? code)
                      .join(", ")
                  : "No assigned workers"}
              </div>
            </header>
            <div className="director-regional-goals">
              {(grouped[region.region] ?? []).map((goal) => {
                const progress = goalProgress(goal);
                const miningPolicy = miningPolicies.get(region.region) ?? {
                  region: region.region,
                  expand_moderate: true,
                  expand_sparse: true,
                };
                return (
                  <div
                    className={`director-regional-goal ${goal.status}`}
                    key={goal.id}
                  >
                    <div>
                      <strong>{goalLabels[goal.kind]}</strong>
                      <label className="director-toggle">
                        <input
                          type="checkbox"
                          checked={goal.enabled}
                          disabled={mutating}
                          aria-label={`${goalLabels[goal.kind]} in ${goal.region ?? region.region}`}
                          onChange={(event) =>
                            void mutate(() =>
                              daemonApi.setDirectorGoal(
                                goal.kind,
                                goal.region,
                                event.target.checked,
                              ),
                            )
                          }
                        />
                        {goal.status}
                        {progress ? ` · ${progress}` : ""}
                      </label>
                    </div>
                    {goal.kind === "expand_mining_ops" ? (
                      <div
                        className="director-mining-policy"
                        aria-label={`Mining expansion density in ${region.region}`}
                      >
                        <span>New expansion · 4 ward-backed belts</span>
                        <strong>Dense</strong>
                        <label>
                          <input
                            type="checkbox"
                            checked={miningPolicy.expand_moderate}
                            disabled={mutating}
                            aria-label={`Expand mining to moderate belts in ${region.region}`}
                            onChange={(event) =>
                              void mutate(() =>
                                daemonApi.setDirectorMiningPolicy(
                                  region.region,
                                  event.target.checked,
                                  miningPolicy.expand_sparse,
                                ),
                              )
                            }
                          />
                          Moderate
                        </label>
                        <label>
                          <input
                            type="checkbox"
                            checked={miningPolicy.expand_sparse}
                            disabled={mutating}
                            aria-label={`Expand mining to sparse belts in ${region.region}`}
                            onChange={(event) =>
                              void mutate(() =>
                                daemonApi.setDirectorMiningPolicy(
                                  region.region,
                                  miningPolicy.expand_moderate,
                                  event.target.checked,
                                ),
                              )
                            }
                          />
                          Sparse
                        </label>
                      </div>
                    ) : null}
                    <p>{goal.objective}</p>
                    {goal.blocker ? (
                      <p>
                        <b>Blocked:</b> {goal.blocker}
                      </p>
                    ) : null}
                    {goal.next_action ? (
                      <p>
                        <b>Next:</b> {goal.next_action}
                      </p>
                    ) : null}
                    {goal.active_workflows.length ? (
                      <p>
                        <b>Active:</b>{" "}
                        {goal.active_workflows.map((workflowId) => (
                          <button
                            className="text-button"
                            key={workflowId}
                            onClick={() => {
                              onOpenWorkflow(workflowId);
                            }}
                          >
                            {workflowId}
                          </button>
                        ))}
                      </p>
                    ) : null}
                  </div>
                );
              })}
            </div>
          </article>
        ))}
      </section>

      <section className="director-roster">
        <h3>Regional Replicant assignments</h3>
        <div className="director-roster-grid">
          {data.replicants.map((replicant) => (
            <label key={replicant.code}>
              <span>
                <strong>{replicant.name ?? replicant.code}</strong>
                <small>{replicant.busy ? "busy" : "idle"}</small>
              </span>
              <select
                value={replicant.region ?? ""}
                disabled={mutating}
                onChange={(event) =>
                  void mutate(() =>
                    daemonApi.assignDirectorReplicant(
                      replicant.code,
                      event.target.value || null,
                      replicant.role_affinity,
                    ),
                  )
                }
              >
                <option value="">Unassigned</option>
                {data.regions.map((region) => (
                  <option key={region.region} value={region.region}>
                    {region.region}
                  </option>
                ))}
              </select>
            </label>
          ))}
        </div>
      </section>
    </section>
  );
}

export function AutomationsPage({
  workflows,
  entities,
  selectedWorkflowId,
  onSelectedWorkflowConsumed,
}: {
  workflows: WorkflowSummary[];
  entities: Record<string, unknown>;
  selectedWorkflowId?: string;
  onSelectedWorkflowConsumed?: () => void;
}) {
  const [tab, setTab] = useState<Tab>("Director");
  const [descriptors, setDescriptors] = useState<WorkflowDescriptor[]>([]);
  const [triggerDescriptors, setTriggerDescriptors] = useState<
    OperationDescriptor[]
  >([]);
  const [selectedTemplate, setSelectedTemplate] = useState<string | null>(null);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [created, setCreated] = useState<WorkflowSummary[]>([]);
  const [details, setDetails] = useState<Record<string, WorkflowDetail>>({});
  const [detailErrors, setDetailErrors] = useState<Record<string, string>>({});
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
  const activeWorkflowRevisions = useMemo(
    () =>
      tab === "Active"
        ? currentWorkflows
            .filter((workflow) => activeStatuses.includes(workflow.status))
            .map((workflow) => `${workflow.id}:${String(workflow.revision)}`)
            .sort((left, right) => left.localeCompare(right))
            .join("|")
        : "",
    [currentWorkflows, tab],
  );
  const selectedWorkflowRevision = currentWorkflows.find(
    (workflow) => workflow.id === selectedId,
  )?.revision;

  useEffect(() => {
    if (!selectedWorkflowId) return;
    const selectedWorkflow = currentWorkflows.find(
      (workflow) => workflow.id === selectedWorkflowId,
    );
    if (!selectedWorkflow) return;
    setTab(
      activeStatuses.includes(selectedWorkflow.status) ? "Active" : "History",
    );
    setSelectedId(selectedWorkflowId);
    onSelectedWorkflowConsumed?.();
  }, [currentWorkflows, onSelectedWorkflowConsumed, selectedWorkflowId]);

  useEffect(() => {
    const controller = new AbortController();
    void daemonApi
      .descriptors(controller.signal)
      .then((catalog) => {
        if (controller.signal.aborted) return;
        const visibleWorkflows = catalog.workflows.filter(
          (descriptor) => descriptor.category !== "compatibility",
        );
        setDescriptors(visibleWorkflows);
        setTriggerDescriptors([...catalog.actions, ...visibleWorkflows]);
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) setError(String(reason));
      });
    return () => {
      controller.abort();
    };
  }, []);

  useEffect(() => {
    if (!activeWorkflowRevisions) return;
    const controller = new AbortController();
    const pending = activeWorkflowRevisions
      .split("|")
      .map((entry) => {
        const separator = entry.lastIndexOf(":");
        return {
          id: entry.slice(0, separator),
          revision: Number(entry.slice(separator + 1)),
        };
      })
      .filter(({ id, revision }) => details[id]?.summary.revision !== revision);
    let cursor = 0;

    const loadNext = async () => {
      while (cursor < pending.length) {
        const target = pending[cursor];
        cursor += 1;
        if (!target) return;
        try {
          const detail = await daemonApi.workflow(target.id, controller.signal);
          if (controller.signal.aborted) return;
          setDetails((current) => ({ ...current, [target.id]: detail }));
          setDetailErrors((current) => {
            if (!(target.id in current)) return current;
            return Object.fromEntries(
              Object.entries(current).filter(([key]) => key !== target.id),
            );
          });
        } catch (reason: unknown) {
          if (controller.signal.aborted) return;
          setDetailErrors((current) => ({
            ...current,
            [target.id]: String(reason),
          }));
        }
      }
    };

    // Workflow detail is a local SQLite read. Keep a small concurrency ceiling
    // so opening Automations cannot flood the daemon when thousands of
    // historical workflows are retained.
    const workers = Array.from({ length: Math.min(4, pending.length) }, () =>
      loadNext(),
    );
    void Promise.all(workers);
    return () => {
      controller.abort();
    };
    // `details` is deliberately read as the cache at effect start. Re-running
    // for every completed request would cancel the remaining bounded queue.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeWorkflowRevisions]);

  useEffect(() => {
    if (!selectedId) {
      setActivity([]);
      return;
    }
    const controller = new AbortController();
    void daemonApi
      .workflow(selectedId, controller.signal)
      .then((detail) => {
        if (controller.signal.aborted) return;
        setDetails((current) => ({ ...current, [selectedId]: detail }));
        setDetailErrors((current) => {
          if (!(selectedId in current)) return current;
          return Object.fromEntries(
            Object.entries(current).filter(([key]) => key !== selectedId),
          );
        });
      })
      .catch((reason: unknown) => {
        if (controller.signal.aborted) return;
        setDetailErrors((current) => ({
          ...current,
          [selectedId]: String(reason),
        }));
      });
    void daemonApi
      .workflowActivity(selectedId, controller.signal)
      .then(setActivity)
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) setError(String(reason));
      });
    return () => {
      controller.abort();
    };
  }, [selectedId, selectedWorkflowRevision]);

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
    if (
      action === "cancel" &&
      !window.confirm(`Cancel ${workflow.kind}? This cannot be undone.`)
    )
      return;
    setError(null);
    const request =
      action === "cancel"
        ? daemonApi.controlAutomation("cancel", [workflow.id], true)
        : daemonApi.controlWorkflow(workflow.id, action);
    void request.catch((reason: unknown) => {
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
              if (item !== tab) {
                setSelectedId(null);
                setActivity([]);
              }
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

      {tab === "Director" ? (
        <DirectorView
          onOpenWorkflow={(workflowId) => {
            setTab("Active");
            setSelectedId(workflowId);
          }}
        />
      ) : tab === "Templates" ? (
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
            selectedDescriptor.kind === "logistics.delivery" ? (
              <LogisticsWorkflowForm
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
            )
          ) : (
            <p className="empty-state">
              Select a workflow template to configure it.
            </p>
          )}
        </div>
      ) : tab === "Schedules" ? (
        <TriggersView descriptors={triggerDescriptors} entities={entities} />
      ) : (
        <section className="workflow-list" aria-label={`${tab} workflows`}>
          {visible.length ? (
            visible.map((workflow) => (
              <WorkflowRow
                key={workflow.id}
                workflow={workflow}
                descriptor={descriptorsByKind[workflow.kind]}
                detail={details[workflow.id]}
                detailError={detailErrors[workflow.id]}
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
