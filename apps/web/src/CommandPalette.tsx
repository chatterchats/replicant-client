/* eslint-disable react-refresh/only-export-components */
import { useMemo, useState } from "react";

import { ParameterField, validateParameters } from "./AutomationsPage";
import { daemonApi } from "./api";
import type {
  DescriptorCatalog,
  FiniteExecution,
  OperationDescriptor,
  WorkflowSummary,
} from "./protocol";

type OperationClass = "report" | "action" | "workflow";
type ContextKind = "system" | "location" | "device" | "replicant";

export type CommandContext = Partial<Record<ContextKind, string>>;

export interface DescriptorCommand {
  descriptor: OperationDescriptor;
  operationClass: OperationClass;
}

export function descriptorCommands(
  catalog: DescriptorCatalog,
): DescriptorCommand[] {
  return [...catalog.reports, ...catalog.actions, ...catalog.workflows].map(
    (descriptor) => ({
      descriptor,
      operationClass: descriptor.operation_class,
    }),
  );
}

export function applicableDescriptorCommands(
  catalog: DescriptorCatalog,
  entityKind: ContextKind,
): DescriptorCommand[] {
  return descriptorCommands(catalog).filter(({ descriptor }) =>
    descriptor.applicable_to.includes(entityKind),
  );
}

export function searchDescriptors(
  catalog: DescriptorCatalog,
  query: string,
): DescriptorCommand[] {
  const terms = query.trim().toLowerCase().split(/\s+/).filter(Boolean);
  return descriptorCommands(catalog).filter(
    ({ descriptor, operationClass }) => {
      const entityTypes = descriptor.parameters.flatMap((parameter) =>
        parameter.kind.type === "entity"
          ? [parameter.kind.entity_kind]
          : [parameter.kind.type],
      );
      const haystack = [
        descriptor.display_name,
        descriptor.kind,
        ...descriptor.aliases,
        descriptor.category,
        operationClass,
        ...entityTypes,
      ]
        .join(" ")
        .toLowerCase();
      return terms.every((term) => haystack.includes(term));
    },
  );
}

export function resolveContextDefaults(
  descriptor: OperationDescriptor,
  context: CommandContext,
): Record<string, unknown> {
  return Object.fromEntries(
    descriptor.parameters.map((parameter) => {
      const contextKind =
        parameter.kind.type === "entity"
          ? parameter.kind.entity_kind
          : parameter.kind.type;
      const contextual =
        contextKind === "system" ||
        contextKind === "location" ||
        contextKind === "device" ||
        contextKind === "replicant"
          ? context[contextKind]
          : undefined;
      return [parameter.name, contextual ?? parameter.default ?? ""];
    }),
  );
}

export function CommandPalette({
  catalog,
  context,
  entities,
  navigation,
  onClose,
  onNavigate,
  onWorkflowStarted,
  onOperationFinished,
  initialCommand,
}: {
  catalog: DescriptorCatalog;
  context: CommandContext;
  entities: Record<string, unknown>;
  navigation: readonly string[];
  onClose: () => void;
  onNavigate: (page: string) => void;
  onWorkflowStarted: (workflow: WorkflowSummary) => void;
  onOperationFinished: (execution: FiniteExecution) => void;
  initialCommand?: DescriptorCommand;
}) {
  const [query, setQuery] = useState("");
  const [selected, setSelected] = useState<DescriptorCommand | null>(
    initialCommand ?? null,
  );
  const [values, setValues] = useState<Record<string, unknown>>(() =>
    initialCommand
      ? resolveContextDefaults(initialCommand.descriptor, context)
      : {},
  );
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [confirming, setConfirming] = useState(false);
  const [confirmation, setConfirmation] = useState("");
  const [serverError, setServerError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [active, setActive] = useState(0);
  const descriptorMatches = useMemo(
    () => searchDescriptors(catalog, query),
    [catalog, query],
  );
  const pageMatches = navigation.filter((page) =>
    page.toLowerCase().includes(query.trim().toLowerCase()),
  );
  const matchCount = pageMatches.length + descriptorMatches.length;

  const choose = (command: DescriptorCommand) => {
    setSelected(command);
    setValues(resolveContextDefaults(command.descriptor, context));
  };

  const run = async () => {
    if (!selected) return;
    setSubmitting(true);
    setServerError(null);
    const parameters = Object.fromEntries(
      Object.entries(values).filter(
        ([, value]) => value !== "" && value !== null && value !== undefined,
      ),
    );
    try {
      if (selected.operationClass === "workflow") {
        onWorkflowStarted(
          await daemonApi.startWorkflow(selected.descriptor.kind, parameters),
        );
      } else {
        onOperationFinished(
          await daemonApi.runOperation(
            selected.operationClass,
            selected.descriptor.kind,
            parameters,
          ),
        );
      }
    } catch (error) {
      setServerError(String(error));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="palette-backdrop" role="presentation" onMouseDown={onClose}>
      <section
        className="palette"
        role="dialog"
        aria-modal="true"
        aria-label="Command palette"
        onMouseDown={(event) => {
          event.stopPropagation();
        }}
      >
        {confirming && selected ? (
          <div className="palette-confirmation">
            <span className={`risk ${selected.descriptor.risk}`}>
              {selected.descriptor.risk} risk
            </span>
            <h2>Confirm {selected.descriptor.display_name}</h2>
            <p>This {selected.operationClass} can change game state.</p>
            {selected.descriptor.risk === "elevated" ? (
              <label>
                Type <strong>{selected.descriptor.display_name}</strong> to
                continue
                <input
                  autoFocus
                  value={confirmation}
                  onChange={(event) => {
                    setConfirmation(event.target.value);
                  }}
                />
              </label>
            ) : null}
            {serverError ? <p className="form-error">{serverError}</p> : null}
            <div className="palette-actions">
              <button
                onClick={() => {
                  setConfirming(false);
                }}
              >
                Back
              </button>
              <button
                className="primary"
                disabled={
                  submitting ||
                  (selected.descriptor.risk === "elevated" &&
                    confirmation !== selected.descriptor.display_name)
                }
                onClick={() => {
                  void run();
                }}
              >
                {submitting ? "Running…" : "Confirm and run"}
              </button>
            </div>
          </div>
        ) : selected ? (
          <form
            className="workflow-form palette-form"
            onSubmit={(event) => {
              event.preventDefault();
              const nextErrors = validateParameters(
                selected.descriptor,
                values,
              );
              setErrors(nextErrors);
              if (Object.keys(nextErrors).length) return;
              if (
                selected.operationClass === "action" ||
                selected.descriptor.risk === "elevated"
              ) {
                setConfirming(true);
              } else {
                void run();
              }
            }}
          >
            <header>
              <button
                type="button"
                onClick={() => {
                  setSelected(null);
                }}
              >
                ← Commands
              </button>
              <span className={`risk ${selected.descriptor.risk}`}>
                {selected.operationClass} · {selected.descriptor.risk} risk
              </span>
              <h2>{selected.descriptor.display_name}</h2>
              <p>{selected.descriptor.description}</p>
            </header>
            <div className="form-grid">
              {selected.descriptor.parameters.map((parameter) => (
                <ParameterField
                  key={parameter.name}
                  parameter={parameter}
                  value={values[parameter.name]}
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
            {serverError ? <p className="form-error">{serverError}</p> : null}
            <button className="primary" disabled={submitting} type="submit">
              Continue
            </button>
          </form>
        ) : (
          <>
            <input
              autoFocus
              aria-label="Search commands"
              placeholder="Search commands, aliases, categories, or entity types…"
              value={query}
              onChange={(event) => {
                setQuery(event.target.value);
                setActive(0);
              }}
              onKeyDown={(event) => {
                if (event.key === "ArrowDown" || event.key === "ArrowUp") {
                  event.preventDefault();
                  const direction = event.key === "ArrowDown" ? 1 : -1;
                  setActive((current) =>
                    matchCount
                      ? (current + direction + matchCount) % matchCount
                      : 0,
                  );
                } else if (event.key === "Enter") {
                  event.preventDefault();
                  const page = pageMatches[active];
                  if (page) onNavigate(page);
                  else {
                    const command =
                      descriptorMatches[active - pageMatches.length];
                    if (command) choose(command);
                  }
                }
              }}
            />
            <div className="palette-results">
              {pageMatches.map((page, index) => (
                <button
                  className={active === index ? "active" : ""}
                  key={page}
                  onClick={() => {
                    onNavigate(page);
                  }}
                >
                  <span>{page}</span>
                  <small>page</small>
                </button>
              ))}
              {descriptorMatches.map((command, index) => (
                <button
                  className={
                    active === pageMatches.length + index ? "active" : ""
                  }
                  key={`${command.operationClass}:${command.descriptor.kind}`}
                  onClick={() => {
                    choose(command);
                  }}
                >
                  <span>{command.descriptor.display_name}</span>
                  <small>
                    {command.operationClass} · {command.descriptor.category}
                  </small>
                </button>
              ))}
              {matchCount === 0 ? <p>No commands found.</p> : null}
            </div>
          </>
        )}
      </section>
    </div>
  );
}
