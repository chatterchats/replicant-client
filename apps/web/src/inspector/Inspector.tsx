import { useCallback, useEffect, useState, type ReactNode } from "react";

import { daemonApi } from "../api";
import type { DescriptorCommand } from "../CommandPalette";
import { DeviceLogButton } from "../DeviceLogPanel";
import { useDaemonState } from "../daemon";
import { useDomainQuery } from "../domainQuery";
import type {
  DescriptorCatalog,
  EntityInspectorSnapshot,
  EntitySummary,
  FiniteExecution,
  WorkflowActivity,
  WorkflowDetail,
} from "../protocol";
import type { SelectedEntity } from "../shellState";
import { DeviceInspector } from "./DeviceInspector";
import { EventInspector } from "./EventInspector";
import { GenericInspector } from "./GenericInspector";
import {
  InspectorShell,
  type InspectorNavigationControls,
} from "./InspectorShell";
import {
  associatedWorkflowIds,
  InspectorIntelligence,
} from "./InspectorIntelligence";
import { InspectorActivityTimeline } from "./InspectorActivityTimeline";
import { LocationInspector } from "./LocationInspector";
import {
  DeviceInspectorVitals,
  LocationInspectorVitals,
  ReplicantInspectorVitals,
  SystemInspectorVitals,
} from "./InspectorVitals";
import { ReplicantInspector } from "./ReplicantInspector";
import { ResourceInspector } from "./ResourceInspector";
import { SystemInspector } from "./SystemInspector";
import { WorkflowInspector } from "./WorkflowInspector";
import {
  fallbackSummary,
  isDeviceSummary,
  isEntitySummary,
  isEventSummary,
  isWorkflowSummary,
} from "./inspectorModel";

interface InspectorSlots {
  vitals?: ReactNode;
  body?: ReactNode;
  relations?: ReactNode;
  contents?: ReactNode;
  intelligence?: ReactNode;
  activity?: ReactNode;
}

export interface InspectorProps {
  entity: SelectedEntity;
  value: unknown;
  descriptors: DescriptorCatalog;
  entities: Record<string, EntitySummary>;
  activity: WorkflowActivity[];
  revision?: number | null;
  onClose: () => void;
  onClear: () => void;
  onOpenGalaxy: (system: string) => void;
  onOpenSystem: (system: string) => void;
  onOpenWorkflow: (workflowId: string) => void;
  onSelectEntity: (entity: SelectedEntity) => void;
  onRunCommand: (command: DescriptorCommand) => void;
  onOperationFinished: (execution: FiniteExecution) => void;
  navigation?: InspectorNavigationControls;
}

function managedKind(
  kind: string,
): kind is "device" | "replicant" | "system" | "location" {
  return (
    kind === "device" ||
    kind === "replicant" ||
    kind === "system" ||
    kind === "location"
  );
}

function detailSlices(kind: "device" | "replicant" | "system" | "location") {
  if (kind === "device") return ["devices", "cargo"] as const;
  if (kind === "replicant") return ["entities", "devices"] as const;
  if (kind === "system") return ["universe"] as const;
  return ["universe", "devices", "entities"] as const;
}

function useInspectorWorkflowIntelligence() {
  return useDomainQuery({
    slice: "workflows",
    queryKey: "inspector:workflow-intelligence",
    fetcher: (signal) => daemonApi.workflowIntelligence(signal),
    isEmpty: () => false,
  }).data;
}

export function InspectorView({
  props,
  snapshot,
  warning,
  intelligence,
  associatedActivity,
}: {
  props: InspectorProps;
  snapshot?: EntityInspectorSnapshot;
  warning?: ReactNode;
  intelligence?: ReactNode;
  associatedActivity?: WorkflowActivity[];
}) {
  const { entity, value, entities, activity } = props;
  const summary = snapshot?.summary ?? fallbackSummary(entity, value);
  const navigate = (kind: string, id: string) => {
    props.onSelectEntity({ kind: kind as SelectedEntity["kind"], id });
  };
  let slots: InspectorSlots;
  let deviceLogDevice: string | null = null;
  if (snapshot?.detail.kind === "device") {
    const detail = snapshot.detail.detail;
    const device = detail.device;
    deviceLogDevice =
      device.ownership.toLowerCase() === "owned" ? device.entity.id : null;
    slots = {
      vitals: <DeviceInspectorVitals detail={detail} />,
      body: (
        <DeviceInspector
          device={device}
          detail={detail}
          descriptors={props.descriptors}
          entities={entities}
          onNavigate={navigate}
          onRunCommand={props.onRunCommand}
          onOperationFinished={props.onOperationFinished}
        />
      ),
    };
  } else if (snapshot?.detail.kind === "replicant") {
    const detail = snapshot.detail.detail;
    slots = {
      vitals: <ReplicantInspectorVitals detail={detail} />,
      body: <ReplicantInspector detail={detail} onNavigate={navigate} />,
    };
  } else if (snapshot?.detail.kind === "system") {
    slots = {
      vitals: <SystemInspectorVitals detail={snapshot.detail.detail} />,
      body: (
        <SystemInspector
          summary={summary}
          detail={snapshot.detail.detail}
          onNavigate={navigate}
        />
      ),
    };
  } else if (snapshot?.detail.kind === "location") {
    slots = {
      vitals: <LocationInspectorVitals detail={snapshot.detail.detail} />,
      body: (
        <LocationInspector
          summary={summary}
          detail={snapshot.detail.detail}
          onNavigate={navigate}
        />
      ),
    };
  } else if (isDeviceSummary(value)) {
    deviceLogDevice =
      value.ownership.toLowerCase() === "owned" ? value.entity.id : null;
    slots = {
      body: (
        <DeviceInspector
          device={value}
          descriptors={props.descriptors}
          entities={entities}
          onNavigate={navigate}
          onRunCommand={props.onRunCommand}
          onOperationFinished={props.onOperationFinished}
        />
      ),
    };
  } else if (isWorkflowSummary(value)) {
    const workflowActivity = activity.filter(
      (item) => item.workflow_id === value.id,
    );
    slots = {
      vitals: (
        <>
          <span className="status-chip">{value.status}</span>
          {value.current_step ? <span>{value.current_step}</span> : null}
        </>
      ),
      body: <GenericInspector value={value} />,
      activity: workflowActivity.length ? (
        <ul className="inspector-entity-list">
          {workflowActivity.map((item) => (
            <li key={item.id}>{item.message}</li>
          ))}
        </ul>
      ) : null,
    };
  } else if (isEventSummary(value)) {
    slots = {
      vitals: (
        <>
          {value.status ? (
            <span className="status-chip">{value.status}</span>
          ) : null}
          {value.tier !== null ? <span>Tier {value.tier}</span> : null}
        </>
      ),
      body: <EventInspector event={value} onNavigate={navigate} />,
    };
  } else if (
    entity.kind === "resource" &&
    (value !== undefined || props.revision !== undefined)
  ) {
    slots = {
      body: (
        <ResourceInspector
          resource={entity.id}
          summary={isEntitySummary(value) ? value : undefined}
        />
      ),
    };
  } else {
    slots = { body: <GenericInspector value={value} /> };
  }

  if (intelligence) slots.intelligence = intelligence;
  if (managedKind(entity.kind) || associatedActivity?.length) {
    slots.activity = (
      <InspectorActivityTimeline
        entity={entity}
        workflowActivity={associatedActivity ?? []}
        onNavigate={navigate}
      />
    );
  }

  const targetSystem =
    entity.kind === "system" ? entity.id : (summary.system ?? null);
  const actions =
    targetSystem || entity.kind === "workflow" || deviceLogDevice ? (
      <>
        {deviceLogDevice ? <DeviceLogButton device={deviceLogDevice} /> : null}
        {targetSystem ? (
          <>
            <button
              onClick={() => {
                props.onOpenGalaxy(targetSystem);
              }}
            >
              Show on Galaxy
            </button>
            <button
              onClick={() => {
                props.onOpenSystem(targetSystem);
              }}
            >
              Show on System
            </button>
          </>
        ) : null}
        {entity.kind === "workflow" ? (
          <button
            onClick={() => {
              props.onOpenWorkflow(entity.id);
            }}
          >
            Open in Automation
          </button>
        ) : null}
      </>
    ) : null;

  return (
    <InspectorShell
      summary={summary}
      {...slots}
      provenance={snapshot?.provenance}
      warning={warning}
      actions={actions}
      onClose={props.onClose}
      navigation={props.navigation}
      onClear={props.onClear}
    />
  );
}

function ManagedInspector({ props }: { props: InspectorProps }) {
  const daemon = useDaemonState();
  const workflowIntelligence = useInspectorWorkflowIntelligence();
  const kind = props.entity.kind as
    "device" | "replicant" | "system" | "location";
  const query = useDomainQuery({
    slice: detailSlices(kind),
    queryKey: `inspector:entity:${kind}:${props.entity.id}`,
    fetcher: (signal) =>
      daemonApi.entityInspector(kind, props.entity.id, signal),
    isEmpty: () => false,
  });
  const warning = query.error ? (
    <p className="inline-warning">
      Detail refresh failed; showing current page data. {query.error}
    </p>
  ) : null;
  const snapshot =
    query.data?.summary.entity.id === props.entity.id ? query.data : undefined;
  const summary =
    snapshot?.summary ?? fallbackSummary(props.entity, props.value);
  const workflowIds = associatedWorkflowIds(
    props.entity,
    snapshot,
    daemon.requirements,
    workflowIntelligence,
  );
  const associatedActivity = props.activity
    .filter((item) => workflowIds.includes(item.workflow_id))
    .sort((left, right) => right.occurred_at_ms - left.occurred_at_ms)
    .slice(0, 10);
  const navigate = (kind: string, id: string) => {
    props.onSelectEntity({ kind: kind as SelectedEntity["kind"], id });
  };
  return (
    <InspectorView
      props={props}
      snapshot={snapshot}
      warning={warning}
      intelligence={
        <InspectorIntelligence
          entity={props.entity}
          summary={summary}
          snapshot={snapshot}
          workflowIds={workflowIds}
          workflowIntelligence={workflowIntelligence}
          onNavigate={navigate}
        />
      }
      associatedActivity={associatedActivity}
    />
  );
}

function ContextualInspector({ props }: { props: InspectorProps }) {
  const daemon = useDaemonState();
  const workflowIntelligence = useInspectorWorkflowIntelligence();
  const summary = fallbackSummary(props.entity, props.value);
  const workflowIds = associatedWorkflowIds(
    props.entity,
    undefined,
    daemon.requirements,
    workflowIntelligence,
  );
  const associatedActivity = props.activity
    .filter((item) => workflowIds.includes(item.workflow_id))
    .sort((left, right) => right.occurred_at_ms - left.occurred_at_ms)
    .slice(0, 10);
  const navigate = (kind: string, id: string) => {
    props.onSelectEntity({ kind: kind as SelectedEntity["kind"], id });
  };
  return (
    <InspectorView
      props={props}
      intelligence={
        <InspectorIntelligence
          entity={props.entity}
          summary={summary}
          workflowIds={workflowIds}
          workflowIntelligence={workflowIntelligence}
          onNavigate={navigate}
        />
      }
      associatedActivity={associatedActivity}
    />
  );
}

function WorkflowManagedInspector({ props }: { props: InspectorProps }) {
  const daemon = useDaemonState();
  const workflowIntelligence = useInspectorWorkflowIntelligence();
  const [detail, setDetail] = useState<WorkflowDetail>();
  const [error, setError] = useState<string | null>(null);
  const workflowRevision = daemon.invalidated.workflows;
  useEffect(() => {
    if (daemon.connection !== "connected") return;
    const controller = new AbortController();
    setDetail(undefined);
    setError(null);
    void daemonApi
      .workflow(props.entity.id, controller.signal)
      .then(setDetail)
      .catch((failure: unknown) => {
        if (!controller.signal.aborted) setError(String(failure));
      });
    return () => {
      controller.abort();
    };
  }, [daemon.connection, props.entity.id, workflowRevision]);

  const summary = detail
    ? {
        entity: { kind: "workflow" as const, id: detail.summary.id },
        label: detail.summary.kind,
        secondary_label: detail.summary.id,
        system: null,
        location: null,
        entity_type: detail.summary.kind,
        status: detail.summary.status,
      }
    : fallbackSummary(props.entity, props.value);
  const workflowActivity = props.activity.filter(
    (item) => item.workflow_id === props.entity.id,
  );
  const navigate = (kind: string, id: string) => {
    props.onSelectEntity({ kind: kind as SelectedEntity["kind"], id });
  };
  return (
    <InspectorShell
      summary={summary}
      vitals={
        detail ? (
          <>
            <span className="status-chip">{detail.summary.status}</span>
            {detail.summary.current_step ? (
              <span>{detail.summary.current_step}</span>
            ) : null}
            {detail.wait_reason ? (
              <span className="status-chip busy">Waiting</span>
            ) : null}
            {detail.error ? (
              <span className="status-chip error">Needs attention</span>
            ) : null}
          </>
        ) : undefined
      }
      body={
        detail ? (
          <WorkflowInspector detail={detail} onNavigate={navigate} />
        ) : (
          <p className="empty-state">Loading workflow detail…</p>
        )
      }
      intelligence={
        <InspectorIntelligence
          entity={props.entity}
          summary={summary}
          workflowIds={[props.entity.id]}
          workflowIntelligence={workflowIntelligence}
          onNavigate={navigate}
        />
      }
      activity={
        <InspectorActivityTimeline
          entity={props.entity}
          workflowActivity={workflowActivity}
          onNavigate={navigate}
        />
      }
      warning={
        error ? (
          <p className="inline-warning">
            Workflow detail refresh failed. {error}
          </p>
        ) : null
      }
      actions={
        <button
          onClick={() => {
            props.onOpenWorkflow(props.entity.id);
          }}
        >
          Open in Automation
        </button>
      }
      navigation={props.navigation}
      onClose={props.onClose}
      onClear={props.onClear}
    />
  );
}

function InspectorContent(props: InspectorProps) {
  if (props.entity.kind === "workflow")
    return <WorkflowManagedInspector props={props} />;
  if (managedKind(props.entity.kind)) return <ManagedInspector props={props} />;
  if (
    props.revision !== undefined &&
    (props.entity.kind === "resource" || props.entity.kind === "event")
  )
    return <ContextualInspector props={props} />;
  return <InspectorView props={props} />;
}

function sameEntity(left: SelectedEntity | undefined, right: SelectedEntity) {
  return left?.kind === right.kind && left.id === right.id;
}

function historyLabel(
  entity: SelectedEntity | undefined,
  entities: Record<string, EntitySummary>,
) {
  if (!entity) return null;
  return (
    entities[`${entity.kind}:${entity.id}`]?.label ??
    `${entity.kind} ${entity.id}`
  );
}

export function Inspector(props: InspectorProps) {
  const [history, setHistory] = useState<{
    entries: SelectedEntity[];
    index: number;
  }>(() => ({
    entries: [props.entity],
    index: 0,
  }));

  useEffect(() => {
    setHistory((current) => {
      if (sameEntity(current.entries[current.index], props.entity))
        return current;
      const entries = [
        ...current.entries.slice(0, current.index + 1),
        props.entity,
      ].slice(-50);
      return { entries, index: entries.length - 1 };
    });
  }, [props.entity.kind, props.entity.id]);

  const selectFromInspector = (entity: SelectedEntity) => {
    setHistory((current) => {
      if (sameEntity(current.entries[current.index], entity)) return current;
      const entries = [
        ...current.entries.slice(0, current.index + 1),
        entity,
      ].slice(-50);
      return { entries, index: entries.length - 1 };
    });
    props.onSelectEntity(entity);
  };

  const selectHistoryIndex = useCallback(
    (index: number) => {
      if (
        index < 0 ||
        index >= history.entries.length ||
        index === history.index
      )
        return;
      const entity = history.entries[index];
      if (!entity) return;
      setHistory((current) => ({ ...current, index }));
      props.onSelectEntity(entity);
    },
    [history.entries, history.index, props.onSelectEntity],
  );

  const goBack = () => {
    selectHistoryIndex(history.index - 1);
  };

  const goForward = () => {
    selectHistoryIndex(history.index + 1);
  };

  useEffect(() => {
    const keydown = (event: KeyboardEvent) => {
      if (!event.altKey || event.ctrlKey || event.metaKey || event.shiftKey)
        return;
      if (event.key === "ArrowLeft" && history.index > 0) {
        event.preventDefault();
        selectHistoryIndex(history.index - 1);
      } else if (
        event.key === "ArrowRight" &&
        history.index < history.entries.length - 1
      ) {
        event.preventDefault();
        selectHistoryIndex(history.index + 1);
      }
    };
    window.addEventListener("keydown", keydown);
    return () => {
      window.removeEventListener("keydown", keydown);
    };
  }, [history.index, history.entries.length, selectHistoryIndex]);

  const navigation: InspectorNavigationControls = {
    canGoBack: history.index > 0,
    canGoForward: history.index < history.entries.length - 1,
    backLabel: historyLabel(history.entries[history.index - 1], props.entities),
    forwardLabel: historyLabel(
      history.entries[history.index + 1],
      props.entities,
    ),
    position: history.index,
    total: history.entries.length,
    entries: history.entries.map((entry, index) => ({
      index,
      label: historyLabel(entry, props.entities) ?? `${entry.kind} ${entry.id}`,
    })),
    onBack: goBack,
    onForward: goForward,
    onJump: selectHistoryIndex,
  };

  return (
    <InspectorContent
      {...props}
      navigation={navigation}
      onSelectEntity={selectFromInspector}
    />
  );
}
