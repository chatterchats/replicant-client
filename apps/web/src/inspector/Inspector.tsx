import type { ReactNode } from "react";

import { daemonApi } from "../api";
import type { DescriptorCommand } from "../CommandPalette";
import { DeviceLogPanel } from "../DeviceLogPanel";
import { useDomainQuery } from "../domainQuery";
import type {
  DescriptorCatalog,
  EntityInspectorSnapshot,
  EntitySummary,
  FiniteExecution,
  WorkflowActivity,
} from "../protocol";
import type { SelectedEntity } from "../shellState";
import { DeviceInspector } from "./DeviceInspector";
import { EventInspector } from "./EventInspector";
import { GenericInspector } from "./GenericInspector";
import { InspectorShell } from "./InspectorShell";
import { LocationInspector } from "./LocationInspector";
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

export { relatedDeviceLabel, specializeDeviceCommand } from "./inspectorModel";

interface InspectorSlots {
  vitals?: ReactNode;
  body?: ReactNode;
  relations?: ReactNode;
  contents?: ReactNode;
  activity?: ReactNode;
}

export interface InspectorProps {
  entity: SelectedEntity;
  value: unknown;
  descriptors: DescriptorCatalog;
  entities: Record<string, EntitySummary>;
  activity: WorkflowActivity[];
  onClose: () => void;
  onClear: () => void;
  onOpenGalaxy: (system: string) => void;
  onOpenSystem: (system: string) => void;
  onOpenWorkflow: (workflowId: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
  onOperationFinished: (execution: FiniteExecution) => void;
}

function managedKind(kind: string): kind is "device" | "system" | "location" {
  return kind === "device" || kind === "system" || kind === "location";
}

function detailSlices(kind: "device" | "system" | "location") {
  if (kind === "device") return ["devices", "cargo"] as const;
  if (kind === "system") return ["universe"] as const;
  return ["universe", "devices", "entities"] as const;
}

export function InspectorView({
  props,
  snapshot,
  warning,
}: {
  props: InspectorProps;
  snapshot?: EntityInspectorSnapshot;
  warning?: ReactNode;
}) {
  const { entity, value, entities, activity } = props;
  const summary = snapshot?.summary ?? fallbackSummary(entity, value);
  let slots: InspectorSlots;
  if (snapshot?.detail.kind === "device") {
    const device = snapshot.detail.detail;
    slots = {
      body: (
        <DeviceInspector
          device={device}
          descriptors={props.descriptors}
          entities={entities}
          onRunCommand={props.onRunCommand}
          onOperationFinished={props.onOperationFinished}
        />
      ),
      activity:
        device.ownership.toLowerCase() === "owned" ? (
          <DeviceLogPanel device={device.entity.id} />
        ) : null,
    };
  } else if (snapshot?.detail.kind === "system") {
    slots = {
      body: (
        <SystemInspector summary={summary} detail={snapshot.detail.detail} />
      ),
    };
  } else if (snapshot?.detail.kind === "location") {
    slots = {
      body: (
        <LocationInspector summary={summary} detail={snapshot.detail.detail} />
      ),
    };
  } else if (isDeviceSummary(value)) {
    slots = {
      body: (
        <DeviceInspector
          device={value}
          descriptors={props.descriptors}
          entities={entities}
          onRunCommand={props.onRunCommand}
          onOperationFinished={props.onOperationFinished}
        />
      ),
      activity:
        value.ownership.toLowerCase() === "owned" ? (
          <DeviceLogPanel device={value.entity.id} />
        ) : null,
    };
  } else if (isWorkflowSummary(value)) {
    const workflowActivity = activity.filter(
      (item) => item.workflow_id === value.id,
    );
    slots = {
      body: <WorkflowInspector workflow={value} />,
      activity: workflowActivity.length ? (
        <ul className="inspector-entity-list">
          {workflowActivity.map((item) => (
            <li key={item.id}>{item.message}</li>
          ))}
        </ul>
      ) : null,
    };
  } else if (isEventSummary(value)) {
    slots = { body: <EventInspector event={value} /> };
  } else if (entity.kind === "resource" && isEntitySummary(value)) {
    slots = { body: <ResourceInspector summary={value} /> };
  } else {
    slots = { body: <GenericInspector value={value} /> };
  }

  const targetSystem =
    entity.kind === "system" ? entity.id : (summary.system ?? null);
  const actions =
    targetSystem || entity.kind === "workflow" ? (
      <>
        {targetSystem ? (
          <>
            <button onClick={() => props.onOpenGalaxy(targetSystem)}>
              Show on Galaxy
            </button>
            <button onClick={() => props.onOpenSystem(targetSystem)}>
              Show on System
            </button>
          </>
        ) : null}
        {entity.kind === "workflow" ? (
          <button onClick={() => props.onOpenWorkflow(entity.id)}>
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
      onClear={props.onClear}
    />
  );
}

function ManagedInspector({ props }: { props: InspectorProps }) {
  const kind = props.entity.kind as "device" | "system" | "location";
  const query = useDomainQuery({
    slice: detailSlices(kind),
    fetcher: (signal) =>
      daemonApi.entityInspector(kind, props.entity.id, signal),
    isEmpty: () => false,
  });
  const warning = query.error ? (
    <p className="inline-warning">
      Detail refresh failed; showing current page data. {query.error}
    </p>
  ) : null;
  return (
    <InspectorView props={props} snapshot={query.data} warning={warning} />
  );
}

export function Inspector(props: InspectorProps) {
  return managedKind(props.entity.kind) ? (
    <ManagedInspector props={props} />
  ) : (
    <InspectorView props={props} />
  );
}
