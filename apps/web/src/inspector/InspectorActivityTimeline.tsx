import { useMemo } from "react";

import { daemonApi } from "../api";
import { useDomainQuery } from "../domainQuery";
import type { AccountEventSummary, WorkflowActivity } from "../protocol";
import type { SelectedEntity } from "../shellState";

interface TimelineItem {
  id: string;
  occurredAtMs: number;
  level: "debug" | "info" | "warning" | "error";
  source: "game" | "workflow";
  title: string;
  detail: string | null;
  workflowId: string | null;
}

function activityFilter(entity: SelectedEntity) {
  if (entity.kind === "device") return { device: entity.id, limit: 30 };
  if (entity.kind === "replicant") return { replicant: entity.id, limit: 30 };
  if (entity.kind === "system") return { system: entity.id, limit: 30 };
  if (entity.kind === "location") return { location: entity.id, limit: 30 };
  // Event/resource/workflow relationships already have durable workflow
  // activity. Do not imply that every event at their surrounding location is
  // activity for the selected logical entity.
  return null;
}

function eventLevel(event: AccountEventSummary): TimelineItem["level"] {
  const key = `${event.name} ${event.category}`.toLowerCase();
  if (key.includes("failed") || key.includes("error")) return "error";
  if (key.includes("warning") || key.includes("blocked")) return "warning";
  return "info";
}

function humanize(value: string) {
  return value
    .replace(/[._-]+/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function scalar(value: unknown): string | null {
  if (typeof value === "string" && value.trim()) return value;
  if (typeof value === "number" || typeof value === "boolean")
    return String(value);
  return null;
}

function eventDetail(event: AccountEventSummary): string | null {
  const preferred = [
    "message",
    "reason",
    "status",
    "destination",
    "target",
    "location",
    "system",
    "device_code",
  ];
  for (const key of preferred) {
    const value = scalar(event.payload[key]);
    if (value) return value;
  }
  const entries = Object.entries(event.payload)
    .flatMap(([key, value]) => {
      const rendered = scalar(value);
      return rendered ? [`${humanize(key)}: ${rendered}`] : [];
    })
    .slice(0, 2);
  return entries.length ? entries.join(" · ") : null;
}

function gameItem(event: AccountEventSummary): TimelineItem {
  return {
    id: `game:${event.id}`,
    occurredAtMs: Date.parse(event.occurred_at) || 0,
    level: eventLevel(event),
    source: "game",
    title: humanize(event.name),
    detail: eventDetail(event),
    workflowId: null,
  };
}

function workflowItem(activity: WorkflowActivity): TimelineItem {
  return {
    id: `workflow:${String(activity.id)}`,
    occurredAtMs: activity.occurred_at_ms,
    level: activity.level,
    source: "workflow",
    title: activity.message,
    detail: activity.step,
    workflowId: activity.workflow_id,
  };
}

function Timeline({
  items,
  onNavigate,
}: {
  items: TimelineItem[];
  onNavigate: (kind: string, id: string) => void;
}) {
  if (!items.length) return <p className="empty-state">No recent activity.</p>;
  return (
    <ol className="inspector-timeline">
      {items.map((item) => {
        const content = (
          <>
            <span className="inspector-timeline-meta">
              <span className={`status-chip ${item.level}`}>{item.source}</span>
              <time dateTime={new Date(item.occurredAtMs).toISOString()}>
                {new Date(item.occurredAtMs).toLocaleString()}
              </time>
            </span>
            <strong>{item.title}</strong>
            {item.detail ? <small>{item.detail}</small> : null}
          </>
        );
        return (
          <li key={item.id} className={`inspector-timeline-item ${item.level}`}>
            {item.workflowId ? (
              <button
                type="button"
                className="inspector-timeline-link"
                onClick={() => onNavigate("workflow", item.workflowId!)}
              >
                {content}
              </button>
            ) : (
              <div className="inspector-timeline-card">{content}</div>
            )}
          </li>
        );
      })}
    </ol>
  );
}

function EntityTimeline({
  entity,
  workflowActivity,
  onNavigate,
}: {
  entity: SelectedEntity;
  workflowActivity: WorkflowActivity[];
  onNavigate: (kind: string, id: string) => void;
}) {
  const filter = activityFilter(entity)!;
  const queryKey = `inspector:activity:${entity.kind}:${entity.id}`;
  const history = useDomainQuery({
    slice: "activity",
    queryKey,
    fetcher: (signal) => daemonApi.activity(filter, signal),
    isEmpty: (value) => value.events.length === 0,
  });
  const items = useMemo(
    () =>
      [
        ...(history.data?.events ?? []).map(gameItem),
        ...workflowActivity.map(workflowItem),
      ]
        .sort((left, right) => right.occurredAtMs - left.occurredAtMs)
        .slice(0, 20),
    [history.data?.events, workflowActivity],
  );
  if (!items.length && history.status === "loading") {
    return <p className="empty-state">Loading recent activity…</p>;
  }
  if (!items.length) return null;
  return (
    <>
      {history.error ? (
        <p className="inline-warning">
          Game-event history refresh failed. {history.error}
        </p>
      ) : null}
      <Timeline items={items} onNavigate={onNavigate} />
    </>
  );
}

export function InspectorActivityTimeline({
  entity,
  workflowActivity,
  onNavigate,
}: {
  entity: SelectedEntity;
  workflowActivity: WorkflowActivity[];
  onNavigate: (kind: string, id: string) => void;
}) {
  const filter = activityFilter(entity);
  if (filter) {
    return (
      <EntityTimeline
        entity={entity}
        workflowActivity={workflowActivity}
        onNavigate={onNavigate}
      />
    );
  }
  return workflowActivity.length ? (
    <Timeline
      items={workflowActivity
        .map(workflowItem)
        .sort((left, right) => right.occurredAtMs - left.occurredAtMs)
        .slice(0, 20)}
      onNavigate={onNavigate}
    />
  ) : null;
}
