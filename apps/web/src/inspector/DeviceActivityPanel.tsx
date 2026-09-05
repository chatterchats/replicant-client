import { useEffect, useState, type ReactNode } from "react";

import type { DeviceRuntimeInspectorSummary } from "../protocol";

function record(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function number(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function humanize(value: string) {
  return value
    .replace(/[._-]+/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function formatDuration(seconds: number | null) {
  if (seconds === null || seconds < 0) return null;
  const whole = Math.round(seconds);
  const days = Math.floor(whole / 86_400);
  const hours = Math.floor((whole % 86_400) / 3600);
  const minutes = Math.floor((whole % 3600) / 60);
  const remainder = whole % 60;
  if (days) return `${String(days)}d ${String(hours)}h`;
  if (hours) return `${String(hours)}h ${String(minutes)}m`;
  if (minutes) return `${String(minutes)}m ${String(remainder)}s`;
  return `${String(remainder)}s`;
}

function parseTimestamp(value: unknown) {
  const source = text(value);
  if (!source) return null;
  const parsed = Date.parse(source);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatTimestamp(value: unknown) {
  const source = text(value);
  if (!source) return null;
  const parsed = parseTimestamp(source);
  return parsed === null ? source : new Date(parsed).toLocaleString();
}

function Progress({ value }: { value: number | null }) {
  if (value === null) return null;
  const clamped = Math.max(0, Math.min(100, value));
  return (
    <div
      className="inspector-progress"
      aria-label={`${String(Math.round(clamped))}% complete`}
    >
      <progress max={100} value={clamped} />
      <span>{Math.round(clamped)}%</span>
    </div>
  );
}

function Remaining({
  completesAt,
  reportedSeconds,
}: {
  completesAt: unknown;
  reportedSeconds: number | null;
}) {
  const completion = parseTimestamp(completesAt);
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (completion === null) return undefined;
    const timer = window.setInterval(() => {
      setNow(Date.now());
    }, 30_000);
    return () => {
      window.clearInterval(timer);
    };
  }, [completion]);
  const remaining =
    completion === null
      ? reportedSeconds
      : completion > now
        ? (completion - now) / 1000
        : (reportedSeconds ?? 0);
  const label = formatDuration(remaining);
  return label ? <span>{label} remaining</span> : null;
}

function Facts({
  facts,
}: {
  facts: Array<[string, string | number | ReactNode | null]>;
}) {
  const visible = facts.filter(([, value]) => value !== null && value !== "");
  if (!visible.length) return null;
  return (
    <dl className="inspector-activity-facts">
      {visible.map(([label, value]) => (
        <div key={label}>
          <dt>{label}</dt>
          <dd>{value}</dd>
        </div>
      ))}
    </dl>
  );
}

function relation(
  label: string | null,
  kind: string,
  onNavigate?: (kind: string, id: string) => void,
) {
  if (!label) return null;
  return (
    <button
      type="button"
      className="inspector-inline-link"
      disabled={!onNavigate}
      onClick={() => onNavigate?.(kind, label)}
    >
      {label}
    </button>
  );
}

function ActivityCard({
  title,
  value,
  onNavigate,
}: {
  title: string;
  value: unknown;
  onNavigate?: (kind: string, id: string) => void;
}) {
  const item = record(value);
  if (!item) return null;
  const progress = number(item.progress_percent);
  const etaSeconds = number(item.eta_seconds);
  const remaining = (
    <Remaining completesAt={item.completes_at} reportedSeconds={etaSeconds} />
  );

  if (title === "Printing") {
    return (
      <article className="inspector-activity-card">
        <header>
          <strong>Printing {text(item.device_type) ?? "device"}</strong>
          {remaining}
        </header>
        <Progress value={progress} />
        <Facts
          facts={[
            ["Completes", formatTimestamp(item.completes_at)],
            ["Started", formatTimestamp(item.started_at)],
            ["Quantity", number(item.quantity)],
            ["Tags", Array.isArray(item.tags) ? item.tags.join(", ") : null],
          ]}
        />
      </article>
    );
  }

  if (title === "Mining") {
    const belt = text(item.belt);
    return (
      <article className="inspector-activity-card">
        <header>
          <strong>Mining {text(item.resource_type) ?? "resource"}</strong>
          {remaining}
        </header>
        <Progress value={progress} />
        <Facts
          facts={[
            ["Belt", relation(belt, "location", onNavigate)],
            ["Availability", text(item.availability)],
            ["Density", text(item.density)],
            ["Cycle time", formatDuration(number(item.cycle_time_seconds))],
            ["Pending cycles", number(item.pending_cycles)],
            ["Pending quantity", number(item.pending_quantity)],
            ["Total mined", number(item.quantity_mined)],
            ["Started", formatTimestamp(item.started_at)],
          ]}
        />
      </article>
    );
  }

  if (title === "Prospecting") {
    const direction = Array.isArray(item.direction)
      ? item.direction.filter((entry) => typeof entry === "number").join(", ")
      : null;
    return (
      <article className="inspector-activity-card">
        <header>
          <strong>Prospecting</strong>
          {remaining}
        </header>
        <Progress value={progress} />
        <Facts
          facts={[
            ["Origin", text(item.origin)],
            ["Direction", direction],
            ["Completes", formatTimestamp(item.completes_at)],
            ["Started", formatTimestamp(item.started_at)],
          ]}
        />
      </article>
    );
  }

  if (title === "Scanning") {
    return (
      <article className="inspector-activity-card">
        <header>
          <strong>Scanning {text(item.target) ?? "target"}</strong>
          {remaining}
        </header>
        <Progress value={progress} />
        <Facts
          facts={[
            ["Completes", formatTimestamp(item.completes_at)],
            ["Started", formatTimestamp(item.started_at)],
          ]}
        />
      </article>
    );
  }

  if (title === "Repair") {
    const target = text(item.target_device_code);
    return (
      <article className="inspector-activity-card">
        <header>
          <strong>Repairing</strong>
          {target ? relation(target, "device", onNavigate) : null}
          {remaining}
        </header>
        <Progress value={progress} />
        <Facts
          facts={[
            ["Completes", formatTimestamp(item.completes_at)],
            ["Started", formatTimestamp(item.started_at)],
          ]}
        />
      </article>
    );
  }

  if (title === "Waiting") {
    const target =
      text(item.device_code) ??
      text(item.target_device_code) ??
      text(item.target_device);
    return (
      <article className="inspector-activity-card warning">
        <header>
          <strong>Waiting</strong>
          {target ? relation(target, "device", onNavigate) : null}
        </header>
        <Facts
          facts={[
            [
              "For",
              text(item.reason) ?? text(item.state) ?? text(item.operation),
            ],
            ["Command", text(item.command)],
            [
              "Since",
              formatTimestamp(item.started_at) ?? formatTimestamp(item.since),
            ],
            [
              "Until",
              formatTimestamp(item.until) ?? formatTimestamp(item.resumes_at),
            ],
          ]}
        />
      </article>
    );
  }

  return (
    <article className="inspector-activity-card">
      <header>
        <strong>{title}</strong>
      </header>
      <Facts
        facts={Object.entries(item).flatMap(([key, entry]) => {
          if (typeof entry === "string" || typeof entry === "number") {
            return [[humanize(key), entry] as [string, string | number]];
          }
          return [];
        })}
      />
    </article>
  );
}

function PrintQueue({ jobs }: { jobs: Record<string, unknown>[] }) {
  if (!jobs.length) return null;
  return (
    <section className="inspector-section">
      <h3>Print queue</h3>
      <ol className="inspector-job-list">
        {jobs.map((job, index) => {
          const progress = number(job.progress_percent);
          const deviceType = text(job.device_type);
          const quantity = number(job.quantity);
          const status = text(job.status);
          return (
            <li key={`${deviceType ?? "job"}:${String(index)}`}>
              <strong>{deviceType ?? `Job ${String(index + 1)}`}</strong>
              {quantity !== null ? <span>× {quantity}</span> : null}
              {status ? <small>{humanize(status)}</small> : null}
              {progress !== null ? (
                <div className="inspector-job-progress">
                  <progress
                    max={100}
                    value={Math.max(0, Math.min(100, progress))}
                  />
                  <small>{Math.round(progress)}%</small>
                </div>
              ) : null}
            </li>
          );
        })}
      </ol>
    </section>
  );
}

export function DeviceActivityPanel({
  runtime,
  onNavigate,
}: {
  runtime: DeviceRuntimeInspectorSummary;
  onNavigate?: (kind: string, id: string) => void;
}) {
  const cards = [
    ["Printing", runtime.printing],
    ["Mining", runtime.mining],
    ["Prospecting", runtime.prospect],
    ["Scanning", runtime.scan],
    ["Repair", runtime.repair],
    ["Waiting", runtime.waiting_for],
  ] as const;
  const active = cards.filter(([, value]) => value !== null);
  return (
    <>
      {active.length ? (
        <section className="inspector-section">
          <h3>Current activity</h3>
          <div className="inspector-activity-cards">
            {active.map(([title, value]) => (
              <ActivityCard
                key={title}
                title={title}
                value={value}
                onNavigate={onNavigate}
              />
            ))}
          </div>
        </section>
      ) : null}
      <PrintQueue jobs={runtime.print_queue} />
    </>
  );
}
