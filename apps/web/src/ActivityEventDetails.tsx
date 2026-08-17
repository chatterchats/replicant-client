import type { AccountEventSummary } from "./protocol";

function asRecord(value: unknown): Record<string, unknown> | null {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null;
}

function humanize(value: string): string {
  return value
    .replace(/[._-]+/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase());
}

function primitiveText(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number") return value.toLocaleString();
  if (typeof value === "string") return value || "—";
  return String(value);
}

function StructuredValue({
  value,
  depth = 0,
}: {
  value: unknown;
  depth?: number;
}) {
  if (value === null || value === undefined || typeof value !== "object") {
    return <span>{primitiveText(value)}</span>;
  }

  if (Array.isArray(value)) {
    if (!value.length) return <span>None</span>;
    return (
      <div className="activity-payload-list">
        {value.map((item, index) => (
          <div className="activity-payload-list-item" key={index}>
            <StructuredValue value={item} depth={depth + 1} />
          </div>
        ))}
      </div>
    );
  }

  const entries = Object.entries(value as Record<string, unknown>);
  if (!entries.length) return <span>No details</span>;
  return (
    <dl className="activity-payload-fields">
      {entries.map(([key, item]) => (
        <div key={key}>
          <dt>{humanize(key)}</dt>
          <dd>
            <StructuredValue value={item} depth={depth + 1} />
          </dd>
        </div>
      ))}
    </dl>
  );
}

function ActivitySummary({ value }: { value: Record<string, unknown> }) {
  const counts = asRecord(value.counts);
  const window = Array.isArray(value.window) ? value.window : [];
  const extra = Object.fromEntries(
    Object.entries(value).filter(
      ([key]) => !["counts", "event_count", "window"].includes(key),
    ),
  );
  return (
    <div className="activity-digest-section">
      <h4>Activity</h4>
      <div className="activity-metrics">
        {value.event_count !== undefined && (
          <span>
            <strong>{primitiveText(value.event_count)}</strong> events
          </span>
        )}
        {window.length > 0 && (
          <span>
            Window {window.map((item) => primitiveText(item)).join(" → ")}
          </span>
        )}
      </div>
      {counts && Object.keys(counts).length > 0 && (
        <div className="activity-counts">
          {Object.entries(counts).map(([name, count]) => (
            <span className="status-chip" key={name}>
              {humanize(name)} · {primitiveText(count)}
            </span>
          ))}
        </div>
      )}
      {Object.keys(extra).length > 0 && <StructuredValue value={extra} />}
    </div>
  );
}

function DeviceSummaries({ devices }: { devices: unknown[] }) {
  if (!devices.length) return null;
  return (
    <div className="activity-digest-section">
      <h4>Managed devices</h4>
      <div className="activity-device-grid">
        {devices.map((value, index) => {
          const device = asRecord(value);
          if (!device) {
            return (
              <div className="activity-device-card" key={index}>
                <StructuredValue value={value} />
              </div>
            );
          }
          const known = new Set([
            "device_code",
            "status",
            "events",
            "last_event",
          ]);
          const extra = Object.fromEntries(
            Object.entries(device).filter(([key]) => !known.has(key)),
          );
          return (
            <div
              className="activity-device-card"
              key={String(device.device_code ?? index)}
            >
              <strong>{primitiveText(device.device_code)}</strong>
              <span>{primitiveText(device.status)}</span>
              {device.events !== undefined && (
                <small>{primitiveText(device.events)} events</small>
              )}
              {device.last_event !== undefined && (
                <small>Last: {primitiveText(device.last_event)}</small>
              )}
              {Object.keys(extra).length > 0 && (
                <StructuredValue value={extra} depth={1} />
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

/**
 * Human-readable renderer for forward-compatible account event payloads.
 *
 * AMI digests get first-class sections for the stable envelope fields the SDK
 * already understands, while unknown report shapes remain visible through a
 * recursive labelled view instead of falling back to a wall of JSON.
 */
export function ActivityEventDetails({
  event,
  compact = false,
}: {
  event: AccountEventSummary;
  compact?: boolean;
}) {
  const payload = event.payload ?? {};
  const directive = payload.directive;
  const activity = asRecord(payload.activity);
  const report = asRecord(payload.report);
  const devices = Array.isArray(payload.devices) ? payload.devices : [];
  const known = new Set(["directive", "activity", "report", "devices"]);
  const extra = Object.fromEntries(
    Object.entries(payload).filter(([key]) => !known.has(key)),
  );

  return (
    <div className={`activity-event-details${compact ? " compact" : ""}`}>
      {directive !== undefined && (
        <div className="activity-digest-section activity-directive">
          <h4>Directive</h4>
          <strong>
            {typeof directive === "string"
              ? humanize(directive)
              : primitiveText(directive)}
          </strong>
        </div>
      )}
      {activity && <ActivitySummary value={activity} />}
      <DeviceSummaries devices={devices} />
      {report && (
        <div className="activity-digest-section">
          <h4>Report</h4>
          <StructuredValue value={report} />
        </div>
      )}
      {Object.keys(extra).length > 0 && (
        <div className="activity-digest-section">
          <h4>{event.ami_digest ? "Additional data" : "Payload"}</h4>
          <StructuredValue value={extra} />
        </div>
      )}
      {!Object.keys(payload).length && <span>No event payload.</span>}
    </div>
  );
}
