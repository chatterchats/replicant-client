import type { WorkflowDetail } from "../protocol";
import { InspectorFields } from "./InspectorFields";

function parameterValue(value: unknown) {
  if (value === null || value === undefined) return "—";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean")
    return String(value);
  try {
    return JSON.stringify(value);
  } catch {
    return "Unserializable value";
  }
}

export function WorkflowInspector({
  detail,
  onNavigate,
}: {
  detail: WorkflowDetail;
  onNavigate?: (kind: string, id: string) => void;
}) {
  return (
    <>
      <InspectorFields
        fields={[
          { label: "Kind", value: detail.summary.kind },
          { label: "Status", value: detail.summary.status },
          { label: "Current step", value: detail.summary.current_step },
          {
            label: "Created",
            value: new Date(detail.created_at_ms).toLocaleString(),
          },
          {
            label: "Updated",
            value: new Date(detail.summary.updated_at_ms).toLocaleString(),
          },
          {
            label: "Finished",
            value:
              detail.finished_at_ms === null
                ? null
                : new Date(detail.finished_at_ms).toLocaleString(),
          },
          { label: "Waiting on", value: detail.wait_reason },
          { label: "Failure", value: detail.error },
        ]}
      />
      {detail.parent_id ? (
        <section className="inspector-section">
          <h3>Orchestration</h3>
          <ul className="inspector-entity-list">
            <li>
              <button
                type="button"
                disabled={!onNavigate}
                onClick={() => onNavigate?.("workflow", detail.parent_id!)}
              >
                <strong>Parent workflow</strong>
                <small>{detail.parent_id}</small>
              </button>
            </li>
          </ul>
        </section>
      ) : null}
      <section className="inspector-section">
        <h3>Parameters</h3>
        {Object.keys(detail.parameters).length ? (
          <InspectorFields
            fields={Object.entries(detail.parameters).map(([key, value]) => ({
              label: key.replaceAll("_", " "),
              value: parameterValue(value),
            }))}
          />
        ) : (
          <p className="empty-state">No configured parameters.</p>
        )}
      </section>
      {detail.targets.length ? (
        <section className="inspector-section">
          <h3>Structured targets</h3>
          <ul className="inspector-entity-list">
            {detail.targets.map((target) => {
              const navigable = [
                "event",
                "system",
                "location",
                "device",
                "resource",
              ].includes(target.kind);
              return (
                <li key={`${target.kind}:${target.key}`}>
                  <button
                    type="button"
                    disabled={!onNavigate || !navigable}
                    onClick={() =>
                      navigable && onNavigate?.(target.kind, target.key)
                    }
                  >
                    <strong>{target.key}</strong>
                    <small>
                      {target.kind} · {target.active ? "selected" : "released"}
                      {target.location
                        ? ` · ${target.location}`
                        : target.system
                          ? ` · ${target.system}`
                          : ""}
                    </small>
                  </button>
                </li>
              );
            })}
          </ul>
        </section>
      ) : null}
      {detail.reservations.length ? (
        <section className="inspector-section">
          <h3>Active reservations</h3>
          <ul className="inspector-entity-list">
            {detail.reservations.map((reservation) => {
              const target =
                reservation.entity ??
                (reservation.resource
                  ? { kind: "resource", id: reservation.resource }
                  : null);
              return (
                <li key={reservation.allocation_id}>
                  <button
                    type="button"
                    disabled={!onNavigate || target === null}
                    onClick={() =>
                      target && onNavigate?.(target.kind, target.id)
                    }
                  >
                    <strong>
                      {reservation.quantity.toLocaleString()} ·{" "}
                      {reservation.requirement_key}
                    </strong>
                    <small>
                      {reservation.resource ?? reservation.pool_identity}
                      {reservation.location ? ` · ${reservation.location}` : ""}
                    </small>
                  </button>
                </li>
              );
            })}
          </ul>
        </section>
      ) : null}
      {detail.claims.length ? (
        <section className="inspector-section">
          <h3>Claimed resources</h3>
          <ul className="inspector-entity-list">
            {detail.claims.map((claim) => (
              <li key={`${claim.kind}:${claim.id}`}>
                <button
                  type="button"
                  disabled={!onNavigate}
                  onClick={() => onNavigate?.(claim.kind, claim.id)}
                >
                  <strong>{claim.id}</strong>
                  <small>{claim.kind}</small>
                </button>
              </li>
            ))}
          </ul>
        </section>
      ) : null}
      <section className="inspector-section">
        <h3>Advanced</h3>
        <InspectorFields
          fields={[
            { label: "Revision", value: detail.summary.revision },
            { label: "Schema version", value: detail.schema_version },
          ]}
        />
      </section>
    </>
  );
}
