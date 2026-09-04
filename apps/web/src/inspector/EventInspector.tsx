import type { EventSummary } from "../protocol";
import { InspectorFields } from "./InspectorFields";

function timestampLabel(value: string | null) {
  if (!value) return null;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp)
    ? new Date(timestamp).toLocaleString()
    : value;
}

export function EventInspector({
  event,
  onNavigate,
}: {
  event: EventSummary;
  onNavigate?: (kind: string, id: string) => void;
}) {
  const rewardItems = [
    ...event.rewards.resources.map((item) => ({
      label: item.item,
      value: item.quantity,
      kind: "resource",
    })),
    ...event.rewards.devices.map((item) => ({
      label: item.item,
      value: item.quantity,
      kind: "device",
    })),
  ];
  return (
    <>
      <InspectorFields
        fields={[
          { label: "Status", value: event.status },
          {
            label: "Type",
            value: [event.event_type, event.category, event.tier].filter(
              Boolean,
            ),
          },
          { label: "System", value: event.system },
          { label: "Location", value: event.location },
          { label: "Discovered", value: timestampLabel(event.discovered_at) },
          { label: "Completed", value: timestampLabel(event.completed_at) },
          { label: "Description", value: event.description },
        ]}
      />
      <section className="inspector-section">
        <h3>Relations</h3>
        <ul className="inspector-entity-list">
          <li>
            <button
              type="button"
              disabled={!onNavigate}
              onClick={() => onNavigate?.("system", event.system)}
            >
              <strong>System</strong>
              <small>{event.system}</small>
            </button>
          </li>
          <li>
            <button
              type="button"
              disabled={!onNavigate}
              onClick={() => onNavigate?.("location", event.location)}
            >
              <strong>Location</strong>
              <small>{event.location}</small>
            </button>
          </li>
        </ul>
      </section>
      {event.criteria.length ? (
        <section className="inspector-section">
          <h3>Requirements</h3>
          <div className="inspector-event-criteria">
            {event.criteria.map((criterion) => (
              <div key={criterion.name} className="inspector-event-criterion">
                <strong>{criterion.name}</strong>
                <span
                  className={`status-chip ${criterion.complete ? "success" : ""}`}
                >
                  {criterion.complete ? "Complete" : "In progress"}
                </span>
                <ul className="inspector-resource-list">
                  {criterion.requirements.map((requirement) => (
                    <li key={`${requirement.kind}:${requirement.item}`}>
                      <span>
                        {requirement.item} <small>({requirement.kind})</small>
                      </span>
                      <strong>
                        {requirement.completed.toLocaleString()} /{" "}
                        {requirement.required.toLocaleString()}
                      </strong>
                    </li>
                  ))}
                </ul>
              </div>
            ))}
          </div>
        </section>
      ) : null}
      {rewardItems.length ||
      event.rewards.xp !== null ||
      event.rewards.civilisation_points !== null ||
      event.rewards.completion_achievement !== null ? (
        <section className="inspector-section">
          <h3>Rewards</h3>
          {rewardItems.length ? (
            <ul className="inspector-resource-list">
              {rewardItems.map((reward) => (
                <li key={`${reward.kind}:${reward.label}`}>
                  <span>
                    {reward.label} <small>({reward.kind})</small>
                  </span>
                  <strong>{reward.value.toLocaleString()}</strong>
                </li>
              ))}
            </ul>
          ) : null}
          <InspectorFields
            fields={[
              { label: "XP", value: event.rewards.xp },
              {
                label: "Civilisation points",
                value: event.rewards.civilisation_points,
              },
              {
                label: "Achievement",
                value: event.rewards.completion_achievement,
              },
            ]}
          />
        </section>
      ) : null}
    </>
  );
}
