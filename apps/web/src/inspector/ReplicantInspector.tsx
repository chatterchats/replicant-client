import type { ReplicantInspectorSummary } from "../protocol";
import { InspectorFields } from "./InspectorFields";
import { TravelSection } from "./TravelInspector";

export function ReplicantInspector({
  detail,
  onNavigate,
}: {
  detail: ReplicantInspectorSummary;
  onNavigate: (kind: string, id: string) => void;
}) {
  return (
    <>
      <InspectorFields
        fields={[
          { label: "Code", value: detail.entity.id },
          { label: "Status", value: detail.status },
          { label: "Ownership", value: detail.ownership },
          { label: "NPC", value: detail.is_npc },
          { label: "Physical region", value: detail.region },
          { label: "Assigned region", value: detail.assigned_region },
          { label: "Director state", value: detail.director_state },
          { label: "Role affinity", value: detail.role_affinity },
          { label: "System", value: detail.system },
          { label: "Location", value: detail.location },
          { label: "Pronouns", value: detail.pronouns },
          { label: "Experience", value: detail.experience_points },
          { label: "Plan", value: detail.plan },
          { label: "Cohort permission", value: detail.cohort_permission },
          { label: "Description", value: detail.description },
        ]}
      />
      <TravelSection travel={detail.travel} />
      {detail.system ||
      detail.location ||
      detail.hosted_device ||
      detail.workflow_id ? (
        <section className="inspector-section">
          <h3>Relations</h3>
          <ul className="inspector-entity-list">
            {detail.system ? (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    if (detail.system) onNavigate("system", detail.system);
                  }}
                >
                  <strong>System</strong>
                  <small>{detail.system}</small>
                </button>
              </li>
            ) : null}
            {detail.location ? (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    if (detail.location)
                      onNavigate("location", detail.location);
                  }}
                >
                  <strong>Location</strong>
                  <small>{detail.location}</small>
                </button>
              </li>
            ) : null}
            {detail.hosted_device ? (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    const hostedDevice = detail.hosted_device;
                    if (hostedDevice)
                      onNavigate(hostedDevice.kind, hostedDevice.id);
                  }}
                >
                  <strong>Hosted vessel</strong>
                  <small>{detail.hosted_device.id}</small>
                </button>
              </li>
            ) : null}
            {detail.workflow_id ? (
              <li>
                <button
                  type="button"
                  onClick={() => {
                    if (detail.workflow_id)
                      onNavigate("workflow", detail.workflow_id);
                  }}
                >
                  <strong>Active workflow</strong>
                  <small>{detail.workflow_id}</small>
                </button>
              </li>
            ) : null}
          </ul>
        </section>
      ) : null}
    </>
  );
}
