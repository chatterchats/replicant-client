import type {
  DeviceInspectorSummary,
  LocationInspectorSummary,
  ReplicantInspectorSummary,
  SystemInspectorSummary,
} from "../protocol";
import { TravelVitals } from "./TravelInspector";

function humanize(value: string | null | undefined) {
  return value
    ? value
        .replace(/[._-]+/g, " ")
        .replace(/\b\w/g, (letter) => letter.toUpperCase())
    : null;
}

function deviceActivity(detail: DeviceInspectorSummary) {
  if (detail.travel) return null;
  if (detail.runtime.printing) return "Printing";
  if (detail.runtime.mining) return "Mining";
  if (detail.runtime.prospect) return "Prospecting";
  if (detail.runtime.scan) return "Scanning";
  if (detail.runtime.repair) return "Repairing";
  if (detail.runtime.waiting_for) return "Waiting";
  if (detail.device.active_directive)
    return humanize(detail.device.active_directive);
  return null;
}

export function DeviceInspectorVitals({
  detail,
}: {
  detail: DeviceInspectorSummary;
}) {
  const activity = deviceActivity(detail);
  return (
    <>
      <TravelVitals travel={detail.travel} status={detail.device.status} />
      {activity ? <span className="status-chip busy">{activity}</span> : null}
      {detail.runtime.waiting_for && activity !== "Waiting" ? (
        <span className="status-chip busy">Waiting</span>
      ) : null}
      {detail.device.claim ? (
        <span>Automation · {detail.device.claim.workflow_kind}</span>
      ) : null}
      {detail.device.operational_capacity_percent !== null ? (
        <span>
          {Math.round(detail.device.operational_capacity_percent)}% operational
        </span>
      ) : null}
    </>
  );
}

export function ReplicantInspectorVitals({
  detail,
}: {
  detail: ReplicantInspectorSummary;
}) {
  return (
    <>
      <TravelVitals travel={detail.travel} status={detail.status} />
      {detail.director_state ? (
        <span className="status-chip">{humanize(detail.director_state)}</span>
      ) : null}
      {detail.assigned_region ? <span>{detail.assigned_region}</span> : null}
      {detail.region &&
      detail.assigned_region &&
      detail.region !== detail.assigned_region ? (
        <span className="status-chip busy">Away from assigned region</span>
      ) : null}
      {detail.workflow_id ? <span>Automation busy</span> : null}
    </>
  );
}

export function SystemInspectorVitals({
  detail,
}: {
  detail: SystemInspectorSummary;
}) {
  return (
    <>
      {detail.region ? <span>{detail.region}</span> : null}
      {detail.explored !== null ? (
        <span className={`status-chip ${detail.explored ? "available" : ""}`}>
          {detail.explored ? "Explored" : "Unexplored"}
        </span>
      ) : null}
      {detail.has_hub ? (
        <span className="status-chip available">System Hub</span>
      ) : null}
      {detail.has_ward ? <span>Ward</span> : null}
      {detail.has_life ? <span>Life detected</span> : null}
    </>
  );
}

export function LocationInspectorVitals({
  detail,
}: {
  detail: LocationInspectorSummary;
}) {
  const surveyComplete = detail.survey.system_complete;
  return (
    <>
      {detail.location_type ? (
        <span>{humanize(detail.location_type)}</span>
      ) : null}
      {detail.scanned !== null ? (
        <span className={`status-chip ${detail.scanned ? "available" : ""}`}>
          {detail.scanned ? "Scanned" : "Unscanned"}
        </span>
      ) : null}
      {surveyComplete ? (
        <span className="status-chip available">System survey complete</span>
      ) : null}
      {detail.resource_sites.length ? (
        <span>
          {detail.resource_sites.length} resource{" "}
          {detail.resource_sites.length === 1 ? "site" : "sites"}
        </span>
      ) : null}
    </>
  );
}
