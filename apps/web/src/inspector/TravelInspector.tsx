import { useEffect, useMemo, useState } from "react";

import type { TravelInspectorSummary } from "../protocol";
import { InspectorFields } from "./InspectorFields";

function parseTimestamp(value: string | null) {
  if (!value) return null;
  const timestamp = Date.parse(value);
  return Number.isFinite(timestamp) ? timestamp : null;
}

function formatDuration(seconds: number) {
  const value = Math.max(0, Math.round(seconds));
  const days = Math.floor(value / 86_400);
  const hours = Math.floor((value % 86_400) / 3_600);
  const minutes = Math.floor((value % 3_600) / 60);
  if (days > 0) return `${String(days)}d ${String(hours)}h`;
  if (hours > 0) return `${String(hours)}h ${String(minutes)}m`;
  if (minutes > 0) return `${String(minutes)}m`;
  return `${String(value)}s`;
}

function humanize(value: string | null) {
  return value
    ? value
        .replace(/[._-]+/g, " ")
        .replace(/\b\w/g, (letter) => letter.toUpperCase())
    : null;
}

function timestampLabel(value: string | null) {
  const timestamp = parseTimestamp(value);
  return timestamp === null ? value : new Date(timestamp).toLocaleString();
}

export function TravelVitals({
  travel,
  status,
}: {
  travel: TravelInspectorSummary | null;
  status?: string | null;
}) {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!travel) return undefined;
    const timer = window.setInterval(() => {
      setNow(Date.now());
    }, 30_000);
    return () => {
      window.clearInterval(timer);
    };
  }, [travel]);

  const arrival = parseTimestamp(
    travel?.final_arrives_at ?? travel?.arrives_at ?? null,
  );
  const remaining =
    arrival === null
      ? (travel?.route_eta_seconds ?? travel?.eta_seconds ?? null)
      : Math.max(0, (arrival - now) / 1000);
  const destination = travel?.final_destination ?? travel?.destination ?? null;
  if (!travel && !status) return null;

  return (
    <>
      {status ? <span className="status-chip">{humanize(status)}</span> : null}
      {destination ? (
        <strong className="inspector-vital-primary">
          {humanize(travel?.travel_type ?? null) ?? "Travelling"} →{" "}
          {destination}
        </strong>
      ) : null}
      {arrival !== null ? (
        <span>Arrival {new Date(arrival).toLocaleString()}</span>
      ) : null}
      {remaining !== null ? (
        <span>{formatDuration(remaining)} remaining</span>
      ) : null}
    </>
  );
}

export function TravelSection({
  travel,
}: {
  travel: TravelInspectorSummary | null;
}) {
  const currentDestination = travel?.destination ?? null;
  const finalDestination = travel?.final_destination ?? null;
  const showFinal =
    finalDestination !== null && finalDestination !== currentDestination;
  const currentArrival = travel?.arrives_at ?? null;
  const finalArrival = travel?.final_arrives_at ?? null;
  const fields = useMemo(
    () => [
      { label: "Type", value: humanize(travel?.travel_type ?? null) },
      { label: "Stage", value: humanize(travel?.stage ?? null) },
      { label: "Origin", value: travel?.origin },
      { label: "Current destination", value: currentDestination },
      { label: "Current arrival", value: timestampLabel(currentArrival) },
      {
        label: "Final destination",
        value: showFinal ? finalDestination : null,
      },
      {
        label: "Final arrival",
        value:
          showFinal || finalArrival !== currentArrival
            ? timestampLabel(finalArrival)
            : null,
      },
      { label: "Departed", value: timestampLabel(travel?.departed_at ?? null) },
      {
        label: "Current ETA",
        value:
          travel?.eta_seconds === null || travel?.eta_seconds === undefined
            ? null
            : formatDuration(travel.eta_seconds),
      },
      {
        label: "Route ETA",
        value:
          travel?.route_eta_seconds === null ||
          travel?.route_eta_seconds === undefined
            ? null
            : formatDuration(travel.route_eta_seconds),
      },
    ],
    [
      currentArrival,
      currentDestination,
      finalArrival,
      finalDestination,
      showFinal,
      travel,
    ],
  );

  if (!travel) return null;
  return (
    <section className="inspector-section" aria-label="Travel details">
      <h3>Travel</h3>
      <InspectorFields fields={fields} />
    </section>
  );
}
