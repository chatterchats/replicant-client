import type { ReactNode } from "react";

import { presentInspectorValue } from "./InspectorFields";

function humanizeInspectorKey(value: string) {
  return value
    .replace(/[._-]+/g, " ")
    .replace(/\b\w/g, (letter) => letter.toUpperCase())
    .replace(/\bAu\b/g, "AU")
    .replace(/\bPct\b/g, "%");
}

function plainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function primitive(value: unknown) {
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number" || typeof value === "bigint")
    return value.toString();
  return String(value);
}

function scarcityLevel(value: unknown) {
  return typeof value === "string"
    ? value.toLowerCase().replace(/[^a-z0-9]+/g, "-")
    : "unknown";
}

function ResourceScarcity({ value }: { value: Record<string, unknown> }) {
  return (
    <div className="inspector-resource-scarcity">
      {Object.entries(value).map(([resource, scarcity]) => (
        <div
          className="inspector-resource-scarcity-item"
          data-level={scarcityLevel(scarcity)}
          key={resource}
        >
          <span>{humanizeInspectorKey(resource)}</span>
          <strong>{humanizeInspectorKey(String(scarcity))}</strong>
        </div>
      ))}
    </div>
  );
}

function StructuredObject({ value }: { value: Record<string, unknown> }) {
  const entries = Object.entries(value).filter(([, item]) =>
    presentInspectorValue(item),
  );
  if (!entries.length) return null;
  return (
    <dl className="inspector-nested-fields">
      {entries.map(([key, item]) => (
        <div key={key}>
          <dt>{humanizeInspectorKey(key)}</dt>
          <dd>
            <InspectorStructuredValue value={item} fieldKey={key} />
          </dd>
        </div>
      ))}
    </dl>
  );
}

function StructuredArray({ value }: { value: unknown[] }) {
  if (!value.length) return null;
  const structured = value.some(
    (item) => plainObject(item) || Array.isArray(item),
  );
  if (!structured) {
    return (
      <div className="inspector-value-chips">
        {value.map((item, index) => (
          <span key={`${String(item)}:${String(index)}`}>
            {primitive(item)}
          </span>
        ))}
      </div>
    );
  }
  return (
    <div className="inspector-structured-list">
      {value.map((item, index) => (
        <div className="inspector-structured-card" key={index}>
          <InspectorStructuredValue value={item} />
        </div>
      ))}
    </div>
  );
}

function InspectorStructuredValue({
  value,
  fieldKey,
}: {
  value: unknown;
  fieldKey?: string;
}): ReactNode {
  if (fieldKey?.toLowerCase() === "resources" && plainObject(value)) {
    return <ResourceScarcity value={value} />;
  }
  if (Array.isArray(value)) return <StructuredArray value={value} />;
  if (plainObject(value)) return <StructuredObject value={value} />;
  return primitive(value);
}

export function InspectorStructuredFields({
  value,
}: {
  value: Record<string, unknown>;
}) {
  const entries = Object.entries(value).filter(([, item]) =>
    presentInspectorValue(item),
  );
  if (!entries.length) return null;
  return (
    <dl className="inspector-fields inspector-structured-fields">
      {entries.map(([key, item]) => {
        const structured = plainObject(item) || Array.isArray(item);
        return (
          <div
            key={key}
            className={`inspector-field inspector-structured-field ${
              structured ? "wide" : ""
            }`}
          >
            <dt>{humanizeInspectorKey(key)}</dt>
            <dd>
              <InspectorStructuredValue value={item} fieldKey={key} />
            </dd>
          </div>
        );
      })}
    </dl>
  );
}
