import type { ReactNode } from "react";

export interface InspectorField {
  label: string;
  value: unknown;
  render?: (value: unknown) => ReactNode;
}

export function presentInspectorValue(value: unknown): boolean {
  if (value === null || value === undefined || value === "") return false;
  if (Array.isArray(value)) return value.length > 0;
  if (
    typeof value === "string" &&
    ["unknown", "unobserved"].includes(value.toLowerCase())
  ) {
    return false;
  }
  return true;
}

export function formatInspectorValue(value: unknown): string {
  if (typeof value === "boolean") return value ? "Yes" : "No";
  if (typeof value === "number" || typeof value === "bigint")
    return value.toString();
  if (Array.isArray(value)) return value.join(", ");
  if (typeof value === "object" && value !== null) return JSON.stringify(value);
  return String(value);
}

export function InspectorFields({
  fields,
}: {
  fields: readonly InspectorField[];
}) {
  const visible = fields.filter((field) => presentInspectorValue(field.value));
  if (visible.length === 0) return null;
  return (
    <dl className="inspector-fields">
      {visible.map((field) => (
        <div key={field.label} className="inspector-field">
          <dt>{field.label}</dt>
          <dd>
            {field.render
              ? field.render(field.value)
              : formatInspectorValue(field.value)}
          </dd>
        </div>
      ))}
    </dl>
  );
}
