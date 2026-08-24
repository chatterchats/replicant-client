import { InspectorFields } from "./InspectorFields";
import { asRecord } from "./inspectorModel";
export function GenericInspector({ value }: { value: unknown }) {
  const item = asRecord(value);
  return item ? (
    <InspectorFields
      fields={Object.entries(item).map(([label, value]) => ({ label, value }))}
    />
  ) : (
    <p>
      {value === undefined
        ? "This entity is not present in the current daemon projection."
        : String(value)}
    </p>
  );
}
