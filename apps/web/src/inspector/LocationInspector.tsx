import type { EntitySummary, LocationInspectorSummary } from "../protocol";
import { InspectorCollection } from "./InspectorCollection";
import { InspectorFields } from "./InspectorFields";
export function LocationInspector({
  summary,
  detail,
}: {
  summary: EntitySummary;
  detail?: LocationInspectorSummary;
}) {
  return (
    <>
      <InspectorFields
        fields={[
          {
            label: "Type",
            value: detail?.location_type ?? summary.entity_type,
          },
          { label: "System", value: detail?.system ?? summary.system },
          { label: "Parent", value: detail?.parent },
          { label: "Scanned", value: detail?.scanned },
          { label: "System scanned", value: detail?.system_scanned },
          { label: "System tags", value: detail?.system_tags },
        ]}
      />
      {detail ? (
        <InspectorFields
          fields={Object.entries(detail.survey).map(([label, value]) => ({
            label,
            value,
          }))}
        />
      ) : null}
      {detail ? (
        <InspectorFields
          fields={Object.entries(detail.environment).map(([label, value]) => ({
            label,
            value,
          }))}
        />
      ) : null}
      {detail ? (
        <section className="inspector-section">
          <h3>Contents</h3>
          <InspectorCollection collection={detail.contents} />
        </section>
      ) : null}
    </>
  );
}
