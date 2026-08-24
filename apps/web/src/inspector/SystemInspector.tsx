import type { EntitySummary, SystemInspectorSummary } from "../protocol";
import { InspectorCollection } from "./InspectorCollection";
import { InspectorFields } from "./InspectorFields";
export function SystemInspector({
  summary,
  detail,
}: {
  summary: EntitySummary;
  detail?: SystemInspectorSummary;
}) {
  return (
    <>
      <InspectorFields
        fields={[
          {
            label: "Spectral type",
            value: detail?.spectral_type ?? summary.entity_type,
          },
          { label: "Region", value: detail?.region },
          { label: "Entry point", value: detail?.entry_point },
          { label: "Explored", value: detail?.explored },
          { label: "Hub", value: detail?.has_hub },
          { label: "Ward", value: detail?.has_ward },
          { label: "Life", value: detail?.has_life },
        ]}
      />
      {detail ? (
        <section className="inspector-section">
          <h3>Contents</h3>
          <InspectorCollection collection={detail.children} />
        </section>
      ) : null}
    </>
  );
}
