import type { EntitySummary, SystemInspectorSummary } from "../protocol";
import { InspectorCollection } from "./InspectorCollection";
import { InspectorFields } from "./InspectorFields";
import { InspectorStructuredFields } from "./InspectorStructuredFields";

export function SystemInspector({
  summary,
  detail,
  onNavigate,
}: {
  summary: EntitySummary;
  detail?: SystemInspectorSummary;
  onNavigate?: (kind: string, id: string) => void;
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
          {
            label: "Position",
            value: detail?.position
              ? `${detail.position.x.toFixed(2)}, ${detail.position.y.toFixed(2)}, ${detail.position.z.toFixed(2)} LY`
              : null,
          },
          { label: "Explored", value: detail?.explored },
          { label: "System Hub", value: detail?.has_hub },
          { label: "Ward", value: detail?.has_ward },
          { label: "Life", value: detail?.has_life },
          { label: "Tags", value: detail?.tags },
          {
            label: "Mining bonus",
            value: detail?.mining_bonus_percent,
            render: (value) => `${String(value)}%`,
          },
          { label: "Shops", value: detail?.shop_count },
          { label: "Active events", value: detail?.active_event_count },
          { label: "System objects", value: detail?.object_count },
        ]}
      />
      {detail && Object.keys(detail.stellar).length ? (
        <section className="inspector-section inspector-data-section">
          <h3>Star</h3>
          <InspectorStructuredFields value={detail.stellar} />
        </section>
      ) : null}
      {detail && Object.keys(detail.asteroid_belt).length ? (
        <section className="inspector-section inspector-data-section">
          <h3>Asteroid belt</h3>
          <InspectorStructuredFields value={detail.asteroid_belt} />
        </section>
      ) : null}
      {detail && Object.keys(detail.outer_system).length ? (
        <section className="inspector-section inspector-data-section">
          <h3>Outer system</h3>
          <InspectorStructuredFields value={detail.outer_system} />
        </section>
      ) : null}
      {detail?.entry_point ? (
        <section className="inspector-section">
          <h3>Relations</h3>
          <ul className="inspector-entity-list">
            <li>
              <button
                type="button"
                disabled={!onNavigate}
                onClick={() => {
                  if (detail.entry_point)
                    onNavigate?.("location", detail.entry_point);
                }}
              >
                <strong>Entry point</strong>
                <small>{detail.entry_point}</small>
              </button>
            </li>
          </ul>
        </section>
      ) : null}
      {detail?.children.total ? (
        <section className="inspector-section">
          <h3>Contents</h3>
          <InspectorCollection
            collection={detail.children}
            onNavigate={onNavigate}
          />
        </section>
      ) : null}
    </>
  );
}
