import type { EntitySummary, LocationInspectorSummary } from "../protocol";
import { InspectorCollection } from "./InspectorCollection";
import {
  InspectorFields,
  presentInspectorValue,
  type InspectorField,
} from "./InspectorFields";
import { InspectorStructuredFields } from "./InspectorStructuredFields";

function hasObject(value: Record<string, unknown> | undefined) {
  return value !== undefined && Object.keys(value).length > 0;
}

function stringField(value: Record<string, unknown>, key: string) {
  return typeof value[key] === "string" ? value[key] : null;
}

function numberField(value: Record<string, unknown>, key: string) {
  return typeof value[key] === "number" ? value[key] : null;
}

function resourceEntries(value: unknown) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return [];
  return Object.entries(value as Record<string, unknown>).flatMap(
    ([resource, amount]) =>
      typeof amount === "number" ? [{ resource, amount }] : [],
  );
}

export function LocationInspector({
  summary,
  detail,
  onNavigate,
}: {
  summary: EntitySummary;
  detail?: LocationInspectorSummary;
  onNavigate?: (kind: string, id: string) => void;
}) {
  const surveyFields: InspectorField[] = detail
    ? [
        {
          label: "System survey complete",
          value: detail.survey.system_complete,
        },
        { label: "Planets total", value: detail.survey.planets_total },
        { label: "Planets scanned", value: detail.survey.planets_scanned },
        { label: "Moons total", value: detail.survey.moons_total },
        { label: "Moons scanned", value: detail.survey.moons_scanned },
        {
          label: "Moon total estimated",
          value: detail.survey.moons_total_estimated,
        },
      ]
    : [];
  const environmentFields: InspectorField[] = detail
    ? [
        { label: "Atmosphere", value: detail.environment.atmosphere },
        { label: "Magnetic field", value: detail.environment.magnetic_field },
        { label: "Gravity", value: detail.environment.gravity_g },
        {
          label: "Surface temperature",
          value: detail.environment.surface_temperature_c,
          render: (value) => `${String(value)} °C`,
        },
        {
          label: "Surface temperature",
          value:
            detail.environment.surface_temperature_c === null
              ? detail.environment.surface_temperature_k
              : null,
          render: (value) => `${String(value)} K`,
        },
        {
          label: "Atmospheric pressure",
          value: detail.environment.atmospheric_pressure_atm,
          render: (value) => `${String(value)} atm`,
        },
        {
          label: "Oxygen",
          value: detail.environment.oxygen_percent,
          render: (value) => `${String(value)}%`,
        },
        {
          label: "Atmospheric toxicity",
          value: detail.environment.atmospheric_toxicity,
        },
        {
          label: "Hydrosphere",
          value: detail.environment.hydrosphere_percent,
          render: (value) => `${String(value)}%`,
        },
        { label: "Tectonic index", value: detail.environment.tectonic_index },
        { label: "Biosphere index", value: detail.environment.biosphere_index },
        {
          label: "Subsurface ocean",
          value: detail.environment.subsurface_ocean,
        },
        { label: "Habitable zone", value: detail.environment.habitable_zone },
        { label: "Life stage", value: detail.environment.life_stage },
        {
          label: "Axial tilt",
          value: detail.environment.axial_tilt_degrees,
          render: (value) => `${String(value)}°`,
        },
        { label: "Rotation", value: detail.environment.rotation_state },
        {
          label: "Star spectral type",
          value: detail.environment.star_spectral_type,
        },
        {
          label: "Nearby belt richness",
          value: detail.environment.nearby_belt_richness,
        },
        {
          label: "Distance from Sol",
          value: detail.environment.distance_from_sol_light_years,
          render: (value) => `${String(value)} LY`,
        },
      ]
    : [];
  const specialized = detail
    ? ([
        ["Physical & orbit", detail.physical],
        ["Asteroid belt", detail.belt],
        ["Lagrange", detail.lagrange],
        ["Outer system", detail.outer_system],
        ["Incoming object", detail.incoming_object],
        ["Megastructure", detail.megastructure],
      ].filter(([, value]) => hasObject(value as Record<string, unknown>)) as [
        string,
        Record<string, unknown>,
      ][])
    : [];

  return (
    <>
      <InspectorFields
        fields={[
          {
            label: "Type",
            value: detail?.location_type ?? summary.entity_type,
          },
          { label: "Custom name", value: detail?.custom_name },
          { label: "System", value: detail?.system ?? summary.system },
          { label: "Parent", value: detail?.parent },
          { label: "Scanned", value: detail?.scanned },
          { label: "System scanned", value: detail?.system_scanned },
          { label: "System tags", value: detail?.system_tags },
        ]}
      />
      {detail?.system || detail?.parent ? (
        <section className="inspector-section">
          <h3>Relations</h3>
          <ul className="inspector-entity-list">
            {detail.system ? (
              <li>
                <button
                  type="button"
                  disabled={!onNavigate}
                  onClick={() => {
                    if (detail.system) onNavigate?.("system", detail.system);
                  }}
                >
                  <strong>System</strong>
                  <small>{detail.system}</small>
                </button>
              </li>
            ) : null}
            {detail.parent ? (
              <li>
                <button
                  type="button"
                  disabled={!onNavigate}
                  onClick={() => {
                    if (detail.parent) onNavigate?.("location", detail.parent);
                  }}
                >
                  <strong>Parent location</strong>
                  <small>{detail.parent}</small>
                </button>
              </li>
            ) : null}
          </ul>
        </section>
      ) : null}
      {surveyFields.some((field) => presentInspectorValue(field.value)) ? (
        <section className="inspector-section">
          <h3>Survey</h3>
          <InspectorFields fields={surveyFields} />
        </section>
      ) : null}
      {environmentFields.some((field) => presentInspectorValue(field.value)) ? (
        <section className="inspector-section">
          <h3>Environment</h3>
          <InspectorFields fields={environmentFields} />
        </section>
      ) : null}
      {specialized.map(([title, value]) => (
        <section
          className="inspector-section inspector-data-section"
          key={title}
        >
          <h3>{title}</h3>
          <InspectorStructuredFields value={value} />
        </section>
      ))}
      {detail?.resource_sites.length ? (
        <section className="inspector-section">
          <h3>Resource sites</h3>
          <div className="inspector-site-list">
            {detail.resource_sites.map((site, index) => {
              const designation =
                stringField(site, "designation") ?? `Site ${String(index + 1)}`;
              const name = stringField(site, "name");
              const resources = resourceEntries(site.resources_remaining_pct);
              return (
                <article className="inspector-context-card" key={designation}>
                  <strong>{name ?? designation}</strong>
                  {name ? <small>{designation}</small> : null}
                  {resources.length ? (
                    <ul className="inspector-resource-list compact">
                      {resources.map(({ resource, amount }) => (
                        <li key={resource}>
                          <button
                            type="button"
                            className="inspector-resource-link"
                            disabled={!onNavigate}
                            onClick={() => onNavigate?.("resource", resource)}
                          >
                            <span>{resource}</span>
                            <strong>{amount.toFixed(1)}%</strong>
                          </button>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <InspectorStructuredFields value={site} />
                  )}
                </article>
              );
            })}
          </div>
        </section>
      ) : null}
      {detail?.inventory.length ? (
        <section className="inspector-section">
          <h3>Inventory</h3>
          <ul className="inspector-resource-list">
            {detail.inventory.map((item, index) => {
              const resource =
                stringField(item, "resource") ??
                stringField(item, "resource_type") ??
                stringField(item, "name");
              const quantity =
                numberField(item, "quantity") ?? numberField(item, "amount");
              return resource ? (
                <li key={`${resource}:${String(index)}`}>
                  <button
                    type="button"
                    className="inspector-resource-link"
                    disabled={!onNavigate}
                    onClick={() => onNavigate?.("resource", resource)}
                  >
                    <span>{resource}</span>
                    <strong>{quantity?.toLocaleString() ?? "present"}</strong>
                  </button>
                </li>
              ) : (
                <li key={index}>
                  <InspectorStructuredFields value={item} />
                </li>
              );
            })}
          </ul>
        </section>
      ) : null}
      {detail && hasObject(detail.advanced) ? (
        <details className="inspector-section">
          <summary>Advanced</summary>
          <InspectorStructuredFields value={detail.advanced} />
        </details>
      ) : null}
      {detail?.contents.total ? (
        <section className="inspector-section">
          <h3>Contents</h3>
          <InspectorCollection
            collection={detail.contents}
            onNavigate={onNavigate}
          />
        </section>
      ) : null}
    </>
  );
}
