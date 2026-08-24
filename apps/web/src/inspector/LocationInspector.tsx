import type { EntitySummary, LocationInspectorSummary } from "../protocol";
import { InspectorCollection } from "./InspectorCollection";
import {
  InspectorFields,
  presentInspectorValue,
  type InspectorField,
} from "./InspectorFields";

export function LocationInspector({
  summary,
  detail,
}: {
  summary: EntitySummary;
  detail?: LocationInspectorSummary;
}) {
  const surveyFields: InspectorField[] = detail
    ? [
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
      {detail?.contents.total ? (
        <section className="inspector-section">
          <h3>Contents</h3>
          <InspectorCollection collection={detail.contents} />
        </section>
      ) : null}
    </>
  );
}
