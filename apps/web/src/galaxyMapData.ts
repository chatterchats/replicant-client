import type { GalaxySceneSnapshot, GalaxyStar } from "./protocol";

export interface GalaxyFilters {
  search: string;
  exploration: "all" | GalaxyStar["exploration"];
}

export interface GalaxyLayers {
  relays: boolean;
  travel: boolean;
  signals: boolean;
  highlights: boolean;
  life: boolean;
  devices: boolean;
  influence: boolean;
}

export const defaultGalaxyLayers: GalaxyLayers = {
  relays: true,
  travel: true,
  signals: true,
  highlights: true,
  life: true,
  devices: true,
  influence: true,
};

export function filterGalaxyStars(
  stars: GalaxyStar[],
  filters: GalaxyFilters,
): GalaxyStar[] {
  const search = filters.search.trim().toLowerCase();
  return stars.filter(
    (star) =>
      (filters.exploration === "all" ||
        star.exploration === filters.exploration ||
        star.current) &&
      (!search ||
        star.current ||
        star.id.toLowerCase().includes(search) ||
        star.name?.toLowerCase().includes(search) ||
        star.spectral_type?.toLowerCase().includes(search)),
  );
}

function edge(
  from: string,
  to: string,
  stars: Map<string, GalaxyStar>,
  flags: Record<string, boolean | string | null> = {},
) {
  const left = stars.get(from)?.position;
  const right = stars.get(to)?.position;
  return left && right ? { from: left, to: right, ...flags } : null;
}

export function mapGalaxyScene(
  scene: GalaxySceneSnapshot,
  visibleStars: GalaxyStar[],
  layers: GalaxyLayers,
) {
  const stars = new Map(scene.stars.map((star) => [star.id, star]));
  const links = (
    source: { from: string; to: string }[],
    flags: Record<string, boolean | string | null>,
  ) =>
    source
      .map((item) => edge(item.from, item.to, stars, flags))
      .filter((item) => item !== null);
  const centers = (kind: "life" | "device" | "influence") =>
    scene.overlays
      .filter((item) => item.kind === kind)
      .map((item) => item.position);

  return {
    stars: visibleStars.map((star) => ({
      designation: star.id,
      color: "",
      spectral_type: star.spectral_type ?? "",
      current: star.current,
      exploration: star.exploration,
      is_hub: star.has_hub,
      is_relay: star.has_relay,
      is_megastructure: star.has_megastructure === true,
      dimmed: false,
      ...star.position,
    })),
    signals: layers.signals
      ? scene.signals.map((signal) => ({ key: signal.id, ...signal.position }))
      : [],
    relays: layers.relays ? links(scene.relay_edges, { relay: true }) : [],
    travel: layers.travel
      ? scene.active_travel
          .map((travel) =>
            edge(travel.from, travel.to, stars, {
              travel_route: true,
              travel_started_at: travel.started_at,
              travel_ends_at: travel.arrives_at,
            }),
          )
          .filter((item) => item !== null)
      : [],
    highlights: layers.highlights
      ? scene.highlights
          .map((item) =>
            edge(item.from, item.to, stars, {
              exploration_route: true,
              workflow_id: item.workflow_id,
            }),
          )
          .filter((item) => item !== null)
      : [],
    life: layers.life ? centers("life") : [],
    devices: layers.devices ? centers("device") : [],
    influence: layers.influence ? centers("influence") : [],
  };
}
