import { useEffect, useMemo, useRef, useState } from "react";

import { daemonApi } from "./api";
import { useGalaxyRevision } from "./daemon";
import {
  defaultGalaxyLayers,
  filterGalaxyStars,
  type GalaxyFilters,
  type GalaxyLayers,
} from "./galaxyMapData";
import { GalaxyMapWasm } from "./GalaxyMapWasm";
import {
  applicableDescriptorCommands,
  descriptorCommands,
  type DescriptorCommand,
} from "./CommandPalette";
import { useDomainQuery } from "./domainQuery";
import { recordWebEvent } from "./telemetry";
import type {
  DescriptorCatalog,
  GalaxySceneSnapshot,
  GalaxyStar,
} from "./protocol";

const SETTINGS_KEY = "replicant.galaxy.settings";
const explorations = ["all", "undiscovered", "partial", "explored"] as const;
type GalaxySettings = {
  filters: GalaxyFilters;
  layers: GalaxyLayers;
  anchor: string;
};

function record(value: unknown): Record<string, unknown> {
  return typeof value === "object" && value !== null
    ? (value as Record<string, unknown>)
    : {};
}

function loadSettings(): GalaxySettings {
  const defaults: GalaxySettings = {
    filters: { search: "", exploration: "all" },
    layers: defaultGalaxyLayers,
    anchor: "",
  };
  try {
    const saved = record(
      JSON.parse(localStorage.getItem(SETTINGS_KEY) ?? "{}") as unknown,
    );
    const filters = record(saved.filters);
    const savedLayers = record(saved.layers);
    const exploration = explorations.includes(
      filters.exploration as (typeof explorations)[number],
    )
      ? (filters.exploration as GalaxyFilters["exploration"])
      : "all";
    const layers = Object.fromEntries(
      Object.entries(defaultGalaxyLayers).map(([name, enabled]) => [
        name,
        typeof savedLayers[name] === "boolean" ? savedLayers[name] : enabled,
      ]),
    ) as GalaxyLayers;
    return {
      filters: {
        search: typeof filters.search === "string" ? filters.search : "",
        exploration,
      },
      layers,
      // Camera targeting is transient. Persisting a selected/jump anchor caused
      // old selections to keep snapping the map back after the inspector closed.
      anchor: "",
    };
  } catch {
    return defaults;
  }
}

export function GalaxyPage({
  onSelectStar,
  descriptors,
  onRunCommand,
  onSelectWorkflow,
  onOpenSystem,
}: {
  onSelectStar: (star: GalaxyStar) => void;
  descriptors: DescriptorCatalog;
  onRunCommand: (command: DescriptorCommand) => void;
  onSelectWorkflow: (workflowId: string) => void;
  onOpenSystem: (star: GalaxyStar) => void;
}) {
  const galaxyRevision = useGalaxyRevision();
  const deviceQuery = useDomainQuery({
    slice: "devices",
    queryKey: "devices",
    fetcher: (signal) => daemonApi.devices(signal),
    isEmpty: (snapshot) => snapshot.devices.length === 0,
  });
  const [scene, setScene] = useState<GalaxySceneSnapshot>();
  const [error, setError] = useState<string>();
  const [refreshing, setRefreshing] = useState(false);
  const [settings, setSettings] = useState(loadSettings);
  const [jumpRegion, setJumpRegion] = useState("");
  const [menu, setMenu] = useState<{
    x: number;
    y: number;
    star: GalaxyStar;
  }>();
  const latestRevision = useRef(galaxyRevision);
  const loading = useRef(false);
  const mounted = useRef(true);
  const sceneController = useRef<AbortController | undefined>(undefined);

  useEffect(() => {
    const controller = new AbortController();
    sceneController.current = controller;
    mounted.current = true;
    return () => {
      mounted.current = false;
      controller.abort();
      if (sceneController.current === controller)
        sceneController.current = undefined;
    };
  }, []);

  useEffect(() => {
    latestRevision.current = galaxyRevision;
    if (loading.current) return;
    const controller = sceneController.current;
    if (!controller) return;
    loading.current = true;
    void (async () => {
      try {
        let requestedRevision: number;
        do {
          requestedRevision = latestRevision.current;
          const started = performance.now();
          const next = await daemonApi.galaxyScene(controller.signal);
          if (controller.signal.aborted || !mounted.current) return;
          recordWebEvent(
            "info",
            "frontend.galaxy_scene_loaded",
            "galaxy scene projection loaded",
            {
              elapsed_ms: Math.round(performance.now() - started),
              revision: next.revision,
              stars: next.stars.length,
              relay_edges: next.relay_edges.length,
              active_travel: next.active_travel.length,
              signals: next.signals.length,
              workflow_targets: next.workflow_targets.length,
            },
          );
          setScene((current) =>
            current?.revision === next.revision ? current : next,
          );
          setError(undefined);
        } while (latestRevision.current !== requestedRevision);
      } catch (reason: unknown) {
        if (controller.signal.aborted || !mounted.current) return;
        recordWebEvent(
          "error",
          "frontend.galaxy_scene_failed",
          "galaxy scene projection failed",
          { error: String(reason).slice(0, 500) },
        );
        setError(String(reason));
      } finally {
        loading.current = false;
      }
    })();
  }, [galaxyRevision]);

  useEffect(() => {
    try {
      localStorage.setItem(
        SETTINGS_KEY,
        JSON.stringify({ ...settings, anchor: "" }),
      );
    } catch {
      // Storage is optional; React state still preserves this session.
    }
  }, [settings]);

  useEffect(() => {
    if (!menu) return;
    const close = () => {
      setMenu(undefined);
    };
    window.addEventListener("click", close);
    return () => {
      window.removeEventListener("click", close);
    };
  }, [menu]);

  const visibleStars = useMemo(
    () => filterGalaxyStars(scene?.stars ?? [], settings.filters),
    [scene?.stars, settings.filters],
  );
  const anchor =
    scene?.stars.some((star) => star.id === settings.anchor) === true
      ? settings.anchor
      : "";
  const regionAnchors = useMemo(() => {
    const byRegion = new Map<string, GalaxyStar[]>();
    for (const star of scene?.stars ?? []) {
      if (!star.region) continue;
      const stars = byRegion.get(star.region) ?? [];
      stars.push(star);
      byRegion.set(star.region, stars);
    }
    return [...byRegion.entries()]
      .map(([region, stars]) => {
        const centroid = stars.reduce(
          (point, star) => ({
            x: point.x + star.position.x / stars.length,
            y: point.y + star.position.y / stars.length,
            z: point.z + star.position.z / stars.length,
          }),
          { x: 0, y: 0, z: 0 },
        );
        const anchor = [...stars].sort((left, right) => {
          const distance = (star: GalaxyStar) =>
            (star.position.x - centroid.x) ** 2 +
            (star.position.y - centroid.y) ** 2 +
            (star.position.z - centroid.z) ** 2;
          return (
            distance(left) - distance(right) || left.id.localeCompare(right.id)
          );
        })[0];
        return anchor ? { region, system: anchor.id } : null;
      })
      .filter(
        (value): value is { region: string; system: string } => value !== null,
      )
      .sort((left, right) => left.region.localeCompare(right.region));
  }, [scene?.stars]);
  const operations = descriptorCommands(descriptors);
  const teleport = operations.find(
    (command) => command.descriptor.kind === "replicant.teleport",
  );
  const slingshot = operations.find(
    (command) => command.descriptor.kind === "replicant.slingshot",
  );
  const teleportTargets = menu
    ? (deviceQuery.data?.devices ?? []).filter(
        (device) =>
          device.system === menu.star.id &&
          device.device_type === "empty_replicant_matrix",
      )
    : [];
  const targetMatrices = new Set(
    teleportTargets.map((device) => device.entity.id),
  );
  const linkedSlingshots = menu
    ? (deviceQuery.data?.devices ?? []).filter(
        (device) =>
          device.device_type === "ftl_slingshot" &&
          device.linked_device !== null &&
          targetMatrices.has(device.linked_device),
      )
    : [];
  const refreshGalaxy = () => {
    setRefreshing(true);
    setError(undefined);
    void daemonApi
      .refreshGalaxy()
      .catch((reason: unknown) => {
        if (!mounted.current) return;
        setError(
          reason instanceof Error ? reason.message : "Galaxy refresh failed",
        );
      })
      .finally(() => {
        if (mounted.current) setRefreshing(false);
      });
  };

  return (
    <article className="galaxy-map">
      <header className="galaxy-toolbar">
        <div>
          <p className="eyebrow">Operations</p>
          <h1>Galaxy</h1>
          <small>
            {scene
              ? `${String(visibleStars.length)} of ${String(scene.stars.length)} systems`
              : "Loading scene…"}
          </small>
        </div>
        <label>
          System search
          <input
            list="known-galaxy-systems"
            value={settings.filters.search}
            placeholder="System or spectral type"
            onChange={(event) => {
              const search = event.target.value;
              setSettings((current) => ({
                ...current,
                filters: { ...current.filters, search },
                anchor:
                  scene?.stars.find(
                    (star) => star.id.toLowerCase() === search.toLowerCase(),
                  )?.id ?? current.anchor,
              }));
            }}
          />
          <datalist id="known-galaxy-systems">
            {scene?.stars.map((star) => (
              <option key={star.id} value={star.id} />
            ))}
          </datalist>
        </label>
        <label>
          Jump to region
          <select
            value={jumpRegion}
            onChange={(event) => {
              const region = event.target.value;
              setJumpRegion(region);
              const target = regionAnchors.find(
                (item) => item.region === region,
              );
              if (target) {
                setSettings((current) => ({
                  ...current,
                  anchor: target.system,
                }));
              }
            }}
          >
            <option value="">Choose region…</option>
            {regionAnchors.map((item) => (
              <option key={item.region} value={item.region}>
                {item.region}
              </option>
            ))}
          </select>
        </label>
        <label>
          Exploration
          <select
            value={settings.filters.exploration}
            onChange={(event) => {
              setSettings((current) => ({
                ...current,
                filters: {
                  ...current.filters,
                  exploration: event.target
                    .value as GalaxyFilters["exploration"],
                },
              }));
            }}
          >
            <option value="all">All</option>
            <option value="explored">Explored</option>
            <option value="partial">Partial</option>
            <option value="undiscovered">Undiscovered</option>
          </select>
        </label>
        <button disabled={refreshing} onClick={refreshGalaxy}>
          {refreshing ? "Refreshing galaxy…" : "Refresh galaxy data"}
        </button>
        <details className="galaxy-layers">
          <summary>Layers</summary>
          {(
            Object.entries(settings.layers) as [keyof GalaxyLayers, boolean][]
          ).map(([name, enabled]) => (
            <label key={name}>
              <input
                type="checkbox"
                checked={enabled}
                onChange={(event) => {
                  setSettings((current) => ({
                    ...current,
                    layers: {
                      ...current.layers,
                      [name]: event.target.checked,
                    },
                  }));
                }}
              />
              {name}
            </label>
          ))}
        </details>
        {error ? <p role="alert">{error}</p> : null}
      </header>
      {scene ? (
        <GalaxyMapWasm
          scene={scene}
          visibleStars={visibleStars}
          layers={settings.layers}
          centerSystem={anchor}
          onSelectStar={(system) => {
            setSettings((current) => ({ ...current, anchor: "" }));
            setJumpRegion("");
            const star = scene.stars.find((item) => item.id === system);
            if (star) onSelectStar(star);
          }}
          onContextStar={(system, x, y) => {
            const star = scene.stars.find((item) => item.id === system);
            if (!star) return;
            setSettings((current) => ({ ...current, anchor: "" }));
            setJumpRegion("");
            onSelectStar(star);
            setMenu({ x, y, star });
          }}
          onSelectWorkflow={onSelectWorkflow}
        />
      ) : null}
      {menu ? (
        <menu
          className="galaxy-context-menu"
          aria-label={`${menu.star.id} operations`}
          style={{ left: menu.x, top: menu.y }}
          onClick={(event) => {
            event.stopPropagation();
          }}
        >
          <li>
            <button
              onClick={() => {
                setMenu(undefined);
              }}
            >
              Inspect
            </button>
          </li>
          <li>
            <button
              onClick={() => {
                setMenu(undefined);
                onOpenSystem(menu.star);
              }}
            >
              Open system
            </button>
          </li>
          {teleportTargets.map((matrix) =>
            teleport ? (
              <li key={`teleport:${matrix.entity.id}`}>
                <button
                  onClick={() => {
                    setMenu(undefined);
                    onRunCommand({
                      ...teleport,
                      initialParameters: { target: matrix.entity.id },
                    });
                  }}
                >
                  <small>action</small>
                  Teleport here · {matrix.entity.id}
                </button>
              </li>
            ) : null,
          )}
          {linkedSlingshots.map((device) =>
            slingshot ? (
              <li key={`slingshot:${device.entity.id}`}>
                <button
                  onClick={() => {
                    setMenu(undefined);
                    onRunCommand({
                      ...slingshot,
                      initialParameters: { slingshot: device.entity.id },
                    });
                  }}
                >
                  <small>action</small>
                  Slingshot here · {device.entity.id}
                </button>
              </li>
            ) : null,
          )}
          {applicableDescriptorCommands(descriptors, "system").map(
            (command) => (
              <li key={`${command.operationClass}:${command.descriptor.kind}`}>
                <button
                  onClick={() => {
                    setMenu(undefined);
                    onRunCommand(
                      command.descriptor.kind === "replicant.travel"
                        ? {
                            ...command,
                            initialParameters: { destination: menu.star.id },
                          }
                        : command,
                    );
                  }}
                >
                  <small>{command.operationClass}</small>
                  {command.descriptor.display_name}
                </button>
              </li>
            ),
          )}
        </menu>
      ) : null}
    </article>
  );
}
