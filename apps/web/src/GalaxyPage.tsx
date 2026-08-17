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
      anchor: typeof saved.anchor === "string" ? saved.anchor : "",
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
    fetcher: daemonApi.devices,
    isEmpty: (snapshot) => snapshot.devices.length === 0,
  });
  const [scene, setScene] = useState<GalaxySceneSnapshot>();
  const [error, setError] = useState<string>();
  const [settings, setSettings] = useState(loadSettings);
  const [menu, setMenu] = useState<{
    x: number;
    y: number;
    star: GalaxyStar;
  }>();
  const latestRevision = useRef(galaxyRevision);
  const loading = useRef(false);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  useEffect(() => {
    latestRevision.current = galaxyRevision;
    if (loading.current) return;
    loading.current = true;
    void (async () => {
      try {
        let requestedRevision: number;
        do {
          requestedRevision = latestRevision.current;
          const next = await daemonApi.galaxyScene();
          if (!mounted.current) return;
          setScene((current) =>
            current?.revision === next.revision ? current : next,
          );
          setError(undefined);
        } while (latestRevision.current !== requestedRevision);
      } catch (reason: unknown) {
        if (mounted.current) setError(String(reason));
      } finally {
        loading.current = false;
      }
    })();
  }, [galaxyRevision]);

  useEffect(() => {
    try {
      localStorage.setItem(SETTINGS_KEY, JSON.stringify(settings));
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
      : (scene?.stars.find((star) => star.current)?.id ??
        scene?.stars[0]?.id ??
        "");
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
            setSettings((current) => ({ ...current, anchor: system }));
            const star = scene.stars.find((item) => item.id === system);
            if (star) onSelectStar(star);
          }}
          onContextStar={(system, x, y) => {
            const star = scene.stars.find((item) => item.id === system);
            if (!star) return;
            setSettings((current) => ({ ...current, anchor: system }));
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
