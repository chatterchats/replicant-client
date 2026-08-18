// Adapted with permission from the approved replicant.react SystemPage and overlays.
import {
  useEffect,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";

import { daemonApi } from "./api";
import { descriptorCommands, type DescriptorCommand } from "./CommandPalette";
import { useDomainQuery } from "./domainQuery";
import { useGalaxyRevision } from "./daemon";
import type {
  DescriptorCatalog,
  EntityRef,
  SystemMarker,
  SystemSceneSnapshot,
} from "./protocol";
import { SystemMapGl } from "./SystemMapGl";
import { markerActions } from "./systemMapData";

export function SystemPage({
  system,
  descriptors,
  onSelectMarker,
  onRunCommand,
  onOpenGalaxy,
  onSelectEntity,
}: {
  system: string | undefined;
  descriptors: DescriptorCatalog;
  onSelectMarker: (marker: SystemMarker) => void;
  onRunCommand: (command: DescriptorCommand) => void;
  onOpenGalaxy: () => void;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const revision = useGalaxyRevision();
  const [scene, setScene] = useState<SystemSceneSnapshot>();
  const [error, setError] = useState<string>();
  const [zoom, setZoom] = useState(0.55);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [showHabitableZone, setShowHabitableZone] = useState(true);
  const [showAssets, setShowAssets] = useState(true);
  const [showLabels, setShowLabels] = useState(false);
  const [menu, setMenu] = useState<{
    marker: SystemMarker;
    x: number;
    y: number;
  }>();
  const drag = useRef<
    | {
        pointer: number;
        x: number;
        y: number;
        pan: { x: number; y: number };
      }
    | undefined
  >(undefined);

  useEffect(() => {
    if (!system) {
      setScene(undefined);
      return;
    }
    const controller = new AbortController();
    void daemonApi
      .systemScene(system, controller.signal)
      .then((value) => {
        setScene(value);
        setError(undefined);
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted)
          setError(
            reason instanceof Error ? reason.message : "System unavailable",
          );
      });
    return () => {
      controller.abort();
    };
  }, [revision, system]);

  const pointerDown = (event: ReactPointerEvent<HTMLDivElement>) => {
    if (event.button !== 0 || (event.target as Element).closest("button"))
      return;
    event.currentTarget.setPointerCapture(event.pointerId);
    drag.current = {
      pointer: event.pointerId,
      x: event.clientX,
      y: event.clientY,
      pan,
    };
    setMenu(undefined);
  };

  return (
    <article className="page system-page">
      <p className="eyebrow">Operations</p>
      <header className="system-page-header">
        <div>
          <h1>{system ?? "System"}</h1>
          <p className="lede">
            Managed locations, assets, travel, and workflows in one scene.
          </p>
        </div>
        <button onClick={onOpenGalaxy}>Back to Galaxy</button>
      </header>
      {!system ? (
        <p className="empty-state">Select a system in Galaxy first.</p>
      ) : error ? (
        <p className="error-state">{error}</p>
      ) : !scene ? (
        <p className="empty-state">Building system scene…</p>
      ) : (
        <>
          <div className="system-map-toolbar" aria-label="System map controls">
            <button
              onClick={() => {
                setZoom((value) => Math.max(0.3, value - 0.2));
              }}
            >
              −
            </button>
            <span>{Math.round(zoom * 100)}%</span>
            <button
              onClick={() => {
                setZoom((value) => Math.min(2.5, value + 0.2));
              }}
            >
              +
            </button>
            <button
              onClick={() => {
                setZoom(0.55);
                setPan({ x: 0, y: 0 });
              }}
            >
              Reset
            </button>
            <details className="galaxy-layers system-map-layers">
              <summary>Layers</summary>
              <label>
                <input
                  type="checkbox"
                  checked={showHabitableZone}
                  onChange={(event) => {
                    setShowHabitableZone(event.target.checked);
                  }}
                />
                Habitable planets
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={showAssets}
                  onChange={(event) => {
                    setShowAssets(event.target.checked);
                  }}
                />
                Assets
              </label>
              <label>
                <input
                  type="checkbox"
                  checked={showLabels}
                  onChange={(event) => {
                    setShowLabels(event.target.checked);
                  }}
                />
                Labels
              </label>
            </details>
            <span className="system-map-counts">
              {scene.markers.length} markers · {scene.active_travel.length}{" "}
              traveling · {scene.workflow_markers.length} workflows
            </span>
          </div>
          <div
            className="system-map-stage"
            onPointerDown={pointerDown}
            onPointerMove={(event) => {
              const active = drag.current;
              if (!active || active.pointer !== event.pointerId) return;
              setPan({
                x: active.pan.x + event.clientX - active.x,
                y: active.pan.y + event.clientY - active.y,
              });
            }}
            onPointerUp={(event) => {
              if (drag.current?.pointer === event.pointerId)
                drag.current = undefined;
            }}
            onWheel={(event) => {
              event.preventDefault();
              setZoom((value) =>
                Math.min(
                  2.5,
                  Math.max(0.3, value * (event.deltaY > 0 ? 0.9 : 1.1)),
                ),
              );
            }}
            onClick={() => {
              setMenu(undefined);
            }}
          >
            <SystemMapGl
              scene={scene}
              zoom={zoom}
              pan={pan}
              showHabitableZone={showHabitableZone}
              showAssets={showAssets}
              showLabels={showLabels}
              onSelect={onSelectMarker}
              onContext={(marker, x, y) => {
                onSelectMarker(marker);
                setMenu({ marker, x, y });
              }}
              onSelectEntity={onSelectEntity}
            />
          </div>
          <div className="system-map-legend" aria-label="System map legend">
            {[
              "planet",
              "moon",
              "belt",
              "lagrange",
              "vessel",
              "device",
              "factory",
              "relay",
              "event",
              "resource site",
              "megastructure",
            ].map((kind) => (
              <span key={kind}>{kind}</span>
            ))}
          </div>
          <ClaimsPanel
            system={system}
            descriptors={descriptors}
            onSelectEntity={onSelectEntity}
            onRunCommand={onRunCommand}
          />
          {menu ? (
            <menu
              className="galaxy-context-menu system-context-menu"
              aria-label={`${menu.marker.label} operations`}
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
              {markerActions(descriptors, menu.marker).map((command) => (
                <li
                  key={`${command.operationClass}:${command.descriptor.kind}`}
                >
                  <button
                    onClick={() => {
                      setMenu(undefined);
                      onRunCommand(command);
                    }}
                  >
                    <small>{command.operationClass}</small>
                    {command.descriptor.display_name}
                  </button>
                </li>
              ))}
            </menu>
          ) : null}
        </>
      )}
    </article>
  );
}

function ClaimsPanel({
  system,
  descriptors,
  onSelectEntity,
  onRunCommand,
}: {
  system: string;
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const { data, error } = useDomainQuery({
    slice: "devices",
    fetcher: (signal) => daemonApi.devices(signal),
    isEmpty: (snapshot) =>
      !snapshot.devices.some(
        (device) =>
          device.system === system &&
          (device.device_type === "system_hub" ||
            device.device_type?.includes("ward")),
      ),
  });
  const claims = (data?.devices ?? []).filter(
    (device) =>
      device.system === system &&
      (device.device_type === "system_hub" ||
        device.device_type?.includes("ward")),
  );
  if (!claims.length && !error) return null;

  const commands = descriptorCommands(descriptors);
  const command = (
    kind: string,
    device: string,
    parameters: Record<string, unknown> = {},
  ) => {
    const found = commands.find((item) => item.descriptor.kind === kind);
    return found
      ? { ...found, initialParameters: { device, ...parameters } }
      : undefined;
  };

  return (
    <section className="connection-card" aria-label="System claims">
      <h2>Claims & wards</h2>
      <p>
        Owned system hubs, naming rights, entry controls, and wards for {system}
        .
      </p>
      {error && <p className="inline-warning">{error}</p>}
      {claims.map((device) => {
        const isHub = device.device_type === "system_hub";
        const isWard = device.device_type?.includes("ward") === true;
        const entry = command("hub.set_entry_point", device.entity.id);
        const welcome = command("hub.set_welcome_message", device.entity.id);
        const rename = command("hub.rename", device.entity.id);
        const activate = command("device.lifecycle", device.entity.id, {
          command: "activate",
        });
        const deactivate = command("device.lifecycle", device.entity.id, {
          command: "deactivate",
        });
        const deploy = command("device.lifecycle", device.entity.id, {
          command: "deploy",
        });
        const lifecycle = command("device.lifecycle", device.entity.id);
        return (
          <div className="asset-operations" key={device.entity.id}>
            <button
              onClick={() => {
                onSelectEntity(device.entity);
              }}
            >
              <strong>{device.device_type}</strong> · {device.entity.id} ·{" "}
              {device.status ?? "unknown"}
            </button>
            {isHub && rename && (
              <button
                onClick={() => {
                  onRunCommand(rename);
                }}
              >
                Naming rights
              </button>
            )}
            {isHub && entry && (
              <button
                onClick={() => {
                  onRunCommand(entry);
                }}
              >
                Set entry point
              </button>
            )}
            {isHub && welcome && (
              <button
                onClick={() => {
                  onRunCommand(welcome);
                }}
              >
                Welcome message
              </button>
            )}
            {isWard && deploy && (
              <button
                onClick={() => {
                  onRunCommand(deploy);
                }}
              >
                Deploy ward
              </button>
            )}
            {isWard && activate && (
              <button
                onClick={() => {
                  onRunCommand(activate);
                }}
              >
                Activate / evict miners
              </button>
            )}
            {isWard && deactivate && (
              <button
                onClick={() => {
                  onRunCommand(deactivate);
                }}
              >
                Deactivate ward
              </button>
            )}
            {lifecycle && (
              <button
                onClick={() => {
                  onRunCommand(lifecycle);
                }}
              >
                More controls
              </button>
            )}
          </div>
        );
      })}
    </section>
  );
}
