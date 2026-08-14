// Adapted with permission from the approved replicant.react SystemPage and overlays.
import {
  useEffect,
  useRef,
  useState,
  type PointerEvent as ReactPointerEvent,
} from "react";

import { daemonApi } from "./api";
import { type DescriptorCommand } from "./CommandPalette";
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
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
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
                setZoom((value) => Math.max(0.45, value - 0.2));
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
                setZoom(1);
                setPan({ x: 0, y: 0 });
              }}
            >
              Reset
            </button>
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
                  Math.max(0.45, value * (event.deltaY > 0 ? 0.9 : 1.1)),
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
            ].map((kind) => (
              <span key={kind}>{kind}</span>
            ))}
          </div>
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
