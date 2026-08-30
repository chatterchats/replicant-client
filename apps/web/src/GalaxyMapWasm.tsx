import { useEffect, useMemo, useRef, useState } from "react";

import { mapGalaxyScene, type GalaxyLayers } from "./galaxyMapData";
import type { GalaxySceneSnapshot, GalaxyStar } from "./protocol";
import { recordWebEvent } from "./telemetry";

const CAMERA_KEY = "replicant.galaxy.camera";
type RendererModule =
  typeof import("./wasm/galaxy_renderer/galaxy_renderer.js");
let rendererModule: Promise<RendererModule> | undefined;

function loadRenderer() {
  return (rendererModule ??=
    import("./wasm/galaxy_renderer/galaxy_renderer.js").then(async (module) => {
      await module.default();
      return module;
    }));
}

interface Camera {
  theta: number;
  phi: number;
  distance: number;
  tx: number;
  ty: number;
  tz: number;
}

interface Renderer {
  free(): void;
  resize(width: number, height: number): void;
  render(): void;
  set_camera(
    theta: number,
    phi: number,
    distance: number,
    tx: number,
    ty: number,
    tz: number,
  ): void;
  camera_theta(): number;
  camera_phi(): number;
  camera_distance(): number;
  camera_target_x(): number;
  camera_target_y(): number;
  camera_target_z(): number;
  pointer_down(x: number, y: number, button: number, shift: boolean): boolean;
  pointer_move(x: number, y: number): boolean;
  pointer_up(x: number, y: number): string | undefined;
  wheel(delta: number): void;
  set_census_geometry(
    key: string,
    stars: string,
    links: string,
    signals: string,
  ): void;
  set_relay_geometry(value: string): void;
  set_highlight_geometry(value: string): void;
  set_travel_geometry(value: string, now: number): void;
  set_life_sphere_geometry(value: string): void;
  set_device_sphere_geometry(value: string): void;
  set_influence_geometry(value: string): void;
}

function savedCamera(): Camera {
  const fallback = {
    theta: 0.4,
    phi: (-17 * Math.PI) / 180,
    distance: 23.2,
    tx: 0,
    ty: 0,
    tz: 0,
  };
  try {
    const saved: unknown = JSON.parse(localStorage.getItem(CAMERA_KEY) ?? "{}");
    if (typeof saved !== "object" || saved === null) return fallback;
    const values = saved as Record<string, unknown>;
    return Object.fromEntries(
      Object.entries(fallback).map(([key, value]) => [
        key,
        typeof values[key] === "number" ? values[key] : value,
      ]),
    ) as unknown as Camera;
  } catch {
    return fallback;
  }
}

function persistCamera(renderer: Renderer) {
  try {
    localStorage.setItem(
      CAMERA_KEY,
      JSON.stringify({
        theta: renderer.camera_theta(),
        phi: renderer.camera_phi(),
        distance: renderer.camera_distance(),
        tx: renderer.camera_target_x(),
        ty: renderer.camera_target_y(),
        tz: renderer.camera_target_z(),
      }),
    );
  } catch {
    // Storage is optional; the active renderer still retains the camera.
  }
}

function canvasPoint(event: React.PointerEvent<HTMLCanvasElement>) {
  const bounds = event.currentTarget.getBoundingClientRect();
  return { x: event.clientX - bounds.left, y: event.clientY - bounds.top };
}

export function GalaxyMapWasm({
  scene,
  visibleStars,
  layers,
  centerSystem,
  onSelectStar,
  onContextStar,
  onSelectWorkflow,
}: {
  scene: GalaxySceneSnapshot;
  visibleStars: GalaxyStar[];
  layers: GalaxyLayers;
  centerSystem: string;
  onSelectStar: (system: string) => void;
  onContextStar: (system: string, x: number, y: number) => void;
  onSelectWorkflow: (workflowId: string) => void;
}) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const rendererRef = useRef<Renderer | undefined>(undefined);
  const lastCenteredSystem = useRef<string>("");
  const [rendererReady, setRendererReady] = useState(false);
  const [error, setError] = useState(false);
  const mappedGeometry = useMemo(() => {
    const started = performance.now();
    const geometry = mapGalaxyScene(scene, visibleStars, layers);
    return { geometry, mapMs: performance.now() - started };
  }, [layers, scene, visibleStars]);
  const geometry = mappedGeometry.geometry;

  useEffect(() => {
    const controller = new AbortController();
    let frame = 0;
    let observer: ResizeObserver | undefined;
    const started = performance.now();
    void loadRenderer()
      .then(({ GalaxyRenderer }) => {
        const canvas = canvasRef.current;
        if (!canvas) return;
        if (controller.signal.aborted) return;
        const renderer = new GalaxyRenderer(canvas) as Renderer;
        rendererRef.current = renderer;
        const camera = savedCamera();
        renderer.set_camera(
          camera.theta,
          camera.phi,
          camera.distance,
          camera.tx,
          camera.ty,
          camera.tz,
        );
        observer = new ResizeObserver(([entry]) => {
          if (entry)
            renderer.resize(entry.contentRect.width, entry.contentRect.height);
        });
        observer.observe(canvas);
        const render = () => {
          renderer.render();
          frame = requestAnimationFrame(render);
        };
        render();
        setRendererReady(true);
        recordWebEvent(
          "info",
          "frontend.galaxy_renderer_ready",
          "galaxy WASM/WebGL renderer initialized",
          { elapsed_ms: Math.round(performance.now() - started) },
        );
      })
      .catch((reason: unknown) => {
        if (!controller.signal.aborted) {
          recordWebEvent(
            "error",
            "frontend.galaxy_renderer_failed",
            "galaxy WASM/WebGL renderer failed to initialize",
            { error: String(reason).slice(0, 500) },
          );
          setError(true);
        }
      });
    return () => {
      controller.abort();
      observer?.disconnect();
      cancelAnimationFrame(frame);
      rendererRef.current?.free();
      rendererRef.current = undefined;
    };
  }, []);

  useEffect(() => {
    const renderer = rendererRef.current;
    if (!rendererReady || !renderer) return;
    const started = performance.now();
    renderer.set_census_geometry(
      String(scene.revision),
      JSON.stringify(geometry.stars),
      "[]",
      JSON.stringify(geometry.signals),
    );
    renderer.set_relay_geometry(JSON.stringify(geometry.relays));
    renderer.set_highlight_geometry(JSON.stringify(geometry.highlights));
    renderer.set_travel_geometry(JSON.stringify(geometry.travel), Date.now());
    renderer.set_life_sphere_geometry(JSON.stringify(geometry.life));
    renderer.set_device_sphere_geometry(JSON.stringify(geometry.devices));
    renderer.set_influence_geometry(JSON.stringify(geometry.influence));
    const elapsedMs = performance.now() - started;
    recordWebEvent(
      elapsedMs >= 250 ? "warn" : "info",
      "frontend.galaxy_geometry_applied",
      "galaxy renderer geometry updated",
      {
        elapsed_ms: Math.round(elapsedMs),
        map_ms: Math.round(mappedGeometry.mapMs),
        revision: scene.revision,
        visible_stars: visibleStars.length,
        relay_edges: geometry.relays.length,
        travel_segments: geometry.travel.length,
      },
    );
  }, [
    geometry,
    mappedGeometry.mapMs,
    rendererReady,
    scene.revision,
    visibleStars.length,
  ]);

  useEffect(() => {
    const renderer = rendererRef.current;
    if (!centerSystem) {
      lastCenteredSystem.current = "";
      return;
    }
    if (!rendererReady || !renderer) return;
    if (lastCenteredSystem.current === centerSystem) return;
    const star = scene.stars.find((item) => item.id === centerSystem);
    if (!star) return;
    renderer.set_camera(
      renderer.camera_theta(),
      renderer.camera_phi(),
      renderer.camera_distance(),
      star.position.x,
      star.position.y,
      star.position.z,
    );
    lastCenteredSystem.current = centerSystem;
    persistCamera(renderer);
  }, [centerSystem, rendererReady, scene.stars]);

  return (
    <div className="galaxy-canvas-shell">
      {error ? (
        <p className="galaxy-map-error" role="alert">
          WebGL galaxy renderer unavailable.
        </p>
      ) : null}
      <canvas
        ref={canvasRef}
        aria-label="Interactive galaxy map"
        onContextMenu={(event) => {
          event.preventDefault();
        }}
        onPointerDown={(event) => {
          const point = canvasPoint(event);
          event.currentTarget.setPointerCapture(event.pointerId);
          rendererRef.current?.pointer_down(
            point.x,
            point.y,
            event.button,
            event.shiftKey,
          );
        }}
        onPointerMove={(event) => {
          const point = canvasPoint(event);
          rendererRef.current?.pointer_move(point.x, point.y);
        }}
        onPointerUp={(event) => {
          const renderer = rendererRef.current;
          if (!renderer) return;
          const point = canvasPoint(event);
          const picked = renderer.pointer_up(point.x, point.y);
          persistCamera(renderer);
          if (!picked) return;
          if (event.button === 2)
            onContextStar(picked, event.clientX, event.clientY);
          else onSelectStar(picked);
        }}
        onWheel={(event) => {
          event.preventDefault();
          const renderer = rendererRef.current;
          if (!renderer) return;
          renderer.wheel(event.deltaY);
          persistCamera(renderer);
        }}
      />
      {layers.highlights && scene.workflow_targets.length ? (
        <aside
          className="galaxy-workflow-overlays"
          aria-label="Active workflow overlays"
        >
          {[
            ...new Map(
              scene.workflow_targets.map((target) => [
                target.workflow_id,
                target,
              ]),
            ).values(),
          ].map((target) => (
            <button
              key={target.workflow_id}
              onClick={() => {
                onSelectWorkflow(target.workflow_id);
              }}
            >
              <small>{target.workflow_kind}</small>
              <strong>{target.system}</strong>
            </button>
          ))}
        </aside>
      ) : null}
    </div>
  );
}
