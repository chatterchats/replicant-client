// Adapted with permission from replicant.react/src/features/system/SystemMapGl.tsx.
import { useEffect, useMemo, useRef, useState } from "react";

import type { EntityRef, SystemMarker, SystemSceneSnapshot } from "./protocol";
import { mapSystemScene } from "./systemMapData";

const vertexShader = `
  attribute vec2 a_position;
  attribute vec4 a_color;
  uniform vec2 u_viewport;
  uniform vec2 u_pan;
  uniform float u_zoom;
  varying vec4 v_color;
  void main() {
    vec2 pixel = (a_position - vec2(500.0)) * u_zoom + vec2(500.0) + u_pan;
    vec2 clip = pixel / u_viewport * 2.0 - 1.0;
    gl_Position = vec4(clip.x, -clip.y, 0.0, 1.0);
    gl_PointSize = 8.0;
    v_color = a_color;
  }
`;

const fragmentShader = `
  precision mediump float;
  varying vec4 v_color;
  void main() { gl_FragColor = v_color; }
`;

const colors = {
  orbit: [0.28, 0.48, 0.58, 0.4],
  travel: [0.32, 0.9, 0.96, 0.9],
  workflow: [0.78, 0.55, 1, 0.9],
} as const;

function shader(gl: WebGLRenderingContext, type: number, source: string) {
  const value = gl.createShader(type);
  if (!value) throw new Error("Unable to create system-map shader");
  gl.shaderSource(value, source);
  gl.compileShader(value);
  if (!gl.getShaderParameter(value, gl.COMPILE_STATUS))
    throw new Error(gl.getShaderInfoLog(value) ?? "System-map shader failed");
  return value;
}

function program(gl: WebGLRenderingContext) {
  const value = gl.createProgram();
  gl.attachShader(value, shader(gl, gl.VERTEX_SHADER, vertexShader));
  gl.attachShader(value, shader(gl, gl.FRAGMENT_SHADER, fragmentShader));
  gl.linkProgram(value);
  if (!gl.getProgramParameter(value, gl.LINK_STATUS))
    throw new Error(gl.getProgramInfoLog(value) ?? "System-map link failed");
  return value;
}

function vertices(scene: SystemSceneSnapshot) {
  const output: number[] = [];
  for (const line of mapSystemScene(scene)) {
    const color = colors[line.kind];
    output.push(
      line.from.x,
      line.from.y,
      ...color,
      line.to.x,
      line.to.y,
      ...color,
    );
  }
  return new Float32Array(output);
}

export function SystemMapGl({
  scene,
  zoom,
  pan,
  showHabitableZone,
  showAssets,
  showLabels,
  onSelect,
  onContext,
  onSelectEntity,
}: {
  scene: SystemSceneSnapshot;
  zoom: number;
  pan: { x: number; y: number };
  showHabitableZone: boolean;
  showAssets: boolean;
  showLabels: boolean;
  onSelect: (marker: SystemMarker) => void;
  onContext: (marker: SystemMarker, x: number, y: number) => void;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const canvas = useRef<HTMLCanvasElement>(null);
  const [supported, setSupported] = useState(true);
  const geometry = useMemo(() => vertices(scene), [scene]);
  const positions = useMemo(
    () => new Map(scene.markers.map((marker) => [marker.id, marker.position])),
    [scene.markers],
  );

  useEffect(() => {
    const element = canvas.current;
    const gl = element?.getContext("webgl", { alpha: true });
    if (!element || !gl) {
      setSupported(false);
      return;
    }
    let renderer: WebGLProgram;
    try {
      renderer = program(gl);
    } catch {
      setSupported(false);
      return;
    }
    const buffer = gl.createBuffer();
    const position = gl.getAttribLocation(renderer, "a_position");
    const color = gl.getAttribLocation(renderer, "a_color");
    const viewport = gl.getUniformLocation(renderer, "u_viewport");
    const panUniform = gl.getUniformLocation(renderer, "u_pan");
    const zoomUniform = gl.getUniformLocation(renderer, "u_zoom");
    const draw = () => {
      const rect = element.getBoundingClientRect();
      const ratio = Math.max(1, window.devicePixelRatio || 1);
      element.width = Math.max(1, Math.round(rect.width * ratio));
      element.height = Math.max(1, Math.round(rect.height * ratio));
      gl.viewport(0, 0, element.width, element.height);
      gl.clearColor(0, 0, 0, 0);
      gl.clear(gl.COLOR_BUFFER_BIT);
      gl.useProgram(renderer);
      gl.bindBuffer(gl.ARRAY_BUFFER, buffer);
      gl.bufferData(gl.ARRAY_BUFFER, geometry, gl.STATIC_DRAW);
      gl.enableVertexAttribArray(position);
      gl.vertexAttribPointer(position, 2, gl.FLOAT, false, 24, 0);
      gl.enableVertexAttribArray(color);
      gl.vertexAttribPointer(color, 4, gl.FLOAT, false, 24, 8);
      gl.uniform2f(viewport, 1000, 1000);
      gl.uniform2f(panUniform, pan.x, pan.y);
      gl.uniform1f(zoomUniform, zoom);
      gl.drawArrays(gl.LINES, 0, geometry.length / 6);
    };
    draw();
    const observer = new ResizeObserver(draw);
    observer.observe(element);
    return () => {
      observer.disconnect();
      gl.deleteBuffer(buffer);
      gl.deleteProgram(renderer);
    };
  }, [geometry, pan.x, pan.y, zoom]);

  return (
    <div className="system-map-content">
      <canvas ref={canvas} className="system-map-webgl" aria-hidden="true" />
      {scene.markers
        .filter(
          (marker) =>
            showAssets ||
            !["vessel", "device", "factory", "relay"].includes(marker.kind),
        )
        .map((marker) => (
          <button
            className={`system-marker ${marker.kind}${showHabitableZone && marker.in_habitable_zone ? " habitable" : ""}`}
            key={marker.id}
            style={{
              left: `${String((marker.position.x - 500) * zoom + 500 + pan.x)}px`,
              top: `${String((marker.position.y - 500) * zoom + 500 + pan.y)}px`,
            }}
            title={`${marker.label} · ${marker.kind}`}
            onClick={(event) => {
              event.stopPropagation();
              onSelect(marker);
            }}
            onContextMenu={(event) => {
              event.preventDefault();
              event.stopPropagation();
              onContext(marker, event.clientX, event.clientY);
            }}
          >
            <span aria-hidden="true" />
            {showLabels ? <small>{marker.label}</small> : null}
            {marker.count > 1 ? <b>{marker.count}</b> : null}
          </button>
        ))}
      {scene.active_travel.map((travel) => {
        const from = positions.get(travel.from);
        const to = positions.get(travel.to);
        if (!from || !to) return null;
        return (
          <button
            className="system-activity-marker travel"
            key={`travel:${travel.entity.kind}:${travel.entity.id}`}
            style={{
              left: `${String(((from.x + to.x) / 2 - 500) * zoom + 500 + pan.x)}px`,
              top: `${String(((from.y + to.y) / 2 - 500) * zoom + 500 + pan.y)}px`,
            }}
            title={`${travel.entity.id} traveling`}
            onClick={() => {
              onSelectEntity(travel.entity);
            }}
          >
            ↗
          </button>
        );
      })}
      {scene.workflow_markers.map((workflow) => {
        const point =
          positions.get(workflow.location) ?? positions.get(scene.system);
        if (!point) return null;
        return (
          <button
            className="system-activity-marker workflow"
            key={`workflow:${workflow.workflow_id}`}
            style={{
              left: `${String((point.x - 500) * zoom + 522 + pan.x)}px`,
              top: `${String((point.y - 500) * zoom + 478 + pan.y)}px`,
            }}
            title={workflow.workflow_kind}
            onClick={() => {
              onSelectEntity({ kind: "workflow", id: workflow.workflow_id });
            }}
          >
            ◇
          </button>
        );
      })}
      {!supported ? (
        <p className="system-map-fallback">WebGL is unavailable.</p>
      ) : null}
    </div>
  );
}
