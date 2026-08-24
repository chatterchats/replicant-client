/* eslint-disable react-refresh/only-export-components */
import {
  useEffect,
  useState,
  type CSSProperties,
  type PointerEvent,
  type ReactNode,
} from "react";

import type { EntityProvenance, EntitySummary } from "../protocol";

const INSPECTOR_WIDTH_KEY = "replicant.inspector.width.v1";
const DEFAULT_INSPECTOR_WIDTH = 390;
const MIN_INSPECTOR_WIDTH = 320;
const INSPECTOR_WIDTH_STEP = 16;

function maximumInspectorWidth() {
  return Math.min(680, window.innerWidth * 0.55);
}

function clampInspectorWidth(value: number) {
  const maximum = Math.max(MIN_INSPECTOR_WIDTH, maximumInspectorWidth());
  return Math.min(maximum, Math.max(MIN_INSPECTOR_WIDTH, value));
}

function storedInspectorWidth() {
  const stored = Number(window.localStorage.getItem(INSPECTOR_WIDTH_KEY));
  return clampInspectorWidth(
    Number.isFinite(stored) && stored > 0 ? stored : DEFAULT_INSPECTOR_WIDTH,
  );
}

export function useInspectorWidth() {
  const [width, setWidthState] = useState(storedInspectorWidth);
  const [desktop, setDesktop] = useState(() => window.innerWidth > 720);
  const setWidth = (value: number) => {
    const next = clampInspectorWidth(value);
    setWidthState(next);
    try {
      window.localStorage.setItem(INSPECTOR_WIDTH_KEY, String(next));
    } catch {
      // Width persistence is a UI preference; storage failure is non-fatal.
    }
  };
  useEffect(() => {
    const resize = () => {
      setDesktop(window.innerWidth > 720);
      setWidthState((current) => clampInspectorWidth(current));
    };
    window.addEventListener("resize", resize);
    return () => {
      window.removeEventListener("resize", resize);
    };
  }, []);
  return {
    width,
    desktop,
    minimum: MIN_INSPECTOR_WIDTH,
    maximum: Math.max(MIN_INSPECTOR_WIDTH, maximumInspectorWidth()),
    setWidth,
  };
}

function Section({ title, children }: { title: string; children?: ReactNode }) {
  return children ? (
    <section className="inspector-section">
      <h3>{title}</h3>
      {children}
    </section>
  ) : null;
}

export function InspectorShell({
  summary,
  vitals,
  body,
  relations,
  contents,
  activity,
  provenance,
  actions,
  warning,
  onClose,
  onClear,
}: {
  summary: EntitySummary;
  vitals?: ReactNode;
  body?: ReactNode;
  relations?: ReactNode;
  contents?: ReactNode;
  activity?: ReactNode;
  provenance?: EntityProvenance | null;
  actions?: ReactNode;
  warning?: ReactNode;
  onClose: () => void;
  onClear: () => void;
}) {
  const size = useInspectorWidth();
  const [drag, setDrag] = useState<{ x: number; width: number } | null>(null);
  const startDrag = (event: PointerEvent<HTMLDivElement>) => {
    setDrag({ x: event.clientX, width: size.width });
    const capture = (event.currentTarget as Partial<HTMLElement>).setPointerCapture;
    if (typeof capture === "function") capture.call(event.currentTarget, event.pointerId);
  };
  const moveDrag = (event: PointerEvent<HTMLDivElement>) => {
    if (drag) size.setWidth(drag.width + drag.x - event.clientX);
  };
  const stopDrag = (event: PointerEvent<HTMLDivElement>) => {
    setDrag(null);
    const release = (event.currentTarget as Partial<HTMLElement>).releasePointerCapture;
    if (typeof release === "function") release.call(event.currentTarget, event.pointerId);
  };
  const style = {
    "--inspector-width": `${String(size.width)}px`,
  } as CSSProperties;
  return (
    <aside
      className="inspector"
      aria-label="Selected entity inspector"
      style={style}
    >
      {size.desktop ? (
        <div
          className={`inspector-resize-handle ${drag ? "dragging" : ""}`}
          role="separator"
          aria-label="Resize inspector"
          aria-orientation="vertical"
          aria-valuenow={Math.round(size.width)}
          aria-valuemin={size.minimum}
          aria-valuemax={Math.round(size.maximum)}
          tabIndex={0}
          onPointerDown={startDrag}
          onPointerMove={moveDrag}
          onPointerUp={stopDrag}
          onPointerCancel={stopDrag}
          onKeyDown={(event) => {
            if (event.key === "ArrowLeft")
              size.setWidth(size.width + INSPECTOR_WIDTH_STEP);
            else if (event.key === "ArrowRight")
              size.setWidth(size.width - INSPECTOR_WIDTH_STEP);
            else if (event.key === "Home") size.setWidth(size.minimum);
            else if (event.key === "End") size.setWidth(size.maximum);
            else return;
            event.preventDefault();
          }}
        />
      ) : null}
      <header className="drawer-header">
        <div>
          <small>{summary.entity.kind}</small>
          <strong>{summary.label}</strong>
          {summary.secondary_label ? (
            <span>{summary.secondary_label}</span>
          ) : null}
        </div>
        <button aria-label="Close inspector" onClick={onClose}>
          ×
        </button>
      </header>
      {vitals ? <div className="inspector-vitals">{vitals}</div> : null}
      <div className="inspector-body">
        {warning}
        <Section title="Details">{body}</Section>
        <Section title="Relations">{relations}</Section>
        <Section title="Contents">{contents}</Section>
        <Section title="Activity">{activity}</Section>
      </div>
      {provenance ? (
        <footer className="inspector-provenance">
          <time dateTime={new Date(provenance.observed_at_ms).toISOString()}>
            Observed {new Date(provenance.observed_at_ms).toLocaleString()}
          </time>
          {provenance.stale ? <span>Stale</span> : null}
          <span>{provenance.reachability}</span>
          <span>{provenance.source_operation}</span>
        </footer>
      ) : null}
      {actions ? <div className="inspector-actions">{actions}</div> : null}
      <button className="clear-selection" onClick={onClear}>
        Clear selection
      </button>
    </aside>
  );
}
