import type { CSSProperties, ReactNode } from "react";

import type { EntityProvenance, EntitySummary } from "../protocol";

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
  style,
  resizeHandle,
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
  style?: CSSProperties;
  resizeHandle?: ReactNode;
}) {
  return (
    <aside
      className="inspector"
      aria-label="Selected entity inspector"
      style={style}
    >
      {resizeHandle}
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
