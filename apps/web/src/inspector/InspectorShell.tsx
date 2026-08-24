import type { ReactNode } from "react";

export function InspectorShell({
  kind,
  label,
  children,
  actions,
  onClose,
  onClear,
}: {
  kind: string;
  label: string;
  children: ReactNode;
  actions?: ReactNode;
  onClose: () => void;
  onClear: () => void;
}) {
  return (
    <aside className="inspector" aria-label="Selected entity inspector">
      <header className="drawer-header">
        <div>
          <small>{kind}</small>
          <strong>{label}</strong>
        </div>
        <button aria-label="Close inspector" onClick={onClose}>
          ×
        </button>
      </header>
      <div className="inspector-body">{children}</div>
      {actions ? <div className="inspector-actions">{actions}</div> : null}
      <button className="clear-selection" onClick={onClear}>
        Clear selection
      </button>
    </aside>
  );
}
