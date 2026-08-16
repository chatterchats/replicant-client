import { useEffect, useRef, useState } from "react";

export interface ConfirmRequest {
  title: string;
  /** What will happen, stated concretely from live state where possible. */
  message: string;
  /** Named items the action will affect, listed so the impact is visible. */
  items?: string[];
  confirmLabel: string;
  /** Requires typing the confirmation word; use for irreversible, wide-scope actions. */
  requireTyped?: string;
  destructive?: boolean;
  onConfirm: () => void;
}

/**
 * Modal confirmation for destructive actions.
 *
 * Replaces `window.confirm`, which blocks the event loop and can only show a
 * single line — so an account-wide cancel looked identical to cancelling one
 * workflow. This states the scope, lists what is affected, and can require the
 * confirmation to be typed.
 */
export function ConfirmDialog({
  request,
  onClose,
}: {
  request: ConfirmRequest | null;
  onClose: () => void;
}) {
  const [typed, setTyped] = useState("");
  const confirmRef = useRef<HTMLButtonElement>(null);
  const dialogRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    setTyped("");
  }, [request]);

  useEffect(() => {
    if (!request) return;
    confirmRef.current?.focus();
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        event.stopPropagation();
        onClose();
        return;
      }
      if (event.key !== "Tab") return;
      // Keep focus inside the dialog while it is open.
      const focusable = dialogRef.current?.querySelectorAll<HTMLElement>(
        "button, input, [href], [tabindex]:not([tabindex='-1'])",
      );
      if (!focusable || focusable.length === 0) return;
      const first = focusable[0];
      const last = focusable[focusable.length - 1];
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    };
    window.addEventListener("keydown", onKeyDown, true);
    return () => {
      window.removeEventListener("keydown", onKeyDown, true);
    };
  }, [request, onClose]);

  if (!request) return null;
  const satisfied =
    request.requireTyped === undefined ||
    typed.trim().toLowerCase() === request.requireTyped.toLowerCase();

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div
        aria-labelledby="confirm-title"
        aria-modal="true"
        className="confirm-dialog"
        onClick={(event) => {
          event.stopPropagation();
        }}
        ref={dialogRef}
        role="dialog"
      >
        <h2 id="confirm-title">{request.title}</h2>
        <p>{request.message}</p>
        {request.items && request.items.length > 0 && (
          <ul className="confirm-items">
            {request.items.map((item) => (
              <li key={item}>{item}</li>
            ))}
          </ul>
        )}
        {request.requireTyped !== undefined && (
          <label className="confirm-typed">
            Type <strong>{request.requireTyped}</strong> to continue
            <input
              autoComplete="off"
              onChange={(event) => {
                setTyped(event.target.value);
              }}
              value={typed}
            />
          </label>
        )}
        <div className="confirm-actions">
          <button onClick={onClose}>Keep running</button>
          <button
            className={request.destructive ? "danger" : ""}
            disabled={!satisfied}
            onClick={() => {
              request.onConfirm();
              onClose();
            }}
            ref={confirmRef}
          >
            {request.confirmLabel}
          </button>
        </div>
      </div>
    </div>
  );
}
