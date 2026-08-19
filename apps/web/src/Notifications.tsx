import { useEffect, useMemo, useRef, useState } from "react";

import type { Notification, NotificationLevel } from "./protocol";
import { relativeTime } from "./time";

/** How long a toast stays on screen before dismissing itself. */
const TOAST_TIMEOUT_MS = 8_000;
const TOAST_LIMIT = 4;

function levelLabel(level: NotificationLevel): string {
  return level === "error" ? "Error" : level === "warning" ? "Warning" : "Info";
}

/**
 * Surfaces newly arrived warnings and errors as transient toasts.
 *
 * Daemon notifications previously reached the UI only as a count in the status
 * bar, so a failed trigger or a degraded sync was easy to miss entirely.
 * Informational notices stay in the notification centre rather than
 * interrupting.
 */
export function NotificationToasts({
  notifications,
  ready,
  onSelect,
  onDismiss,
}: {
  notifications: Notification[];
  ready: boolean;
  onSelect: (notification: Notification) => void;
  onDismiss: (notification: Notification) => void;
}) {
  const seen = useRef<Set<string> | null>(null);
  const [toasts, setToasts] = useState<Notification[]>([]);

  useEffect(() => {
    // The shell mounts before the daemon snapshot arrives. Do not initialize
    // the seen set from that temporary empty state or the first snapshot will
    // be mistaken for a burst of brand-new notifications after every reload.
    if (!ready) return;
    if (seen.current === null) {
      seen.current = new Set(notifications.map((item) => item.id));
      return;
    }
    const fresh = notifications.filter(
      (item) => item.level !== "info" && !seen.current?.has(item.id),
    );
    for (const item of notifications) seen.current.add(item.id);
    if (fresh.length > 0)
      setToasts((current) => [...current, ...fresh].slice(-TOAST_LIMIT));
  }, [notifications, ready]);

  useEffect(() => {
    if (toasts.length === 0) return;
    const timer = setTimeout(() => {
      setToasts((current) => current.slice(1));
    }, TOAST_TIMEOUT_MS);
    return () => {
      clearTimeout(timer);
    };
  }, [toasts]);

  if (toasts.length === 0) return null;
  return (
    <div className="toast-stack" aria-live="polite" role="status">
      {toasts.map((toast) => (
        <article className={`toast ${toast.level}`} key={toast.id}>
          <div>
            <strong>{toast.title}</strong>
            <p>{toast.message}</p>
          </div>
          <div className="toast-actions">
            <button
              onClick={() => {
                onSelect(toast);
                setToasts((current) =>
                  current.filter((item) => item.id !== toast.id),
                );
              }}
            >
              View
            </button>
            <button
              aria-label={`Dismiss ${toast.title}`}
              className="toast-dismiss"
              onClick={() => {
                onDismiss(toast);
                setToasts((current) =>
                  current.filter((item) => item.id !== toast.id),
                );
              }}
            >
              ×
            </button>
          </div>
        </article>
      ))}
    </div>
  );
}

/** Full list of current notifications, newest first. */
export function NotificationCenter({
  notifications,
  onClose,
  onSelect,
  onDismiss,
  onClearAll,
}: {
  notifications: Notification[];
  onClose: () => void;
  onSelect: (notification: Notification) => void;
  onDismiss: (notification: Notification) => void;
  onClearAll: () => void;
}) {
  const ordered = useMemo(
    () => [...notifications].sort((a, b) => b.created_at_ms - a.created_at_ms),
    [notifications],
  );
  return (
    <div className="notification-center" aria-label="Notifications">
      <header>
        <h2>Notifications</h2>
        <div className="notification-center-actions">
          {ordered.length > 0 && (
            <button onClick={onClearAll}>Clear all</button>
          )}
          <button aria-label="Close notifications" onClick={onClose}>
            ×
          </button>
        </div>
      </header>
      {ordered.length === 0 ? (
        <p className="empty-state">Nothing needs attention.</p>
      ) : (
        <ul>
          {ordered.map((notification) => (
            <li className={notification.level} key={notification.id}>
              <button
                className="notification-open"
                onClick={() => {
                  onSelect(notification);
                  onDismiss(notification);
                  onClose();
                }}
              >
                <span className="notification-level">
                  {levelLabel(notification.level)}
                </span>
                <strong>{notification.title}</strong>
                <p>{notification.message}</p>
                <time
                  dateTime={new Date(notification.created_at_ms).toISOString()}
                >
                  {relativeTime(notification.created_at_ms)}
                </time>
              </button>
              <button
                className="notification-dismiss"
                aria-label={`Clear ${notification.title}`}
                onClick={() => onDismiss(notification)}
              >
                ×
              </button>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
