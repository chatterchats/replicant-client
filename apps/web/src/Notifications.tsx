import { useEffect, useMemo, useRef, useState } from "react";

import { useNotifications } from "./daemon";
import type { Notification, NotificationLevel } from "./protocol";
import { relativeTime } from "./time";

/** How long a toast stays on screen before dismissing itself. */
const TOAST_TIMEOUT_MS = 8_000;

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
  onSelect,
}: {
  onSelect: (notification: Notification) => void;
}) {
  const notifications = useNotifications();
  const seen = useRef<Set<string> | null>(null);
  const [toasts, setToasts] = useState<Notification[]>([]);

  useEffect(() => {
    // Notifications present on first render came with the snapshot and are
    // history, not news; only announce what arrives afterwards.
    if (seen.current === null) {
      seen.current = new Set(notifications.map((item) => item.id));
      return;
    }
    const fresh = notifications.filter(
      (item) => item.level !== "info" && !seen.current?.has(item.id),
    );
    for (const item of notifications) seen.current.add(item.id);
    if (fresh.length > 0) setToasts((current) => [...current, ...fresh]);
  }, [notifications]);

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
  onClose,
  onSelect,
}: {
  onClose: () => void;
  onSelect: (notification: Notification) => void;
}) {
  const notifications = useNotifications();
  const ordered = useMemo(
    () => [...notifications].sort((a, b) => b.created_at_ms - a.created_at_ms),
    [notifications],
  );
  return (
    <div className="notification-center" aria-label="Notifications">
      <header>
        <h2>Notifications</h2>
        <button aria-label="Close notifications" onClick={onClose}>
          ×
        </button>
      </header>
      {ordered.length === 0 ? (
        <p className="empty-state">Nothing needs attention.</p>
      ) : (
        <ul>
          {ordered.map((notification) => (
            <li className={notification.level} key={notification.id}>
              <button
                onClick={() => {
                  onSelect(notification);
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
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
