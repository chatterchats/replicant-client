import type { Notification, NotificationLevel } from "./protocol";

function levelLabel(level: NotificationLevel): string {
  return level === "error" ? "Error" : level === "warning" ? "Warning" : "Info";
}

/** Formats the currently visible notification set for clipboard export. */
export function formatNotificationsForClipboard(
  notifications: Notification[],
): string {
  return [...notifications]
    .sort((a, b) => b.created_at_ms - a.created_at_ms)
    .map(
      (notification) =>
        `[${new Date(notification.created_at_ms).toISOString()}] ${levelLabel(notification.level)} — ${notification.title}\n${notification.message}`,
    )
    .join("\n\n");
}
