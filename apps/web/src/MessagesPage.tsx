/* eslint-disable react-refresh/only-export-components */
import { useMemo, useState } from "react";

import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { InboxMessageSummary, MessagesSnapshot } from "./protocol";

const empty = (snapshot: MessagesSnapshot) => snapshot.inbox.length === 0;

function messageTime(value: string | null): number {
  if (!value) return Number.NEGATIVE_INFINITY;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed;
}

export function filterInboxMessages(
  messages: InboxMessageSummary[],
  search: string,
  messageType: string,
  unreadOnly: boolean,
): InboxMessageSummary[] {
  const needle = search.trim().toLowerCase();
  return messages
    .filter((message) => {
      if (messageType && message.message_type !== messageType) return false;
      if (unreadOnly && message.is_read !== false) return false;
      if (!needle) return true;
      return [message.title, message.body]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(needle);
    })
    .sort(
      (left, right) =>
        messageTime(right.created_at) - messageTime(left.created_at) ||
        (right.id ?? Number.NEGATIVE_INFINITY) -
          (left.id ?? Number.NEGATIVE_INFINITY),
    );
}

export function MessagesPage() {
  const query = useDomainQuery({
    fetcher: (signal) => daemonApi.messages(signal),
    isEmpty: empty,
  });
  return <MessagesContent {...query} />;
}

export function MessagesContent({
  data,
  status,
  error,
  refreshing,
  refresh,
}: {
  data?: MessagesSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
}) {
  const [search, setSearch] = useState("");
  const [messageType, setMessageType] = useState("");
  const [unreadOnly, setUnreadOnly] = useState(false);
  const messageTypes = useMemo(
    () =>
      [
        ...new Set(
          (data?.inbox ?? [])
            .map((message) => message.message_type)
            .filter((value): value is string => Boolean(value)),
        ),
      ].sort((left, right) => left.localeCompare(right)),
    [data?.inbox],
  );
  const filteredInbox = useMemo(
    () =>
      filterInboxMessages(data?.inbox ?? [], search, messageType, unreadOnly),
    [data?.inbox, messageType, search, unreadOnly],
  );

  if (!data && status === "loading")
    return <article className="page loading-state">Loading Messages…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Messages unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Messages</h1>
          <p className="lede">
            Account notifications, mission notices, and system messages.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <section>
        <h2>Account inbox</h2>
        <div className="message-filters">
          <label>
            <span>Search</span>
            <input
              type="search"
              placeholder="Title or message body"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
            />
          </label>
          <label>
            <span>Message type</span>
            <select
              value={messageType}
              onChange={(event) => setMessageType(event.target.value)}
            >
              <option value="">All types</option>
              {messageTypes.map((type) => (
                <option key={type} value={type}>
                  {type}
                </option>
              ))}
            </select>
          </label>
          <label className="message-unread-filter">
            <input
              type="checkbox"
              checked={unreadOnly}
              onChange={(event) => setUnreadOnly(event.target.checked)}
            />
            Unread only
          </label>
        </div>
        <p className="table-summary">
          {filteredInbox.length} shown
          {typeof data?.unread_count === "number" &&
            ` · ${data.unread_count} unread`}
        </p>
        {filteredInbox.length ? (
          <div className="message-list">
            {filteredInbox.map((message, index) => (
              <article key={message.id ?? index}>
                <header className="message-card-header">
                  <strong className="message-card-title">
                    {message.title ?? message.message_type ?? "Message"}
                  </strong>
                  <div className="message-card-badges">
                    <span className="status-chip">
                      {message.is_read === false ? "unread" : "read"}
                    </span>
                    {message.message_type && (
                      <span className="status-chip">
                        {message.message_type}
                      </span>
                    )}
                  </div>
                  {(message.category || message.created_at) && (
                    <div className="message-card-meta">
                      {message.category && <span>{message.category}</span>}
                      {message.created_at && (
                        <time dateTime={message.created_at}>
                          {new Date(message.created_at).toLocaleString()}
                        </time>
                      )}
                    </div>
                  )}
                </header>
                <p>{message.body ?? "No message body."}</p>
              </article>
            ))}
          </div>
        ) : (
          <p className="empty-state">
            {data?.inbox.length
              ? "No account messages match the current filters."
              : "No account messages."}
          </p>
        )}
      </section>
    </article>
  );
}
