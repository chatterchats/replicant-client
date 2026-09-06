/* eslint-disable react-refresh/only-export-components */
import { useEffect, useMemo, useState } from "react";

import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { InboxMessageSummary, MessagesSnapshot } from "./protocol";

const empty = (snapshot: MessagesSnapshot) => snapshot.inbox.length === 0;
const MESSAGE_FILTER_STORAGE_KEY = "replicant.messages.filters.v2";
const UNSPECIFIED_MESSAGE_TYPE = "__unspecified__";
const UNCATEGORIZED_MESSAGE = "__uncategorized__";

export interface MessageFilterState {
  search: string;
  dateFrom: string;
  dateTo: string;
  excludedMessageTypes: string[];
  excludedTitleFamilies: string[];
  excludedCategories: string[];
  excludedReadStates: string[];
}

type ExclusionField =
  | "excludedMessageTypes"
  | "excludedTitleFamilies"
  | "excludedCategories"
  | "excludedReadStates";

interface MessageFilterOption {
  key: string;
  label: string;
  count: number;
}

interface TitleFamilyRule {
  key: string;
  label: string;
  matches: (normalizedTitle: string) => boolean;
}

const TITLE_FAMILY_RULES: readonly TitleFamilyRule[] = [
  {
    key: "salvage-discovered",
    label: "Salvage Discovered",
    matches: (title) => title.startsWith("salvage discovered:"),
  },
  {
    key: "new-resource-site-discovered",
    label: "New Resource Site Discovered",
    matches: (title) => title.startsWith("new resource site discovered:"),
  },
  {
    key: "broadcast-intercepted",
    label: "Broadcast Intercepted",
    matches: (title) => title.startsWith("broadcast intercepted:"),
  },
  {
    key: "achievement-unlocked",
    label: "Achievement Unlocked",
    matches: (title) => title.startsWith("achievement unlocked:"),
  },
  {
    key: "event-completed",
    label: "Event Completed",
    matches: (title) => title.startsWith("event completed:"),
  },
  {
    key: "system-hub-activated",
    label: "System Hub Activated",
    matches: (title) => title.startsWith("system hub activated in "),
  },
  {
    key: "new-blueprints-unlocked",
    label: "New Blueprints Unlocked",
    matches: (title) => title.startsWith("new blueprints unlocked"),
  },
  {
    key: "bill-update",
    label: "Bill Update",
    matches: (title) => title.startsWith("bill update:"),
  },
  {
    key: "incoming-object-detected",
    label: "Incoming Object Detected",
    matches: (title) => title.startsWith("incoming object detected:"),
  },
  {
    key: "welcome-to-sol",
    label: "Welcome to SOL",
    matches: (title) => title.startsWith("welcome to sol"),
  },
  {
    key: "message-from-riker",
    label: "Message from Riker",
    matches: (title) => title.startsWith("message from riker"),
  },
  {
    key: "new-region-detected",
    label: "New Region Detected",
    matches: (title) => title.startsWith("new region detected:"),
  },
  {
    key: "first-contact-survey",
    label: "First Contact Survey",
    matches: (title) => title.startsWith("first contact:"),
  },
];

const OTHER_TITLE_FAMILY = { key: "other", label: "Other" } as const;

export function defaultMessageFilterState(): MessageFilterState {
  return {
    search: "",
    dateFrom: "",
    dateTo: "",
    excludedMessageTypes: [],
    excludedTitleFamilies: [],
    excludedCategories: [],
    excludedReadStates: [],
  };
}

function stringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return [
    ...new Set(
      value.filter((item): item is string => typeof item === "string"),
    ),
  ];
}

export function parseMessageFilterState(
  raw: string | null,
): MessageFilterState {
  const fallback = defaultMessageFilterState();
  if (!raw) return fallback;
  try {
    const parsed: unknown = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || Array.isArray(parsed))
      return fallback;
    const value = parsed as Record<string, unknown>;
    return {
      search: typeof value.search === "string" ? value.search : "",
      dateFrom: typeof value.dateFrom === "string" ? value.dateFrom : "",
      dateTo: typeof value.dateTo === "string" ? value.dateTo : "",
      excludedMessageTypes: stringArray(value.excludedMessageTypes),
      excludedTitleFamilies: stringArray(value.excludedTitleFamilies),
      excludedCategories: stringArray(value.excludedCategories),
      excludedReadStates: stringArray(value.excludedReadStates),
    };
  } catch {
    return fallback;
  }
}

function browserStorage(): Pick<Storage, "getItem" | "setItem"> | null {
  if (typeof window === "undefined") return null;
  try {
    return window.localStorage;
  } catch {
    return null;
  }
}

export function loadMessageFilterState(
  storage: Pick<Storage, "getItem"> | null = browserStorage(),
): MessageFilterState {
  if (!storage) return defaultMessageFilterState();
  try {
    return parseMessageFilterState(storage.getItem(MESSAGE_FILTER_STORAGE_KEY));
  } catch {
    return defaultMessageFilterState();
  }
}

export function saveMessageFilterState(
  filters: MessageFilterState,
  storage: Pick<Storage, "setItem"> | null = browserStorage(),
): void {
  if (!storage) return;
  try {
    storage.setItem(MESSAGE_FILTER_STORAGE_KEY, JSON.stringify(filters));
  } catch {
    // Filtering still works when browser storage is unavailable or full.
  }
}

function messageTime(value: string | null): number {
  if (!value) return Number.NEGATIVE_INFINITY;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed;
}

function dateBoundary(value: string, endOfDay: boolean): number | null {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) return null;
  const parsed = Date.parse(
    `${value}T${endOfDay ? "23:59:59.999" : "00:00:00.000"}`,
  );
  return Number.isNaN(parsed) ? null : parsed;
}

function titleCase(value: string): string {
  return value
    .replaceAll("_", " ")
    .replaceAll("-", " ")
    .split(/\s+/)
    .filter(Boolean)
    .map(
      (part) => `${part.charAt(0).toUpperCase()}${part.slice(1).toLowerCase()}`,
    )
    .join(" ");
}

function messageTypeKey(message: InboxMessageSummary): string {
  return message.message_type?.trim() || UNSPECIFIED_MESSAGE_TYPE;
}

function categoryKey(message: InboxMessageSummary): string {
  return message.category?.trim() || UNCATEGORIZED_MESSAGE;
}

function readStateKey(message: InboxMessageSummary): string {
  if (message.is_read === false) return "unread";
  if (message.is_read === true) return "read";
  return "unknown";
}

export function messageTitleFamily(title: string | null): {
  key: string;
  label: string;
} {
  const normalizedTitle = title?.trim().toLowerCase() ?? "";
  const family = TITLE_FAMILY_RULES.find((rule) =>
    rule.matches(normalizedTitle),
  );
  return family
    ? { key: family.key, label: family.label }
    : { ...OTHER_TITLE_FAMILY };
}

function optionCounts(
  messages: InboxMessageSummary[],
  resolve: (message: InboxMessageSummary) => { key: string; label: string },
): MessageFilterOption[] {
  const options = new Map<string, MessageFilterOption>();
  for (const message of messages) {
    const resolved = resolve(message);
    const current = options.get(resolved.key);
    if (current) current.count += 1;
    else options.set(resolved.key, { ...resolved, count: 1 });
  }
  return [...options.values()].sort(
    (left, right) =>
      right.count - left.count || left.label.localeCompare(right.label),
  );
}

export function filterInboxMessagesWithState(
  messages: InboxMessageSummary[],
  filters: MessageFilterState,
): InboxMessageSummary[] {
  const needle = filters.search.trim().toLowerCase();
  const excludedMessageTypes = new Set(filters.excludedMessageTypes);
  const excludedTitleFamilies = new Set(filters.excludedTitleFamilies);
  const excludedCategories = new Set(filters.excludedCategories);
  const excludedReadStates = new Set(filters.excludedReadStates);
  const dateFrom = dateBoundary(filters.dateFrom, false);
  const dateTo = dateBoundary(filters.dateTo, true);

  return messages
    .filter((message) => {
      if (excludedMessageTypes.has(messageTypeKey(message))) return false;
      if (excludedTitleFamilies.has(messageTitleFamily(message.title).key))
        return false;
      if (excludedCategories.has(categoryKey(message))) return false;
      if (excludedReadStates.has(readStateKey(message))) return false;

      const timestamp = messageTime(message.created_at);
      if ((dateFrom !== null || dateTo !== null) && !Number.isFinite(timestamp))
        return false;
      if (dateFrom !== null && timestamp < dateFrom) return false;
      if (dateTo !== null && timestamp > dateTo) return false;

      if (!needle) return true;
      return [
        message.title,
        message.body,
        message.message_type,
        message.category,
      ]
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

// Retain the focused helper used by existing callers/tests while the page
// itself uses the richer persistent filter state below.
export function filterInboxMessages(
  messages: InboxMessageSummary[],
  search: string,
  messageType: string,
  unreadOnly: boolean,
): InboxMessageSummary[] {
  const narrowed = messageType
    ? messages.filter((message) => message.message_type === messageType)
    : messages;
  return filterInboxMessagesWithState(narrowed, {
    ...defaultMessageFilterState(),
    search,
    excludedReadStates: unreadOnly ? ["read"] : [],
  });
}

function FilterChecklist({
  title,
  options,
  excluded,
  onToggle,
  onSelectAll,
  onSelectNone,
}: {
  title: string;
  options: MessageFilterOption[];
  excluded: string[];
  onToggle: (key: string, included: boolean) => void;
  onSelectAll: () => void;
  onSelectNone: () => void;
}) {
  const excludedSet = new Set(excluded);
  const includedCount = options.filter(
    (option) => !excludedSet.has(option.key),
  ).length;
  return (
    <details className="message-filter-group" open>
      <summary>
        <span>{title}</span>
        <small>
          {includedCount}/{options.length} included
        </small>
      </summary>
      <div className="message-filter-group-actions">
        <button type="button" onClick={onSelectAll}>
          All
        </button>
        <button type="button" onClick={onSelectNone}>
          None
        </button>
      </div>
      <div className="message-filter-options">
        {options.map((option) => (
          <label className="message-filter-option" key={option.key}>
            <input
              type="checkbox"
              checked={!excludedSet.has(option.key)}
              onChange={(event) => {
                onToggle(option.key, event.target.checked);
              }}
            />
            <span>{option.label}</span>
            <small>{option.count}</small>
          </label>
        ))}
      </div>
    </details>
  );
}

export function MessagesPage({
  onUnreadCountChange,
}: {
  onUnreadCountChange?: (count: number) => void;
} = {}) {
  const query = useDomainQuery({
    slice: "messages",
    queryKey: "messages",
    fetcher: (signal) => daemonApi.messages(signal),
    isEmpty: empty,
  });
  const [refreshingMessages, setRefreshingMessages] = useState(false);
  const refreshMessages = async () => {
    setRefreshingMessages(true);
    try {
      await daemonApi.refreshMessages();
    } finally {
      try {
        await query.refresh();
      } finally {
        setRefreshingMessages(false);
      }
    }
  };
  useEffect(() => {
    if (typeof query.data?.unread_count === "number")
      onUnreadCountChange?.(query.data.unread_count);
  }, [onUnreadCountChange, query.data?.unread_count]);
  return (
    <MessagesContent
      {...query}
      refreshing={query.refreshing || refreshingMessages}
      refreshMessages={refreshMessages}
      onUnreadCountChange={onUnreadCountChange}
    />
  );
}

export function MessagesContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  refreshMessages,
  onUnreadCountChange,
}: {
  data?: MessagesSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  refreshMessages?: () => Promise<void>;
  onUnreadCountChange?: (count: number) => void;
}) {
  const [filters, setFilters] = useState<MessageFilterState>(
    loadMessageFilterState,
  );
  const [selectedIds, setSelectedIds] = useState<Set<number>>(new Set());
  const [markingRead, setMarkingRead] = useState(false);
  const [markReadError, setMarkReadError] = useState<string | null>(null);
  const inbox = data?.inbox ?? [];

  const messageTypes = useMemo(
    () =>
      optionCounts(inbox, (message) => {
        const key = messageTypeKey(message);
        return {
          key,
          label:
            key === UNSPECIFIED_MESSAGE_TYPE ? "Unspecified" : titleCase(key),
        };
      }),
    [inbox],
  );
  const titleFamilies = useMemo(
    () => optionCounts(inbox, (message) => messageTitleFamily(message.title)),
    [inbox],
  );
  const categories = useMemo(
    () =>
      optionCounts(inbox, (message) => {
        const key = categoryKey(message);
        return {
          key,
          label:
            key === UNCATEGORIZED_MESSAGE ? "Uncategorized" : titleCase(key),
        };
      }),
    [inbox],
  );
  const readStates = useMemo(
    () =>
      optionCounts(inbox, (message) => {
        const key = readStateKey(message);
        return { key, label: titleCase(key) };
      }),
    [inbox],
  );
  const filteredInbox = useMemo(
    () => filterInboxMessagesWithState(inbox, filters),
    [filters, inbox],
  );
  const filtersActive =
    filters.search.length > 0 ||
    filters.dateFrom.length > 0 ||
    filters.dateTo.length > 0 ||
    filters.excludedMessageTypes.length > 0 ||
    filters.excludedTitleFamilies.length > 0 ||
    filters.excludedCategories.length > 0 ||
    filters.excludedReadStates.length > 0;

  useEffect(() => {
    saveMessageFilterState(filters);
  }, [filters]);

  useEffect(() => {
    const unreadIds = new Set(
      inbox
        .filter((message) => message.is_read === false && message.id !== null)
        .map((message) => message.id as number),
    );
    setSelectedIds((current) => {
      const next = new Set([...current].filter((id) => unreadIds.has(id)));
      if (
        next.size === current.size &&
        [...next].every((id) => current.has(id))
      )
        return current;
      return next;
    });
  }, [inbox]);
  const refreshFailed = typeof data?.freshness.last_error === "string";
  const cachedProjectionStale = data?.freshness.stale === true;

  const updateExcluded = (
    field: ExclusionField,
    key: string,
    included: boolean,
  ) => {
    setFilters((current) => {
      const next = new Set(current[field]);
      if (included) next.delete(key);
      else next.add(key);
      return { ...current, [field]: [...next].sort() };
    });
  };
  const selectAll = (field: ExclusionField) => {
    setFilters((current) => ({ ...current, [field]: [] }));
  };
  const selectNone = (
    field: ExclusionField,
    options: MessageFilterOption[],
  ) => {
    setFilters((current) => ({
      ...current,
      [field]: options.map((option) => option.key).sort(),
    }));
  };

  const markRead = async (markAll: boolean) => {
    const ids = markAll ? [] : [...selectedIds];
    if (!markAll && ids.length === 0) return;
    setMarkingRead(true);
    setMarkReadError(null);
    try {
      const updated = await daemonApi.markMessagesRead({ ids, markAll });
      if (typeof updated.unread_count === "number")
        onUnreadCountChange?.(updated.unread_count);
      setSelectedIds(new Set());
      await refresh();
    } catch (markError: unknown) {
      setMarkReadError(String(markError));
    } finally {
      setMarkingRead(false);
    }
  };

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
        <button
          disabled={refreshing}
          onClick={() => void (refreshMessages ?? refresh)()}
        >
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {(refreshFailed || cachedProjectionStale) && (
        <p className="inline-warning">
          {refreshFailed
            ? "Showing cached messages; refresh failed"
            : "Showing cached messages; refresh recommended"}
        </p>
      )}
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      {markReadError && (
        <p className="inline-warning">Mark read failed: {markReadError}</p>
      )}
      <section>
        <div className="message-section-heading">
          <div>
            <h2>Account inbox</h2>
            <p>Filters are saved automatically for your next visit.</p>
          </div>
          <button
            type="button"
            disabled={!filtersActive}
            onClick={() => {
              setFilters(defaultMessageFilterState());
            }}
          >
            Reset filters
          </button>
        </div>
        <div className="message-filter-primary">
          <label className="message-search-filter">
            <span>Search</span>
            <input
              type="search"
              placeholder="Title, body, type, or category"
              value={filters.search}
              onChange={(event) => {
                setFilters((current) => ({
                  ...current,
                  search: event.target.value,
                }));
              }}
            />
          </label>
          <label>
            <span>From</span>
            <input
              type="date"
              value={filters.dateFrom}
              onChange={(event) => {
                setFilters((current) => ({
                  ...current,
                  dateFrom: event.target.value,
                }));
              }}
            />
          </label>
          <label>
            <span>Through</span>
            <input
              type="date"
              value={filters.dateTo}
              onChange={(event) => {
                setFilters((current) => ({
                  ...current,
                  dateTo: event.target.value,
                }));
              }}
            />
          </label>
        </div>
        <div className="message-filter-groups">
          <FilterChecklist
            title="Title family"
            options={titleFamilies}
            excluded={filters.excludedTitleFamilies}
            onToggle={(key, included) => {
              updateExcluded("excludedTitleFamilies", key, included);
            }}
            onSelectAll={() => {
              selectAll("excludedTitleFamilies");
            }}
            onSelectNone={() => {
              selectNone("excludedTitleFamilies", titleFamilies);
            }}
          />
          <FilterChecklist
            title="Message type"
            options={messageTypes}
            excluded={filters.excludedMessageTypes}
            onToggle={(key, included) => {
              updateExcluded("excludedMessageTypes", key, included);
            }}
            onSelectAll={() => {
              selectAll("excludedMessageTypes");
            }}
            onSelectNone={() => {
              selectNone("excludedMessageTypes", messageTypes);
            }}
          />
          <FilterChecklist
            title="Category"
            options={categories}
            excluded={filters.excludedCategories}
            onToggle={(key, included) => {
              updateExcluded("excludedCategories", key, included);
            }}
            onSelectAll={() => {
              selectAll("excludedCategories");
            }}
            onSelectNone={() => {
              selectNone("excludedCategories", categories);
            }}
          />
          <FilterChecklist
            title="Read state"
            options={readStates}
            excluded={filters.excludedReadStates}
            onToggle={(key, included) => {
              updateExcluded("excludedReadStates", key, included);
            }}
            onSelectAll={() => {
              selectAll("excludedReadStates");
            }}
            onSelectNone={() => {
              selectNone("excludedReadStates", readStates);
            }}
          />
        </div>
        <div className="message-bulk-actions">
          <p className="table-summary">
            {filteredInbox.length} of {inbox.length} shown
            {typeof data?.unread_count === "number" &&
              ` · ${String(data.unread_count)} unread`}
            {selectedIds.size > 0 && ` · ${String(selectedIds.size)} selected`}
          </p>
          <div>
            <button
              disabled={markingRead || selectedIds.size === 0}
              onClick={() => void markRead(false)}
            >
              Mark selected read
            </button>
            <button
              disabled={
                markingRead ||
                (typeof data?.unread_count === "number" &&
                  data.unread_count === 0)
              }
              onClick={() => void markRead(true)}
            >
              Mark all as read
            </button>
          </div>
        </div>
        {filteredInbox.length ? (
          <div className="message-list">
            {filteredInbox.map((message, index) => (
              <article key={message.id ?? index}>
                <header className="message-card-header">
                  {message.id !== null && message.is_read === false && (
                    <label className="message-select">
                      <input
                        type="checkbox"
                        aria-label={`Select ${
                          message.title ??
                          message.message_type ??
                          `message ${String(message.id)}`
                        }`}
                        checked={selectedIds.has(message.id)}
                        onChange={(event) => {
                          setSelectedIds((current) => {
                            const next = new Set(current);
                            if (event.target.checked)
                              next.add(message.id as number);
                            else next.delete(message.id as number);
                            return next;
                          });
                        }}
                      />
                    </label>
                  )}
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
            {inbox.length
              ? "No account messages match the current filters."
              : "No account messages."}
          </p>
        )}
      </section>
    </article>
  );
}
