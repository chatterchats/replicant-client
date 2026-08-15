import { useState } from "react";

import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { EntityRef, MessagesSnapshot } from "./protocol";

const empty = (snapshot: MessagesSnapshot) =>
  snapshot.relays.length +
    snapshot.inbox.length +
    snapshot.relay_messages.length ===
  0;

export function MessagesPage({
  onSelectEntity,
}: {
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const [relay, setRelay] = useState<string>();
  return (
    <MessagesQuery
      key={relay ?? "inbox"}
      relay={relay}
      onRelayChange={setRelay}
      onSelectEntity={onSelectEntity}
    />
  );
}

function MessagesQuery({
  relay,
  onRelayChange,
  onSelectEntity,
}: {
  relay?: string;
  onRelayChange: (relay?: string) => void;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const query = useDomainQuery({
    slice: "messages",
    fetcher: (signal) => daemonApi.messages(relay, signal),
    isEmpty: empty,
  });
  return (
    <MessagesContent
      {...query}
      onRelayChange={onRelayChange}
      onSelectEntity={onSelectEntity}
    />
  );
}

export function MessagesContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  onRelayChange,
  onSelectEntity,
}: {
  data?: MessagesSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  onRelayChange: (relay?: string) => void;
  onSelectEntity: (entity: EntityRef) => void;
}) {
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
            Account inbox and relay-observed BobNet history.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <label className="inventory-search">
        Relay history
        <select
          value={data?.selected_relay ?? ""}
          onChange={(event) => {
            onRelayChange(event.target.value || undefined);
          }}
        >
          <option value="">Account inbox only</option>
          {data?.relays.map((relay) => (
            <option key={relay.id} value={relay.id}>
              {relay.id}
            </option>
          ))}
        </select>
      </label>
      {data?.selected_relay && (
        <section>
          <h2>Relay history · {data.selected_relay}</h2>
          <div className="result-links">
            {data.channels.map((channel) => (
              <span className="status-chip" key={channel.name}>
                {channel.name}
              </span>
            ))}
            <button
              onClick={() => {
                onSelectEntity({
                  kind: "device",
                  id: data.selected_relay ?? "",
                });
              }}
            >
              Inspect relay
            </button>
          </div>
          {data.relay_messages.length ? (
            <div className="message-list">
              {data.relay_messages.map((message, index) => (
                <article key={message.id ?? index}>
                  <header>
                    {message.sender ? (
                      <button
                        onClick={() => {
                          onSelectEntity({
                            kind: "replicant",
                            id: message.sender ?? "",
                          });
                        }}
                      >
                        {message.sender_name ?? message.sender}
                      </button>
                    ) : (
                      <strong>NPC / system</strong>
                    )}
                    <small>{message.channel ?? "Unknown channel"}</small>
                    {message.created_at && <time>{message.created_at}</time>}
                  </header>
                  <p>{message.body ?? "No message body."}</p>
                </article>
              ))}
            </div>
          ) : (
            <p className="empty-state">No relay history is available.</p>
          )}
        </section>
      )}
      <section>
        <h2>Account inbox</h2>
        {data?.unread_count !== null && <p>{data?.unread_count} unread</p>}
        {data?.inbox.length ? (
          <div className="message-list">
            {data.inbox.map((message, index) => (
              <article key={message.id ?? index}>
                <header>
                  <strong>
                    {message.title ?? message.message_type ?? "Message"}
                  </strong>
                  <span className="status-chip">
                    {message.is_read === false ? "unread" : "read"}
                  </span>
                  {message.created_at && <time>{message.created_at}</time>}
                </header>
                <p>{message.body ?? "No message body."}</p>
              </article>
            ))}
          </div>
        ) : (
          <p className="empty-state">No account messages.</p>
        )}
      </section>
    </article>
  );
}
