/* eslint-disable react-refresh/only-export-components */
import { useEffect, useMemo, useRef, useState } from "react";

import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  BobnetMessageSummary,
  BobnetSnapshot,
  EntityRef,
} from "./protocol";

const empty = (snapshot: BobnetSnapshot) =>
  snapshot.sources.length === 0 && snapshot.messages.length === 0;

export function normalizeBobnetChannel(channel: string): string {
  const trimmed = channel.trim();
  if (!trimmed) return "";
  return trimmed.startsWith("#") ? trimmed : `#${trimmed}`;
}

function messageTime(value: string | null): number {
  if (!value) return Number.NEGATIVE_INFINITY;
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? Number.NEGATIVE_INFINITY : parsed;
}

export function channelMessages(
  messages: BobnetMessageSummary[],
  channel: string,
): BobnetMessageSummary[] {
  const normalized = normalizeBobnetChannel(channel).toLowerCase();
  return messages
    .filter(
      (message) =>
        normalizeBobnetChannel(message.channel ?? "").toLowerCase() ===
        normalized,
    )
    .sort(
      (left, right) =>
        messageTime(left.created_at) - messageTime(right.created_at) ||
        (left.id ?? Number.NEGATIVE_INFINITY) -
          (right.id ?? Number.NEGATIVE_INFINITY),
    );
}

function clockTime(value: string | null): string {
  if (!value) return "--:--";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "--:--"
    : date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}

export function BobNetPage({
  onSelectEntity,
}: {
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const query = useDomainQuery({
    slice: "bobnet",
    // Always fetch the complete recent history. NPC/system visibility is a
    // presentation preference and should never trigger another daemon/API read.
    fetcher: (signal) => daemonApi.bobnet({ includeNpcs: true }, signal),
    isEmpty: empty,
  });
  const [includeNpcs, setIncludeNpcs] = useState(true);
  return (
    <BobNetContent
      {...query}
      includeNpcs={includeNpcs}
      onIncludeNpcsChange={setIncludeNpcs}
      onSelectEntity={onSelectEntity}
    />
  );
}

export function BobNetContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  includeNpcs,
  onIncludeNpcsChange,
  onSelectEntity,
}: {
  data?: BobnetSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  includeNpcs: boolean;
  onIncludeNpcsChange: (include: boolean) => void;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const [selectedChannel, setSelectedChannel] = useState("");
  const [channelEntry, setChannelEntry] = useState("");
  const [sender, setSender] = useState("");
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const [sendError, setSendError] = useState<string | null>(null);
  const [sendStatus, setSendStatus] = useState<string | null>(null);
  const composerRef = useRef<HTMLTextAreaElement>(null);
  const transcriptRef = useRef<HTMLDivElement>(null);

  const channels = useMemo(() => {
    const names = new Map<string, string>();
    for (const channel of data?.channels ?? []) {
      const normalized = normalizeBobnetChannel(channel.name);
      if (normalized) names.set(normalized.toLowerCase(), normalized);
    }
    for (const message of data?.messages ?? []) {
      const normalized = normalizeBobnetChannel(message.channel ?? "");
      if (normalized) names.set(normalized.toLowerCase(), normalized);
    }
    return [...names.values()].sort((left, right) => left.localeCompare(right));
  }, [data?.channels, data?.messages]);
  const channelActivity = useMemo(
    () =>
      new Map(
        (data?.channels ?? []).map((channel) => [
          normalizeBobnetChannel(channel.name).toLowerCase(),
          channel.last_active,
        ]),
      ),
    [data?.channels],
  );
  const defaultChannel =
    channels.find((channel) => channel.toLowerCase() === "#general") ??
    channels[0] ??
    "#general";
  const activeChannel = normalizeBobnetChannel(
    selectedChannel || defaultChannel,
  );
  const visibleMessages = useMemo(
    () =>
      channelMessages(data?.messages ?? [], activeChannel).filter(
        (message) => includeNpcs || !message.is_npc_or_system,
      ),
    [activeChannel, data?.messages, includeNpcs],
  );
  const activeSender = sender || data?.replicants[0]?.entity.id || "";
  const activeSenderSummary = data?.replicants.find(
    (replicant) => replicant.entity.id === activeSender,
  );

  useEffect(() => {
    const transcript = transcriptRef.current;
    if (transcript) transcript.scrollTop = transcript.scrollHeight;
  }, [activeChannel, visibleMessages.length]);

  const send = async () => {
    const text = draft.trim();
    if (!text || !activeSender || !activeChannel || sending) return;
    setSending(true);
    setSendError(null);
    setSendStatus(null);
    try {
      await daemonApi.runOperation("action", "bobnet.send", {
        replicant: activeSender,
        channel: activeChannel,
        text,
      });
      setDraft("");
      setSendStatus("Submitted to BobNet; waiting for relay echo…");
      if (typeof window !== "undefined") {
        window.setTimeout(() => {
          void refresh();
        }, 900);
      }
    } catch (sendFailure) {
      setSendError(String(sendFailure));
    } finally {
      setSending(false);
    }
  };

  if (!data && status === "loading")
    return (
      <article className="page loading-state">Connecting to BobNet…</article>
    );
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>BobNet unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  return (
    <article className="page bobnet-page">
      <header className="page-heading bobnet-page-heading">
        <div>
          <p className="eyebrow">Intelligence / </p>
          <h1>BobNet</h1>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      {data?.error && <p className="inline-warning">{data.error}</p>}

      <div className="bobnet-client">
        <aside
          className="bobnet-sidebar bobnet-channels"
          aria-label="BobNet channels"
        >
          <div className="bobnet-sidebar-heading">
            <strong>Channels</strong>
            <small>{channels.length} observed</small>
          </div>
          <form
            className="bobnet-channel-entry"
            onSubmit={(event) => {
              event.preventDefault();
              const channel = normalizeBobnetChannel(channelEntry);
              if (!channel) return;
              setSelectedChannel(channel);
              setChannelEntry("");
              composerRef.current?.focus();
            }}
          >
            <input
              aria-label="Open BobNet channel"
              placeholder="#channel"
              value={channelEntry}
              onChange={(event) => {
                setChannelEntry(event.target.value);
              }}
            />
            <button type="submit">Open</button>
          </form>
          <nav className="bobnet-channel-list">
            {channels.map((channel) => (
              <button
                className={
                  normalizeBobnetChannel(channel).toLowerCase() ===
                  activeChannel.toLowerCase()
                    ? "active"
                    : ""
                }
                key={channel}
                onClick={() => {
                  setSelectedChannel(channel);
                }}
              >
                <span>{channel}</span>
                <small>
                  {channelActivity.get(channel.toLowerCase())
                    ? clockTime(
                        channelActivity.get(channel.toLowerCase()) ?? null,
                      )
                    : ""}
                </small>
              </button>
            ))}
            {!channels.length && (
              <p className="bobnet-sidebar-empty">No channels observed yet.</p>
            )}
          </nav>
        </aside>

        <section className="bobnet-chat" aria-label={`BobNet ${activeChannel}`}>
          <header className="bobnet-chat-header">
            <div>
              <strong>{activeChannel}</strong>
              <small>
                {visibleMessages.length} loaded
                {typeof data?.total_messages === "number"
                  ? ` · ${String(data.total_messages)} available in recent history`
                  : ""}
              </small>
            </div>
          </header>

          <div
            ref={transcriptRef}
            className="bobnet-transcript"
            role="log"
            aria-live="polite"
          >
            {visibleMessages.map((message, index) => {
              const sender = message.sender;
              const currentSystem = message.current_system;
              const senderLabel =
                message.sender_name ?? sender ?? "NPC / system";
              return (
                <div
                  className={`bobnet-line${message.is_npc_or_system ? " system" : ""}`}
                  key={
                    message.id ??
                    `${message.created_at ?? "message"}:${String(index)}`
                  }
                >
                  <time dateTime={message.created_at ?? undefined}>
                    {clockTime(message.created_at)}
                  </time>
                  {sender ? (
                    <button
                      className="bobnet-nick"
                      onClick={() => {
                        onSelectEntity({
                          kind: "replicant",
                          id: sender,
                        });
                      }}
                    >
                      &lt;{senderLabel}&gt;
                    </button>
                  ) : (
                    <strong className="bobnet-system-nick">
                      * {senderLabel}
                    </strong>
                  )}
                  <span className="bobnet-line-body">{message.body ?? ""}</span>
                  {currentSystem && (
                    <button
                      className="bobnet-system-link"
                      title={`Sent from ${currentSystem}`}
                      onClick={() => {
                        onSelectEntity({
                          kind: "system",
                          id: currentSystem,
                        });
                      }}
                    >
                      {currentSystem}
                    </button>
                  )}
                </div>
              );
            })}
            {!visibleMessages.length && (
              <p className="bobnet-empty-transcript">
                No messages for {activeChannel} in this relay&apos;s recent
                history.
              </p>
            )}
          </div>

          <form
            className="bobnet-composer"
            onSubmit={(event) => {
              event.preventDefault();
              void send();
            }}
          >
            <span className="bobnet-composer-prompt">
              {(activeSenderSummary?.name ?? activeSender) || "no sender"}@
              {activeChannel}
            </span>
            <textarea
              ref={composerRef}
              rows={2}
              placeholder={
                activeSender
                  ? `Message ${activeChannel}`
                  : "No owned replicant is available to send"
              }
              value={draft}
              disabled={!activeSender || !data?.selected_source}
              onChange={(event) => {
                setDraft(event.target.value);
              }}
              onKeyDown={(event) => {
                if (event.key === "Enter" && !event.shiftKey) {
                  event.preventDefault();
                  void send();
                }
              }}
            />
            <button
              className="primary"
              type="submit"
              disabled={
                !draft.trim() ||
                !activeSender ||
                !data?.selected_source ||
                sending
              }
            >
              {sending ? "Sending…" : "Send"}
            </button>
          </form>
          {sendError && (
            <p className="form-error bobnet-send-status">{sendError}</p>
          )}
          {!sendError && sendStatus && (
            <p className="bobnet-send-status">{sendStatus}</p>
          )}
        </section>

        <aside
          className="bobnet-sidebar bobnet-nicks"
          aria-label="BobNet senders"
        >
          <div className="bobnet-sidebar-heading">
            <strong>Replicants</strong>
            <small>{data?.replicants.length ?? 0} available</small>
          </div>
          <div className="bobnet-nick-list">
            {data?.replicants.map((replicant) => (
              <button
                className={replicant.entity.id === activeSender ? "active" : ""}
                key={replicant.entity.id}
                onClick={() => {
                  setSender(replicant.entity.id);
                }}
              >
                <span>{replicant.name ?? replicant.entity.id}</span>
                <small>
                  {replicant.location ??
                    replicant.status ??
                    replicant.entity.id}
                </small>
              </button>
            ))}
            {!data?.replicants.length && (
              <p className="bobnet-sidebar-empty">
                No owned replicants in managed state.
              </p>
            )}
          </div>
          <label className="bobnet-npc-toggle">
            <input
              type="checkbox"
              checked={includeNpcs}
              onChange={(event) => {
                onIncludeNpcsChange(event.target.checked);
              }}
            />
            Include NPC / system chatter
          </label>
        </aside>
      </div>
    </article>
  );
}
