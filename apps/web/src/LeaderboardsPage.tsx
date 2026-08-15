import { useState } from "react";

import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { EntityRef, LeaderboardsSnapshot } from "./protocol";

const empty = (snapshot: LeaderboardsSnapshot) => snapshot.boards.length === 0;

export function LeaderboardsPage({
  onSelectEntity,
}: {
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const [board, setBoard] = useState<string>();
  return (
    <LeaderboardQuery
      key={board ?? "default"}
      board={board}
      onBoardChange={setBoard}
      onSelectEntity={onSelectEntity}
    />
  );
}

function LeaderboardQuery({
  board,
  onBoardChange,
  onSelectEntity,
}: {
  board?: string;
  onBoardChange: (board: string) => void;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const query = useDomainQuery({
    slice: "leaderboards",
    fetcher: (signal) => daemonApi.leaderboards(board, signal),
    isEmpty: empty,
  });
  return (
    <LeaderboardsContent
      {...query}
      onBoardChange={onBoardChange}
      onSelectEntity={onSelectEntity}
    />
  );
}

export function LeaderboardsContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  onBoardChange,
  onSelectEntity,
}: {
  data?: LeaderboardsSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  onBoardChange: (board: string) => void;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  if (!data && status === "loading")
    return (
      <article className="page loading-state">Loading Leaderboards…</article>
    );
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Leaderboards unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );
  const selected = data?.boards.find(
    (board) => board.key === data.selected_board,
  );
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Leaderboards</h1>
          <p className="lede">
            Daemon-mediated published rankings, refreshed on demand.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <label className="inventory-search">
        Board
        <select
          value={data?.selected_board ?? ""}
          onChange={(event) => {
            onBoardChange(event.target.value);
          }}
        >
          {data?.boards.map((board) => (
            <option key={board.key} value={board.key}>
              {board.name ?? board.key}
            </option>
          ))}
        </select>
      </label>
      {selected && (
        <section className="connection-card">
          <div>
            <strong>{selected.name ?? selected.key}</strong>
            <p>{selected.description ?? "No board description supplied."}</p>
            <small>{selected.board_type ?? "ranking"}</small>
          </div>
        </section>
      )}
      {!data?.boards.length ? (
        <section className="empty-state">No published leaderboards.</section>
      ) : data.entries.length ? (
        <div className="inventory-table-wrap">
          <table className="inventory-table">
            <thead>
              <tr>
                <th>Rank</th>
                <th>Replicant / colony</th>
                <th>Value</th>
                <th>Contributions</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {data.entries.map((entry, index) => (
                <tr
                  key={`${String(entry.rank ?? index)}:${entry.replicant?.id ?? entry.designation ?? "entry"}`}
                >
                  <td>{entry.rank ?? "—"}</td>
                  <td>
                    {entry.name ??
                      entry.replicant?.id ??
                      entry.designation ??
                      "Unknown"}
                  </td>
                  <td>{entry.value ?? "—"}</td>
                  <td>{entry.contribution_count ?? "—"}</td>
                  <td>
                    {entry.replicant && (
                      <button
                        onClick={() => {
                          onSelectEntity(
                            entry.replicant ?? { kind: "replicant", id: "" },
                          );
                        }}
                      >
                        Inspect
                      </button>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <section className="empty-state">
          No ranked entries for this board.
        </section>
      )}
    </article>
  );
}
