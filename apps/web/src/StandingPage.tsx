import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { StandingSnapshot } from "./protocol";

function useStanding() {
  return useDomainQuery({
    slice: "standing",
    fetcher: (signal) => daemonApi.standing(signal),
    isEmpty: (snapshot: StandingSnapshot) =>
      snapshot.achievements.length + snapshot.reputation.length === 0 &&
      snapshot.experience_points_total === null,
  });
}

function LoadingOrError({
  title,
  data,
  status,
  error,
  refresh,
}: {
  title: string;
  data?: StandingSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refresh: () => Promise<void>;
}) {
  if (!data && status === "loading")
    return <article className="page loading-state">Loading {title}…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>{title} unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );
  return null;
}

export function AchievementsPage() {
  const query = useStanding();
  const guard = <LoadingOrError title="Achievements" {...query} />;
  if (!query.data && (query.status === "loading" || query.status === "error"))
    return guard;
  const achievements = query.data?.achievements ?? [];
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Achievements</h1>
        </div>
        <button
          disabled={query.refreshing}
          onClick={() => void query.refresh()}
        >
          {query.refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {query.error ? (
        <p className="inline-warning">Refresh failed: {query.error}</p>
      ) : null}
      <section className="overview-metrics">
        <article>
          <small>Experience</small>
          <strong>{query.data?.experience_points_total ?? "—"}</strong>
        </article>
        <article>
          <small>Civilisation points</small>
          <strong>{query.data?.civilisation_points ?? "Not exposed"}</strong>
        </article>
        <article>
          <small>Achievements</small>
          <strong>{achievements.length}</strong>
        </article>
      </section>
      {achievements.length ? (
        <div className="intelligence-card-grid">
          {achievements.map((achievement) => (
            <article key={achievement.key}>
              <span className="status-chip">
                {achievement.category ?? "achievement"}
              </span>
              <h3>{achievement.title ?? achievement.key}</h3>
              <p>{achievement.description ?? "No description supplied."}</p>
              <small>
                {achievement.xp_reward !== null
                  ? `${String(achievement.xp_reward)} XP`
                  : "XP not supplied"}
                {achievement.achieved_at ? ` · ${achievement.achieved_at}` : ""}
              </small>
            </article>
          ))}
        </div>
      ) : (
        <p className="empty-state">No earned achievements were supplied.</p>
      )}
    </article>
  );
}

export function ReputationPage() {
  const query = useStanding();
  return <ReputationContent {...query} />;
}

export function ReputationContent({
  data,
  status,
  error,
  refreshing,
  refresh,
}: {
  data?: StandingSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
}) {
  const guard = (
    <LoadingOrError
      title="Reputation"
      data={data}
      status={status}
      error={error}
      refresh={refresh}
    />
  );
  if (!data && (status === "loading" || status === "error")) return guard;
  const reputation = [...(data?.reputation ?? [])].sort(
    (left, right) =>
      (right.value ?? Number.NEGATIVE_INFINITY) -
        (left.value ?? Number.NEGATIVE_INFINITY) ||
      (left.name ?? left.species).localeCompare(right.name ?? right.species),
  );
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Species Reputation</h1>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error ? <p className="inline-warning">Refresh failed: {error}</p> : null}
      {reputation.length ? (
        <div className="inventory-table-wrap">
          <table className="inventory-table">
            <thead>
              <tr>
                <th>Species</th>
                <th>Reputation</th>
                <th>Trait</th>
                <th>Description</th>
              </tr>
            </thead>
            <tbody>
              {reputation.map((standing) => (
                <tr key={standing.species}>
                  <td>
                    <strong>{standing.name ?? standing.species}</strong>
                  </td>
                  <td>{standing.value ?? "—"}</td>
                  <td>{standing.trait_name ?? "—"}</td>
                  <td>{standing.description ?? "—"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : (
        <p className="empty-state">No species reputation data was supplied.</p>
      )}
    </article>
  );
}

/** Legacy combined renderer retained for compatibility with existing tests/bookmarks. */
export function StandingContent({
  data,
  status,
  error,
  refreshing,
  refresh,
}: {
  data?: StandingSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
}) {
  if (!data && status === "loading")
    return <article className="page loading-state">Loading Standing…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Standing unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );
  const reputation = [...(data?.reputation ?? [])].sort(
    (left, right) =>
      (right.value ?? Number.NEGATIVE_INFINITY) -
      (left.value ?? Number.NEGATIVE_INFINITY),
  );
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Standing</h1>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      <section className="overview-metrics">
        <article>
          <small>Experience</small>
          <strong>{data?.experience_points_total ?? "—"}</strong>
        </article>
        <article>
          <small>Civilisation points</small>
          <strong>{data?.civilisation_points ?? "Not exposed"}</strong>
        </article>
      </section>
      <section>
        <h2>Achievements</h2>
        {(data?.achievements ?? []).map((achievement) => (
          <article key={achievement.key}>
            <h3>{achievement.title ?? achievement.key}</h3>
          </article>
        ))}
      </section>
      <section>
        <h2>Species reputation</h2>
        {reputation.map((standing) => (
          <p key={standing.species}>
            {standing.name ?? standing.species}: {standing.value ?? "—"}
          </p>
        ))}
      </section>
    </article>
  );
}

/** Backward-compatible export for older imports/tests. */
export function StandingPage() {
  return <ReputationPage />;
}
