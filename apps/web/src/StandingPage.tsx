import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { StandingSnapshot } from "./protocol";

const empty = (snapshot: StandingSnapshot) =>
  snapshot.achievements.length + snapshot.reputation.length === 0 &&
  snapshot.experience_points_total === null;

export function StandingPage() {
  const query = useDomainQuery({
    slice: "standing",
    fetcher: (signal) => daemonApi.standing(signal),
    isEmpty: empty,
  });
  return <StandingContent {...query} />;
}

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
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Standing</h1>
          <p className="lede">
            Account progression, achievements, and species reputation.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <section className="overview-metrics">
        <article>
          <small>Experience</small>
          <strong>{data?.experience_points_total ?? "—"}</strong>
        </article>
        <article>
          <small>Civilisation points</small>
          <strong>{data?.civilisation_points ?? "Not exposed"}</strong>
        </article>
        <article>
          <small>Achievements</small>
          <strong>{data?.achievements.length ?? 0}</strong>
        </article>
      </section>
      <section>
        <h2>Achievements</h2>
        {data?.achievements.length ? (
          <div className="intelligence-card-grid">
            {data.achievements.map((achievement) => (
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
                  {achievement.achieved_at
                    ? ` · ${achievement.achieved_at}`
                    : ""}
                </small>
              </article>
            ))}
          </div>
        ) : (
          <p className="empty-state">No earned achievements were supplied.</p>
        )}
      </section>
      <section>
        <h2>Species reputation</h2>
        {data?.reputation.length ? (
          <div className="inventory-table-wrap">
            <table className="inventory-table">
              <thead>
                <tr>
                  <th>Species</th>
                  <th>Standing</th>
                  <th>Trait</th>
                  <th>Description</th>
                </tr>
              </thead>
              <tbody>
                {data.reputation.map((standing) => (
                  <tr key={standing.species}>
                    <td>{standing.name ?? standing.species}</td>
                    <td>{standing.value ?? "—"}</td>
                    <td>{standing.trait_name ?? "—"}</td>
                    <td>{standing.description ?? "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty-state">No reputation data was supplied.</p>
        )}
      </section>
    </article>
  );
}
