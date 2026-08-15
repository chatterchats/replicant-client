import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type { EntityRef, NetworkSnapshot } from "./protocol";

const empty = (snapshot: NetworkSnapshot) =>
  snapshot.relays.length + snapshot.replicants.length === 0;

export function NetworkPage({
  onSelectEntity,
}: {
  onSelectEntity: (entity: EntityRef) => void;
}) {
  const query = useDomainQuery({
    slice: "network",
    fetcher: (signal) => daemonApi.network(signal),
    isEmpty: empty,
  });
  return <NetworkContent {...query} onSelectEntity={onSelectEntity} />;
}

export function NetworkContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  onSelectEntity,
}: {
  data?: NetworkSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  onSelectEntity: (entity: EntityRef) => void;
}) {
  if (!data && status === "loading")
    return <article className="page loading-state">Loading Network…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Network unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Network</h1>
          <p className="lede">
            Account relationships and real BobNet relay visibility.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <section className="connection-card">
        <span className="status-dot connected" aria-hidden="true" />
        <div>
          <strong>{data?.account_name ?? "Authenticated account"}</strong>
          <p>{data?.account_status ?? "Status not supplied"}</p>
          <small>
            Subscribed channels:{" "}
            {data?.subscribed_channels.join(", ") || "none"}
          </small>
        </div>
      </section>
      <section>
        <h2>Account replicants</h2>
        {data?.replicants.length ? (
          <div className="result-links">
            {data.replicants.map((replicant) => (
              <button
                key={replicant.entity.id}
                onClick={() => {
                  onSelectEntity(replicant.entity);
                }}
              >
                <strong>{replicant.name ?? replicant.entity.id}</strong>
                <small>
                  {replicant.location ?? replicant.system ?? "Location unknown"}
                </small>
              </button>
            ))}
          </div>
        ) : (
          <p className="empty-state">No account replicants were supplied.</p>
        )}
      </section>
      <section>
        <h2>Relay devices</h2>
        {data?.relays.length ? (
          <div className="inventory-table-wrap">
            <table className="inventory-table">
              <thead>
                <tr>
                  <th>Relay</th>
                  <th>Connectivity</th>
                  <th>Location</th>
                  <th>Observed channels</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {data.relays.map((relay) => (
                  <tr key={relay.device.entity.id}>
                    <td>{relay.device.entity.id}</td>
                    <td>
                      <span className="status-chip">
                        {relay.error
                          ? "unavailable"
                          : (relay.device.status ?? "available")}
                      </span>
                      {relay.error && <small>{relay.error}</small>}
                    </td>
                    <td>
                      {relay.device.location ?? relay.device.system ?? "—"}
                    </td>
                    <td>
                      {relay.channels.map((channel) => (
                        <small key={channel.name}>{channel.name}</small>
                      ))}
                      {!relay.channels.length && <small>None observed</small>}
                    </td>
                    <td>
                      <button
                        onClick={() => {
                          onSelectEntity(relay.device.entity);
                        }}
                      >
                        Inspect
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        ) : (
          <p className="empty-state">No managed relay devices.</p>
        )}
      </section>
      <p className="inline-warning">
        The current API exposes account membership and relay visibility, not a
        social graph.
      </p>
    </article>
  );
}
