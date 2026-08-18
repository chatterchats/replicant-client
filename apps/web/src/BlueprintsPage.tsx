import { useState } from "react";
import { daemonApi } from "./api";
import { useDomainQuery } from "./domainQuery";

export function BlueprintsPage() {
  const [search, setSearch] = useState("");
  const { data, status, error, refreshing, refresh } = useDomainQuery({
    slice: "blueprints",
    fetcher: (signal) => daemonApi.blueprints(signal),
    isEmpty: (snapshot) => snapshot.blueprints.length === 0,
  });
  if (!data && status === "loading")
    return (
      <article className="page loading-state">Loading Blueprints…</article>
    );
  const rows = (data?.blueprints ?? []).filter((row) =>
    [row.device_type, row.short_description, row.description].some((value) =>
      value?.toLowerCase().includes(search.toLowerCase()),
    ),
  );
  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Assets</p>
          <h1>Blueprints</h1>
          <p className="lede">
            Unlocked manufacturing catalogue with print time, costs,
            capabilities, and directives.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">{error}</p>}
      <label className="inventory-search">
        Search
        <input
          type="search"
          value={search}
          onChange={(event) => {
            setSearch(event.target.value);
          }}
        />
      </label>
      <div className="inventory-table-wrap">
        <table className="inventory-table">
          <thead>
            <tr>
              <th>Device</th>
              <th>Print time</th>
              <th>Resources</th>
              <th>Components</th>
              <th>Features / directives</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={row.device_type}>
                <td>
                  <strong>{row.device_type}</strong>
                  <small>{row.short_description ?? row.description}</small>
                </td>
                <td>
                  {row.print_time_seconds !== null
                    ? `${String(row.print_time_seconds)}s`
                    : "—"}
                </td>
                <td>
                  {row.resources
                    .map((item) => `${String(item.quantity)} ${item.resource}`)
                    .join(", ") || "—"}
                </td>
                <td>
                  {row.components
                    .map((item) => `${String(item.quantity)} ${item.resource}`)
                    .join(", ") || "—"}
                </td>
                <td>
                  {[
                    ...row.features,
                    ...row.directives.map((value) => `AMI:${value}`),
                  ].join(", ") || "—"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </article>
  );
}
