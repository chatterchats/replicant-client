/* eslint-disable react-refresh/only-export-components */
import { useMemo, useState } from "react";

import { daemonApi } from "./api";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  EntityRef,
  InventoryLocationSummary,
  InventoryResourceSummary,
  InventorySnapshot,
} from "./protocol";

export type InventoryMode = "location" | "resource";

const inventoryEmpty = (snapshot: InventorySnapshot) =>
  snapshot.locations.length === 0 && snapshot.resources.length === 0;

export function filterInventoryLocations(
  rows: InventoryLocationSummary[],
  search: string,
): InventoryLocationSummary[] {
  const query = search.trim().toLowerCase();
  return [...rows]
    .filter((row) =>
      [
        row.system,
        row.location,
        row.owner,
        ...row.resources.map(({ resource }) => resource),
      ]
        .filter(Boolean)
        .join(" ")
        .toLowerCase()
        .includes(query),
    )
    .sort(
      (left, right) =>
        right.total_quantity - left.total_quantity ||
        left.owner.localeCompare(right.owner),
    );
}

export function filterInventoryResources(
  rows: InventoryResourceSummary[],
  search: string,
  descending: boolean,
): InventoryResourceSummary[] {
  const query = search.trim().toLowerCase();
  return [...rows]
    .filter((row) => row.resource.toLowerCase().includes(query))
    .sort((left, right) =>
      descending
        ? right.total_quantity - left.total_quantity ||
          left.resource.localeCompare(right.resource)
        : left.total_quantity - right.total_quantity ||
          left.resource.localeCompare(right.resource),
    );
}

export function InventoryViewTabs({
  mode,
  onChange,
}: {
  mode: InventoryMode;
  onChange: (mode: InventoryMode) => void;
}) {
  return (
    <div className="inventory-tabs" role="tablist" aria-label="Inventory view">
      {(["location", "resource"] as const).map((value) => (
        <button
          key={value}
          role="tab"
          aria-selected={mode === value}
          onClick={() => {
            onChange(value);
          }}
        >
          By {value === "location" ? "Location" : "Resource"}
        </button>
      ))}
    </div>
  );
}

export function InventoryPage({
  onSelectEntity,
  onOpenSystem,
}: {
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
}) {
  const query = useDomainQuery({
    slice: "inventory",
    fetcher: (signal) => daemonApi.inventory(signal),
    isEmpty: inventoryEmpty,
  });
  const [hydrating, setHydrating] = useState(false);
  const [hydrationError, setHydrationError] = useState<string | null>(null);
  const refreshInventory = async () => {
    setHydrating(true);
    setHydrationError(null);
    try {
      await daemonApi.refreshInventory();
    } catch (error) {
      setHydrationError(
        error instanceof Error ? error.message : "Inventory refresh failed",
      );
    } finally {
      await query.refresh();
      setHydrating(false);
    }
  };
  return (
    <InventoryContent
      {...query}
      error={hydrationError ?? query.error}
      refreshing={hydrating || query.refreshing}
      refresh={refreshInventory}
      onSelectEntity={onSelectEntity}
      onOpenSystem={onOpenSystem}
    />
  );
}

export function InventoryContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  onSelectEntity,
  onOpenSystem,
}: {
  data?: InventorySnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
}) {
  const [mode, setMode] = useState<InventoryMode>("location");
  const [search, setSearch] = useState("");
  const [descending, setDescending] = useState(true);
  const locations = useMemo(
    () => filterInventoryLocations(data?.locations ?? [], search),
    [data?.locations, search],
  );
  const resources = useMemo(
    () => filterInventoryResources(data?.resources ?? [], search, descending),
    [data?.resources, descending, search],
  );

  if (!data && status === "loading")
    return <article className="page loading-state">Loading inventory…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Inventory unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );
  if (!data) return null;

  const selectOwner = (row: InventoryLocationSummary) => {
    if (row.location) onSelectEntity({ kind: "location", id: row.location });
    else if (row.owner_kind === "replicant")
      onSelectEntity({ kind: "replicant", id: row.owner });
  };
  const position = (row: {
    system: string | null;
    location: string | null;
    owner: string;
  }) => row.location ?? row.system ?? row.owner;
  const openSystem = (system: string | null) => {
    if (system) onOpenSystem(system);
  };

  return (
    <article className="page inventory-page">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Assets</p>
          <h1>Inventory</h1>
          <p className="lede">
            Resource holdings by storage scope and account-wide totals.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error ? <p className="inline-warning">Refresh failed: {error}</p> : null}
      <section className="inventory-summary" aria-label="Inventory summary">
        <div>
          <strong>{data.total_quantity.toLocaleString()}</strong>
          <span>Total units</span>
        </div>
        <div>
          <strong>{data.resources.length.toLocaleString()}</strong>
          <span>Resources</span>
        </div>
        <div>
          <strong>{data.locations.length.toLocaleString()}</strong>
          <span>Inventory scopes</span>
        </div>
      </section>
      {status === "empty" ? (
        <section className="empty-state">
          No positive managed inventory is currently available.
        </section>
      ) : (
        <>
          <InventoryViewTabs mode={mode} onChange={setMode} />
          <label className="inventory-search">
            <span>Search</span>
            <input
              type="search"
              placeholder={
                mode === "location"
                  ? "System, location, owner, resource"
                  : "Resource"
              }
              value={search}
              onChange={(event) => {
                setSearch(event.target.value);
              }}
            />
          </label>
          <p className="table-summary">
            Showing {mode === "location" ? locations.length : resources.length}{" "}
            · revision {data.metadata.revision}
          </p>
          {mode === "location" ? (
            locations.length === 0 ? (
              <section className="empty-state">
                No inventory scopes match.
              </section>
            ) : (
              <div className="inventory-table-wrap">
                <table className="inventory-table">
                  <thead>
                    <tr>
                      <th>System</th>
                      <th>Location / owner</th>
                      <th>Contents</th>
                      <th>Units</th>
                    </tr>
                  </thead>
                  <tbody>
                    {locations.map((row) => (
                      <tr
                        key={`${row.owner_kind}:${row.owner}:${row.location ?? ""}`}
                      >
                        <td>
                          {row.system ? (
                            <button
                              className="entity-link"
                              onClick={() => {
                                openSystem(row.system);
                              }}
                            >
                              {row.system}
                            </button>
                          ) : (
                            "—"
                          )}
                        </td>
                        <td>
                          <button
                            className="entity-link"
                            disabled={
                              !row.location && row.owner_kind !== "replicant"
                            }
                            onClick={() => {
                              selectOwner(row);
                            }}
                          >
                            {position(row)}
                          </button>
                          <small>
                            {row.owner_kind} · {row.owner}
                          </small>
                        </td>
                        <td>
                          <details>
                            <summary>{row.resources.length} resources</summary>
                            <ul className="inventory-contents">
                              {row.resources.map((item) => (
                                <li key={item.resource}>
                                  <span>{item.resource}</span>
                                  <strong>
                                    {item.quantity.toLocaleString()}
                                  </strong>
                                </li>
                              ))}
                            </ul>
                          </details>
                        </td>
                        <td>{row.total_quantity.toLocaleString()}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )
          ) : resources.length === 0 ? (
            <section className="empty-state">No resources match.</section>
          ) : (
            <div className="inventory-table-wrap">
              <table className="inventory-table">
                <thead>
                  <tr>
                    <th>Resource</th>
                    <th>
                      <button
                        className="table-sort"
                        onClick={() => {
                          setDescending((value) => !value);
                        }}
                      >
                        Total {descending ? "↓" : "↑"}
                      </button>
                    </th>
                    <th>Distribution</th>
                  </tr>
                </thead>
                <tbody>
                  {resources.map((row) => (
                    <tr key={row.resource}>
                      <td>
                        <strong>{row.resource}</strong>
                      </td>
                      <td>{row.total_quantity.toLocaleString()}</td>
                      <td>
                        <details>
                          <summary>{row.distribution.length} scopes</summary>
                          <table className="inventory-distribution">
                            <tbody>
                              {row.distribution.map((item) => (
                                <tr
                                  key={`${item.owner_kind}:${item.owner}:${item.location ?? ""}`}
                                >
                                  <td>
                                    {item.system ? (
                                      <button
                                        className="subtle-link"
                                        onClick={() => {
                                          openSystem(item.system);
                                        }}
                                      >
                                        {item.system}
                                      </button>
                                    ) : (
                                      "—"
                                    )}
                                  </td>
                                  <td>
                                    <button
                                      className="entity-link"
                                      disabled={
                                        !item.location &&
                                        item.owner_kind !== "replicant"
                                      }
                                      onClick={() => {
                                        if (item.location)
                                          onSelectEntity({
                                            kind: "location",
                                            id: item.location,
                                          });
                                        else if (
                                          item.owner_kind === "replicant"
                                        )
                                          onSelectEntity({
                                            kind: "replicant",
                                            id: item.owner,
                                          });
                                      }}
                                    >
                                      {position(item)}
                                    </button>
                                  </td>
                                  <td>{item.quantity.toLocaleString()}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </details>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </>
      )}
    </article>
  );
}
