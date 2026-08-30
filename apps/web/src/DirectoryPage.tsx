import { useEffect, useRef, useState } from "react";
import { daemonApi } from "./api";
import { useDomainQuery } from "./domainQuery";
import type { DirectoryReplicantDetail } from "./protocol";

export function DirectoryPage() {
  const [search, setSearch] = useState("");
  const [profile, setProfile] = useState<DirectoryReplicantDetail>();
  const [profileError, setProfileError] = useState<string>();
  const profileController = useRef<AbortController | undefined>(undefined);
  const query = useDomainQuery({
    slice: "directory",
    fetcher: (signal) => daemonApi.directory(search || undefined, signal),
    isEmpty: (snapshot) => snapshot.replicants.length === 0,
  });

  useEffect(
    () => () => {
      profileController.current?.abort();
    },
    [],
  );

  const inspect = async (code: string) => {
    profileController.current?.abort();
    const controller = new AbortController();
    profileController.current = controller;
    setProfileError(undefined);
    try {
      const detail = await daemonApi.directoryReplicant(
        code,
        controller.signal,
      );
      if (controller.signal.aborted) return;
      setProfile(detail.replicant);
    } catch (error) {
      if (!controller.signal.aborted)
        setProfileError(error instanceof Error ? error.message : String(error));
    }
  };

  if (!query.data && query.status === "loading") {
    return (
      <article className="page loading-state">
        Loading Replicant Directory…
      </article>
    );
  }

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Replicant Directory</h1>
          <p className="lede">
            Public players and NPCs known to the multiplayer directory.
          </p>
        </div>
        <button
          disabled={query.refreshing}
          onClick={() => void query.refresh()}
        >
          {query.refreshing ? "Searching…" : "Search"}
        </button>
      </header>
      {query.error && <p className="inline-warning">{query.error}</p>}
      {profileError && <p className="inline-warning">{profileError}</p>}
      <label className="inventory-search">
        Name
        <input
          value={search}
          onChange={(event) => {
            setSearch(event.target.value);
          }}
          placeholder="Partial name"
        />
      </label>
      <div className="inventory-table-wrap">
        <table className="inventory-table">
          <thead>
            <tr>
              <th>Name</th>
              <th>Code</th>
              <th>Last location</th>
              <th>Type</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {query.data?.replicants.map((row) => (
              <tr key={row.entity.id}>
                <td>{row.name ?? "Unknown"}</td>
                <td>{row.entity.id}</td>
                <td>{row.last_location ?? "—"}</td>
                <td>{row.is_npc ? "NPC" : "Player"}</td>
                <td>
                  <button onClick={() => void inspect(row.entity.id)}>
                    Inspect
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {profile && (
        <section className="connection-card">
          <header className="page-heading">
            <div>
              <p className="eyebrow">Public profile</p>
              <h2>{profile.name ?? profile.entity.id}</h2>
              <p>{profile.entity.id}</p>
            </div>
            <button
              onClick={() => {
                setProfile(undefined);
              }}
            >
              Close
            </button>
          </header>
          <dl className="detail-list">
            <div>
              <dt>Type</dt>
              <dd>{profile.is_npc ? "NPC" : "Player"}</dd>
            </div>
            <div>
              <dt>Status</dt>
              <dd>{profile.status ?? "—"}</dd>
            </div>
            <div>
              <dt>Location</dt>
              <dd>{profile.location ?? "—"}</dd>
            </div>
            <div>
              <dt>Hosted device</dt>
              <dd>{profile.hosted_device?.id ?? "—"}</dd>
            </div>
          </dl>
        </section>
      )}
    </article>
  );
}
