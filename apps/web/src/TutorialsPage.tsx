import { useEffect, useState } from "react";

import { daemonApi } from "./api";
import { useDomainQuery } from "./domainQuery";

export function TutorialsPage() {
  const [slug, setSlug] = useState<string>();
  const query = useDomainQuery({
    slice: "tutorials",
    fetcher: (signal) => daemonApi.tutorials(slug, signal),
    isEmpty: (snapshot) => snapshot.tutorials.length === 0,
  });
  const { data, status, error, refreshing, refresh } = query;

  useEffect(() => {
    if (slug !== undefined) void refresh();
  }, [refresh, slug]);

  if (!data && status === "loading") {
    return <article className="page loading-state">Loading Tutorials…</article>;
  }

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Intelligence</p>
          <h1>Tutorials</h1>
          <p className="lede">
            Account onboarding progression and API-oriented hints.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">{error}</p>}
      <section className="asset-jobs">
        {data?.tutorials.map((tutorial) => (
          <button
            key={tutorial.slug}
            onClick={() => {
              setSlug(tutorial.slug);
            }}
          >
            <strong>{tutorial.name ?? tutorial.slug}</strong> ·{" "}
            {tutorial.completed
              ? "complete"
              : `${String(tutorial.current_step ?? 0)}/${String(tutorial.total_steps ?? "?")}`}
          </button>
        ))}
      </section>
      {data?.selected && (
        <section className="connection-card">
          <h2>{data.selected.name ?? data.selected.slug}</h2>
          <p>{data.selected.description}</p>
          {data.selected.steps.map((step, index) => (
            <article className="activity-item" key={step.key ?? index}>
              <strong>
                {step.completed ? "✓" : step.current ? "→" : "·"}{" "}
                {step.description ?? step.key ?? `Step ${String(index + 1)}`}
              </strong>
              {step.hint && <p>{step.hint}</p>}
            </article>
          ))}
        </section>
      )}
    </article>
  );
}
