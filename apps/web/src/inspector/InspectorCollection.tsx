import { useMemo, useState } from "react";

import type { EntityCollectionSummary, EntityGroupSummary } from "../protocol";

function groupLabel(group: EntityGroupSummary): string {
  return [group.entity_kind, group.entity_type].filter(Boolean).join(" · ");
}

export function InspectorCollection({
  collection,
  onNavigate,
}: {
  collection: EntityCollectionSummary;
  onNavigate?: (kind: string, id: string) => void;
}) {
  const [filter, setFilter] = useState("");
  const groups = useMemo(() => {
    const query = filter.trim().toLowerCase();
    return [...collection.groups]
      .sort((left, right) =>
        [left.entity_kind, left.entity_type ?? ""]
          .join(":")
          .localeCompare(
            [right.entity_kind, right.entity_type ?? ""].join(":"),
          ),
      )
      .filter((group) => {
        if (!query) return true;
        return [
          group.entity_kind,
          group.entity_type,
          ...group.statuses.map((status) => status.status),
        ]
          .filter(Boolean)
          .some((value) => value?.toLowerCase().includes(query));
      });
  }, [collection.groups, filter]);

  if (collection.total === 0) return null;
  if (collection.total <= 8) {
    return (
      <ul className="inspector-entity-list">
        {[...collection.items]
          .sort((left, right) =>
            `${left.entity.kind}:${left.entity.id}`.localeCompare(
              `${right.entity.kind}:${right.entity.id}`,
            ),
          )
          .map((item) => (
            <li key={`${item.entity.kind}:${item.entity.id}`}>
              <button
                type="button"
                disabled={!onNavigate}
                onClick={() => onNavigate?.(item.entity.kind, item.entity.id)}
              >
                <strong>{item.label}</strong>
                <small>{item.secondary_label ?? item.entity.kind}</small>
              </button>
              {item.status ? (
                <span className="status-chip">{item.status}</span>
              ) : null}
            </li>
          ))}
      </ul>
    );
  }

  return (
    <div className="inspector-groups">
      <label>
        Filter {collection.total.toLocaleString()} items
        <input
          type="search"
          value={filter}
          onChange={(event) => setFilter(event.target.value)}
        />
      </label>
      {groups.map((group) => (
        <details
          key={`${group.entity_kind}:${group.entity_type ?? ""}`}
          className="inspector-group"
        >
          <summary>
            <span>{groupLabel(group)}</span>
            <strong>{group.count.toLocaleString()}</strong>
          </summary>
          {group.statuses.length ? (
            <ul>
              {group.statuses.map((status) => (
                <li key={status.status ?? "unknown"}>
                  <span>{status.status ?? "Unobserved"}</span>
                  <strong>{status.count.toLocaleString()}</strong>
                </li>
              ))}
            </ul>
          ) : null}
        </details>
      ))}
    </div>
  );
}
