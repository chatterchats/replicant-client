import { daemonApi } from "../api";
import { useDaemonState } from "../daemon";
import { useDomainQuery } from "../domainQuery";
import type {
  DirectorGoalSummary,
  DirectorRequirementSummary,
  EntityInspectorSnapshot,
  EntitySummary,
  InventoryDistribution,
  InventoryLocationSummary,
  RequirementSummary,
  WorkflowIntelligenceSnapshot,
  WorkflowReservationSummary,
  WorkflowSummary,
  WorkflowTargetSummary,
} from "../protocol";
import type { SelectedEntity } from "../shellState";
import { InspectorFields } from "./InspectorFields";

const RELAY_TYPES = new Set([
  "ftl_relay",
  "system_hub",
  "deep_space_relay_station",
]);

function workflowId(value: string | { id: string }) {
  return typeof value === "string" ? value : value.id;
}

function requirementApplies(
  entity: SelectedEntity,
  requirement: RequirementSummary,
) {
  if (entity.kind === "system")
    return requirement.scope === `system ${entity.id}`;
  if (entity.kind === "location")
    return requirement.scope === `location ${entity.id}`;
  if (entity.kind === "resource")
    return requirement.target === `available ${entity.id}`;
  return false;
}

function targetApplies(entity: SelectedEntity, target: WorkflowTargetSummary) {
  if (entity.kind === "event")
    return target.kind === "event" && target.key === entity.id;
  if (entity.kind === "system")
    return (
      (target.kind === "system" && target.key === entity.id) ||
      target.system === entity.id
    );
  if (entity.kind === "location")
    return (
      (target.kind === "location" && target.key === entity.id) ||
      target.location === entity.id
    );
  if (entity.kind === "device")
    return target.kind === "device" && target.key === entity.id;
  if (entity.kind === "resource")
    return target.kind === "resource" && target.key === entity.id;
  return false;
}

// Exported for Inspector composition and focused association tests.
// eslint-disable-next-line react-refresh/only-export-components
export function associatedWorkflowIds(
  entity: SelectedEntity,
  snapshot: EntityInspectorSnapshot | undefined,
  requirements: RequirementSummary[],
  intelligence?: WorkflowIntelligenceSnapshot,
) {
  const ids = new Set<string>();
  if (entity.kind === "workflow") ids.add(entity.id);
  if (snapshot?.detail.kind === "device") {
    const claim = snapshot.detail.detail.device.claim;
    if (claim) ids.add(claim.workflow_id);
  } else if (snapshot?.detail.kind === "replicant") {
    const workflow = snapshot.detail.detail.workflow_id;
    if (workflow) ids.add(workflow);
  }
  for (const requirement of requirements) {
    if (requirementApplies(entity, requirement))
      ids.add(requirement.workflow_id);
  }
  for (const target of intelligence?.targets ?? []) {
    if (targetApplies(entity, target)) ids.add(target.workflow_id);
  }
  return [...ids];
}

function regionFor(snapshot: EntityInspectorSnapshot | undefined) {
  if (snapshot?.detail.kind === "device")
    return snapshot.detail.detail.device.region;
  if (snapshot?.detail.kind === "replicant")
    return (
      snapshot.detail.detail.assigned_region ?? snapshot.detail.detail.region
    );
  if (snapshot?.detail.kind === "system") return snapshot.detail.detail.region;
  return null;
}

function targetSystem(
  entity: SelectedEntity,
  summary: EntitySummary,
  snapshot: EntityInspectorSnapshot | undefined,
) {
  if (entity.kind === "system") return entity.id;
  if (snapshot?.detail.kind === "location")
    return snapshot.detail.detail.system;
  return summary.system;
}

function entitySupportsInventory(entity: SelectedEntity) {
  return (
    entity.kind === "resource" ||
    entity.kind === "system" ||
    entity.kind === "location" ||
    entity.kind === "replicant" ||
    entity.kind === "event"
  );
}

function entitySupportsConnectivity(
  entity: SelectedEntity,
  snapshot: EntityInspectorSnapshot | undefined,
) {
  if (
    entity.kind === "system" ||
    entity.kind === "location" ||
    entity.kind === "event"
  )
    return true;
  return (
    snapshot?.detail.kind === "device" &&
    snapshot.detail.detail.device.device_type !== null &&
    RELAY_TYPES.has(snapshot.detail.detail.device.device_type)
  );
}

function workflowRows(
  workflowIds: string[],
  workflows: Record<string, WorkflowSummary>,
) {
  return workflowIds
    .flatMap((id) => (workflows[id] ? [workflows[id]] : []))
    .sort((left, right) => right.updated_at_ms - left.updated_at_ms);
}

function reservationApplies(
  entity: SelectedEntity,
  reservation: WorkflowReservationSummary,
) {
  if (entity.kind === "resource") return reservation.resource === entity.id;
  if (entity.kind === "workflow") return reservation.workflow_id === entity.id;
  if (entity.kind === "system") return reservation.system === entity.id;
  if (entity.kind === "location") return reservation.location === entity.id;
  if (entity.kind === "device")
    return (
      reservation.entity?.kind === "device" &&
      reservation.entity.id === entity.id
    );
  return false;
}

function WorkflowOwnershipContext({
  entity,
  intelligence,
  onNavigate,
}: {
  entity: SelectedEntity;
  intelligence?: WorkflowIntelligenceSnapshot;
  onNavigate: (kind: string, id: string) => void;
}) {
  const daemon = useDaemonState();
  const targets = (intelligence?.targets ?? []).filter((target) =>
    targetApplies(entity, target),
  );
  if (!targets.length) return null;
  return (
    <div className="inspector-intelligence-group">
      <h4>Workflow ownership</h4>
      <ul className="inspector-context-list">
        {targets.map((target) => {
          const workflow = daemon.workflows[target.workflow_id];
          return (
            <li key={`${target.workflow_id}:${target.kind}:${target.key}`}>
              <button
                type="button"
                className="inspector-context-link"
                onClick={() => {
                  onNavigate("workflow", target.workflow_id);
                }}
              >
                <strong>{workflow?.kind ?? target.workflow_id}</strong>
                <small>
                  {workflow?.status ?? "active"}
                  {workflow?.current_step ? ` · ${workflow.current_step}` : ""}
                  {` · ${target.kind} ${target.key}`}
                </small>
              </button>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function ReservationContext({
  entity,
  intelligence,
  onNavigate,
}: {
  entity: SelectedEntity;
  intelligence?: WorkflowIntelligenceSnapshot;
  onNavigate: (kind: string, id: string) => void;
}) {
  const daemon = useDaemonState();
  const reservations = (intelligence?.reservations ?? []).filter(
    (reservation) => reservationApplies(entity, reservation),
  );
  if (!reservations.length || entity.kind === "resource") return null;
  const total = reservations.reduce(
    (sum, reservation) => sum + reservation.quantity,
    0,
  );
  return (
    <div className="inspector-intelligence-group">
      <h4>Committed capacity</h4>
      <InspectorFields
        fields={[
          { label: "Active reservations", value: reservations.length },
          { label: "Committed quantity", value: total },
        ]}
      />
      <ul className="inspector-context-list">
        {reservations.slice(0, 10).map((reservation) => {
          const workflow = daemon.workflows[reservation.workflow_id];
          return (
            <li key={reservation.allocation_id}>
              <button
                type="button"
                className="inspector-context-link"
                onClick={() => {
                  onNavigate("workflow", reservation.workflow_id);
                }}
              >
                <strong>{reservation.requirement_key}</strong>
                <small>
                  {reservation.quantity.toLocaleString()} ·{" "}
                  {workflow?.kind ?? reservation.workflow_id}
                </small>
              </button>
            </li>
          );
        })}
      </ul>
    </div>
  );
}

function AutomationContext({
  entity,
  workflowIds,
  onNavigate,
}: {
  entity: SelectedEntity;
  workflowIds: string[];
  onNavigate: (kind: string, id: string) => void;
}) {
  const daemon = useDaemonState();
  const workflows = workflowRows(workflowIds, daemon.workflows);
  const requirements = daemon.requirements.filter((requirement) =>
    requirementApplies(entity, requirement),
  );
  if (!workflows.length && !requirements.length) return null;
  return (
    <div className="inspector-intelligence-group">
      <h4>Automation</h4>
      {requirements.length ? (
        <ul className="inspector-context-list">
          {requirements.map((requirement) => (
            <li key={requirement.id}>
              <button
                type="button"
                className="inspector-context-link"
                onClick={() => {
                  onNavigate("workflow", requirement.workflow_id);
                }}
              >
                <strong>{requirement.name}</strong>
                <small>
                  {requirement.actual.toLocaleString()} present ·{" "}
                  {requirement.in_progress.toLocaleString()} in progress ·{" "}
                  {requirement.missing.toLocaleString()} missing
                </small>
              </button>
            </li>
          ))}
        </ul>
      ) : null}
      {workflows.length ? (
        <ul className="inspector-context-list">
          {workflows.map((workflow) => (
            <li key={workflow.id}>
              <button
                type="button"
                className="inspector-context-link"
                onClick={() => {
                  onNavigate("workflow", workflow.id);
                }}
              >
                <strong>{workflow.kind}</strong>
                <small>
                  {workflow.status}
                  {workflow.current_step ? ` · ${workflow.current_step}` : ""}
                </small>
              </button>
            </li>
          ))}
        </ul>
      ) : null}
    </div>
  );
}

function directorPriority(requirement: DirectorRequirementSummary) {
  const status = requirement.status;
  const statusRank =
    status === "blocked"
      ? 0
      : status === "active"
        ? 1
        : status === "pending"
          ? 2
          : status === "unavailable"
            ? 3
            : 4;
  return [statusRank, -requirement.priority] as const;
}

function relevantDirectorRequirements(
  requirements: DirectorRequirementSummary[],
  region: string | null,
  workflowIds: Set<string>,
) {
  return requirements
    .filter(
      (requirement) =>
        (region !== null && requirement.region === region) ||
        requirement.active_workflows.some((id) =>
          workflowIds.has(workflowId(id)),
        ),
    )
    .sort((left, right) => {
      const [leftStatus, leftPriority] = directorPriority(left);
      const [rightStatus, rightPriority] = directorPriority(right);
      return leftStatus - rightStatus || leftPriority - rightPriority;
    })
    .slice(0, 6);
}

function relevantDirectorGoals(
  goals: DirectorGoalSummary[],
  region: string | null,
  workflowIds: Set<string>,
) {
  return goals
    .filter(
      (goal) =>
        goal.enabled &&
        ((region !== null && goal.region === region) ||
          goal.active_workflows.some((id) => workflowIds.has(workflowId(id)))),
    )
    .sort((left, right) => {
      const rank = (status: DirectorGoalSummary["status"]) =>
        status === "blocked"
          ? 0
          : status === "active"
            ? 1
            : status === "waiting"
              ? 2
              : 3;
      return (
        rank(left.status) - rank(right.status) ||
        left.id.localeCompare(right.id)
      );
    })
    .slice(0, 6);
}

function DirectorContext({
  region,
  workflowIds,
  onNavigate,
}: {
  region: string | null;
  workflowIds: string[];
  onNavigate: (kind: string, id: string) => void;
}) {
  const query = useDomainQuery({
    slice: "director",
    queryKey: "inspector:director",
    fetcher: (signal) => daemonApi.director(signal),
    isEmpty: () => false,
  });
  const director = query.data;
  if (!director) return null;
  const ids = new Set(workflowIds);
  const regionSummary = region
    ? director.regions.find((candidate) => candidate.region === region)
    : undefined;
  const goals = relevantDirectorGoals(director.goals, region, ids);
  const requirements = relevantDirectorRequirements(
    director.requirements,
    region,
    ids,
  );
  if (!regionSummary && !goals.length && !requirements.length) return null;
  return (
    <div className="inspector-intelligence-group">
      <h4>Director</h4>
      <InspectorFields
        fields={[
          { label: "Mode", value: director.mode },
          { label: "Region", value: regionSummary?.region ?? region },
          { label: "Region status", value: regionSummary?.status },
          { label: "Regional hub", value: regionSummary?.hub_system },
          { label: "Manufacturing home", value: regionSummary?.hub_location },
        ]}
      />
      {goals.length ? (
        <div className="inspector-context-subgroup">
          <strong>Standing goals</strong>
          <ul className="inspector-context-list">
            {goals.map((goal) => (
              <li key={goal.id}>
                <div className="inspector-context-card">
                  <strong>{goal.objective}</strong>
                  <small>{goal.status}</small>
                  {goal.blocker ? <span>{goal.blocker}</span> : null}
                  {goal.next_action ? (
                    <span>Next: {goal.next_action}</span>
                  ) : null}
                  {goal.active_workflows.map((id) => (
                    <button
                      key={workflowId(id)}
                      type="button"
                      className="subtle-link"
                      onClick={() => {
                        onNavigate("workflow", workflowId(id));
                      }}
                    >
                      {workflowId(id)}
                    </button>
                  ))}
                </div>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
      {requirements.length ? (
        <div className="inspector-context-subgroup">
          <strong>Shared prerequisites</strong>
          <ul className="inspector-context-list">
            {requirements.map((requirement) => (
              <li key={requirement.id}>
                <div className="inspector-context-card">
                  <strong>{requirement.target}</strong>
                  <small>
                    {requirement.kind} · {requirement.status} · priority{" "}
                    {requirement.priority}
                  </small>
                  {requirement.active_workflows.map((id) => (
                    <button
                      key={workflowId(id)}
                      type="button"
                      className="subtle-link"
                      onClick={() => {
                        onNavigate("workflow", workflowId(id));
                      }}
                    >
                      {workflowId(id)}
                    </button>
                  ))}
                </div>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}

function componentSize(
  start: string,
  edges: { from: string; to: string }[],
  activeNode: boolean,
) {
  if (!activeNode) return 0;
  const adjacency = new Map<string, Set<string>>();
  for (const edge of edges) {
    const from = adjacency.get(edge.from) ?? new Set<string>();
    from.add(edge.to);
    adjacency.set(edge.from, from);
    const to = adjacency.get(edge.to) ?? new Set<string>();
    to.add(edge.from);
    adjacency.set(edge.to, to);
  }
  const visited = new Set([start]);
  const queue = [start];
  while (queue.length) {
    const current = queue.shift();
    if (current === undefined) break;
    for (const neighbor of adjacency.get(current) ?? []) {
      if (!visited.has(neighbor)) {
        visited.add(neighbor);
        queue.push(neighbor);
      }
    }
  }
  return visited.size;
}

function ConnectivityContext({
  system,
  onNavigate,
}: {
  system: string;
  onNavigate: (kind: string, id: string) => void;
}) {
  const query = useDomainQuery({
    slice: "missions",
    queryKey: "inspector:relay",
    fetcher: (signal) => daemonApi.relay(signal),
    isEmpty: () => false,
  });
  const network = query.data;
  if (!network) return null;
  const relays = network.relays.filter((relay) => relay.system === system);
  const staged = network.staged_relays.filter(
    (relay) => relay.system === system,
  );
  const neighbors = [
    ...new Set(
      network.relay_edges.flatMap((edge) =>
        edge.from === system
          ? [edge.to]
          : edge.to === system
            ? [edge.from]
            : [],
      ),
    ),
  ].sort();
  const expansions = network.expansions.filter(
    (expansion) =>
      expansion.targets.includes(system) || expansion.next_system === system,
  );
  const component = componentSize(
    system,
    network.relay_edges,
    relays.length > 0,
  );
  return (
    <div className="inspector-intelligence-group">
      <h4>FTL network</h4>
      <InspectorFields
        fields={[
          { label: "Active relay nodes", value: relays.length },
          { label: "Staged relay devices", value: staged.length },
          { label: "Direct mesh neighbors", value: neighbors.length },
          {
            label: "Mesh component",
            value: component ? `${String(component)} systems` : "Not connected",
          },
        ]}
      />
      {relays.length || staged.length ? (
        <ul className="inspector-context-list">
          {[...relays, ...staged].map((relay) => (
            <li key={relay.entity.id}>
              <button
                type="button"
                className="inspector-context-link"
                onClick={() => {
                  onNavigate("device", relay.entity.id);
                }}
              >
                <strong>{relay.device_type ?? relay.entity.id}</strong>
                <small>
                  {relay.entity.id} · {relay.status ?? "unknown"}
                </small>
              </button>
            </li>
          ))}
        </ul>
      ) : null}
      {neighbors.length ? (
        <div className="inspector-context-subgroup">
          <strong>Direct neighbors</strong>
          <div className="inspector-link-row">
            {neighbors.map((neighbor) => (
              <button
                key={neighbor}
                type="button"
                className="subtle-link"
                onClick={() => {
                  onNavigate("system", neighbor);
                }}
              >
                {neighbor}
              </button>
            ))}
          </div>
        </div>
      ) : null}
      {expansions.length ? (
        <div className="inspector-context-subgroup">
          <strong>Expansion work</strong>
          <ul className="inspector-context-list">
            {expansions.map((expansion) => (
              <li key={expansion.workflow.id}>
                <button
                  type="button"
                  className="inspector-context-link"
                  onClick={() => {
                    onNavigate("workflow", expansion.workflow.id);
                  }}
                >
                  <strong>{expansion.workflow.kind}</strong>
                  <small>
                    {expansion.phase} · {expansion.completed_stops}/
                    {expansion.total_stops ?? "?"} stops
                  </small>
                </button>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}

function distributionTarget(
  item: InventoryDistribution,
): { kind: string; id: string } | null {
  if (item.location) return { kind: "location", id: item.location };
  if (item.owner_kind === "replicant")
    return { kind: "replicant", id: item.owner };
  if (item.system) return { kind: "system", id: item.system };
  return null;
}

function InventoryContext({
  entity,
  summary,
  reservations,
  onNavigate,
}: {
  entity: SelectedEntity;
  summary: EntitySummary;
  reservations: WorkflowReservationSummary[];
  onNavigate: (kind: string, id: string) => void;
}) {
  const query = useDomainQuery({
    slice: "inventory",
    queryKey: "inspector:inventory",
    fetcher: (signal) => daemonApi.inventory(signal),
    isEmpty: (snapshot) =>
      !snapshot.locations.length && !snapshot.resources.length,
  });
  const inventory = query.data;
  if (!inventory) return null;
  if (entity.kind === "resource") {
    const resource = inventory.resources.find(
      (item) => item.resource === entity.id,
    );
    const committed = reservations
      .filter((reservation) => reservation.resource === entity.id)
      .reduce((sum, reservation) => sum + reservation.quantity, 0);
    const present = resource?.total_quantity ?? 0;
    const available = Math.max(0, present - committed);
    const commitments = reservations.filter(
      (reservation) => reservation.resource === entity.id,
    );
    return (
      <div className="inspector-intelligence-group">
        <h4>Inventory distribution</h4>
        <InspectorFields
          fields={[
            { label: "Resource", value: entity.id },
            { label: "Present", value: present },
            { label: "Reserved", value: committed },
            { label: "Available", value: available },
            {
              label: "Storage scopes",
              value: resource?.distribution.length ?? 0,
            },
          ]}
        />
        {commitments.length ? (
          <div className="inspector-context-subgroup">
            <strong>Active commitments</strong>
            <ul className="inspector-context-list">
              {commitments.map((reservation) => (
                <li key={reservation.allocation_id}>
                  <button
                    type="button"
                    className="inspector-context-link"
                    onClick={() => {
                      onNavigate("workflow", reservation.workflow_id);
                    }}
                  >
                    <strong>
                      {reservation.quantity.toLocaleString()} reserved
                    </strong>
                    <small>
                      {reservation.requirement_key}
                      {reservation.location ? ` · ${reservation.location}` : ""}
                    </small>
                  </button>
                </li>
              ))}
            </ul>
          </div>
        ) : null}
        {resource?.distribution.length ? (
          <ul className="inspector-context-list">
            {[...resource.distribution]
              .sort((left, right) => right.quantity - left.quantity)
              .slice(0, 10)
              .map((item) => {
                const target = distributionTarget(item);
                return (
                  <li
                    key={`${item.owner_kind}:${item.owner}:${item.location ?? ""}`}
                  >
                    <button
                      type="button"
                      className="inspector-context-link"
                      disabled={!target}
                      onClick={() => {
                        if (target) onNavigate(target.kind, target.id);
                      }}
                    >
                      <strong>
                        {item.location ?? item.system ?? item.owner}
                      </strong>
                      <small>
                        {item.quantity.toLocaleString()} units ·{" "}
                        {item.owner_kind}
                      </small>
                    </button>
                  </li>
                );
              })}
          </ul>
        ) : (
          <p className="empty-state">No positive managed inventory.</p>
        )}
      </div>
    );
  }

  let rows: InventoryLocationSummary[] = [];
  if (entity.kind === "system")
    rows = inventory.locations.filter((row) => row.system === entity.id);
  else if (entity.kind === "location")
    rows = inventory.locations.filter((row) => row.location === entity.id);
  else if (entity.kind === "replicant")
    rows = inventory.locations.filter(
      (row) => row.owner_kind === "replicant" && row.owner === entity.id,
    );
  else if (entity.kind === "event")
    rows = inventory.locations.filter(
      (row) =>
        (summary.location !== null && row.location === summary.location) ||
        (summary.location === null &&
          summary.system !== null &&
          row.system === summary.system),
    );
  if (!rows.length) {
    return (
      <div className="inspector-intelligence-group">
        <h4>Inventory</h4>
        <p className="empty-state">
          No positive managed inventory in this scope.
        </p>
      </div>
    );
  }
  const resources = new Map<string, number>();
  for (const row of rows) {
    for (const item of row.resources)
      resources.set(
        item.resource,
        (resources.get(item.resource) ?? 0) + item.quantity,
      );
  }
  const total = rows.reduce((sum, row) => sum + row.total_quantity, 0);
  const ranked = [...resources.entries()].sort(
    (left, right) => right[1] - left[1],
  );
  return (
    <div className="inspector-intelligence-group">
      <h4>Inventory</h4>
      <InspectorFields
        fields={[
          { label: "Total units", value: total },
          { label: "Storage scopes", value: rows.length },
          { label: "Resource types", value: resources.size },
        ]}
      />
      <ul className="inspector-context-list">
        {ranked.slice(0, 10).map(([resource, quantity]) => (
          <li key={resource}>
            <button
              type="button"
              className="inspector-context-link"
              onClick={() => {
                onNavigate("resource", resource);
              }}
            >
              <strong>{resource}</strong>
              <small>{quantity.toLocaleString()} units</small>
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

export function InspectorIntelligence({
  entity,
  summary,
  snapshot,
  workflowIds,
  workflowIntelligence,
  onNavigate,
}: {
  entity: SelectedEntity;
  summary: EntitySummary;
  snapshot?: EntityInspectorSnapshot;
  workflowIds: string[];
  workflowIntelligence?: WorkflowIntelligenceSnapshot;
  onNavigate: (kind: string, id: string) => void;
}) {
  const region = regionFor(snapshot);
  const system = targetSystem(entity, summary, snapshot);
  return (
    <div className="inspector-intelligence">
      <WorkflowOwnershipContext
        entity={entity}
        intelligence={workflowIntelligence}
        onNavigate={onNavigate}
      />
      <AutomationContext
        entity={entity}
        workflowIds={workflowIds}
        onNavigate={onNavigate}
      />
      {region || workflowIds.length ? (
        <DirectorContext
          region={region}
          workflowIds={workflowIds}
          onNavigate={onNavigate}
        />
      ) : null}
      {system && entitySupportsConnectivity(entity, snapshot) ? (
        <ConnectivityContext system={system} onNavigate={onNavigate} />
      ) : null}
      <ReservationContext
        entity={entity}
        intelligence={workflowIntelligence}
        onNavigate={onNavigate}
      />
      {entitySupportsInventory(entity) ? (
        <InventoryContext
          entity={entity}
          summary={summary}
          reservations={workflowIntelligence?.reservations ?? []}
          onNavigate={onNavigate}
        />
      ) : null}
    </div>
  );
}
