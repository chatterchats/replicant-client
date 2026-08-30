/* eslint-disable react-refresh/only-export-components */
import { useMemo } from "react";

import { daemonApi } from "./api";
import { descriptorCommands, type DescriptorCommand } from "./CommandPalette";
import { useDomainQuery } from "./domainQuery";
import type { DescriptorCatalog, DeviceSummary, EntityRef } from "./protocol";

export function effectiveDeviceLocation(
  device: DeviceSummary,
  devicesByCode: Map<string, DeviceSummary>,
): string | null {
  let current = device;
  const visited = new Set<string>();
  while (!visited.has(current.entity.id)) {
    visited.add(current.entity.id);
    const parentCode: string | null = current.stowed_in ?? current.attached_to;
    const parent: DeviceSummary | undefined =
      parentCode === null ? undefined : devicesByCode.get(parentCode);
    if (parent) {
      current = parent;
      continue;
    }
    if (current.location) return current.location;
    if (current.system) return current.system;
    break;
  }
  return device.system;
}

function sameLocation(
  left: DeviceSummary,
  right: DeviceSummary,
  devicesByCode: Map<string, DeviceSummary>,
): boolean {
  const leftLocation = effectiveDeviceLocation(left, devicesByCode);
  const rightLocation = effectiveDeviceLocation(right, devicesByCode);
  return Boolean(leftLocation && leftLocation === rightLocation);
}

export function CloningPage({
  descriptors,
  onSelectEntity,
  onRunCommand,
}: {
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "devices",
    queryKey: "devices",
    fetcher: (signal) => daemonApi.devices(signal),
    isEmpty: (snapshot) =>
      !snapshot.devices.some(
        (device) => device.device_type === "empty_replicant_matrix",
      ),
  });
  const commands = useMemo(
    () => descriptorCommands(descriptors),
    [descriptors],
  );
  const stowTarget = commands.find(
    (command) => command.descriptor.kind === "clone.stow_target",
  );
  const replicate = commands.find(
    (command) => command.descriptor.kind === "clone.replicate",
  );
  const snapshotDevices = query.data?.devices;
  const devices = useMemo(() => snapshotDevices ?? [], [snapshotDevices]);
  const devicesByCode = useMemo(
    () => new Map(devices.map((device) => [device.entity.id, device])),
    [devices],
  );
  const matrices = devices.filter(
    (device) => device.device_type === "empty_replicant_matrix",
  );
  const sourceMatrices = devices.filter(
    (device) => device.device_type === "replicant_matrix",
  );

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Operations</p>
          <h1>Cloning</h1>
          <p className="lede">
            Prepare an empty matrix in a cradle, then replicate from a source
            replicant matrix at the same location.
          </p>
        </div>
        <button
          disabled={query.refreshing}
          onClick={() => void query.refresh()}
        >
          {query.refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {query.error && <p className="inline-warning">{query.error}</p>}
      {!matrices.length ? (
        <section className="empty-state">
          No empty replicant matrices are currently visible. Print one from an
          Autofactory before completing the clone flow.
        </section>
      ) : (
        <div className="inventory-table-wrap">
          <table className="inventory-table">
            <thead>
              <tr>
                <th>Target matrix</th>
                <th>Status</th>
                <th>Location</th>
                <th>Cradle</th>
                <th>Next step</th>
              </tr>
            </thead>
            <tbody>
              {matrices.map((matrix) => {
                const localSources = sourceMatrices.filter((source) =>
                  sameLocation(source, matrix, devicesByCode),
                );
                const matrixLocation = effectiveDeviceLocation(
                  matrix,
                  devicesByCode,
                );
                return (
                  <tr key={matrix.entity.id}>
                    <td>
                      <button
                        className="link-button"
                        onClick={() => {
                          onSelectEntity(matrix.entity);
                        }}
                      >
                        {matrix.entity.id}
                      </button>
                    </td>
                    <td>{matrix.status ?? "unknown"}</td>
                    <td>{matrixLocation ?? "—"}</td>
                    <td>{matrix.stowed_in ?? "Not stowed"}</td>
                    <td>
                      <div className="asset-operations">
                        {!matrix.stowed_in && stowTarget && (
                          <button
                            onClick={() => {
                              onRunCommand({
                                ...stowTarget,
                                initialParameters: { matrix: matrix.entity.id },
                              });
                            }}
                          >
                            Stow in cradle
                          </button>
                        )}
                        {matrix.stowed_in &&
                        replicate &&
                        localSources.length > 0
                          ? localSources.map((source) => (
                              <button
                                key={source.entity.id}
                                onClick={() => {
                                  onRunCommand({
                                    ...replicate,
                                    initialParameters: {
                                      source: source.entity.id,
                                      target: matrix.entity.id,
                                    },
                                  });
                                }}
                              >
                                Clone from{" "}
                                {source.owner_name ?? source.entity.id}
                              </button>
                            ))
                          : matrix.stowed_in &&
                            replicate && (
                              <button
                                onClick={() => {
                                  onRunCommand({
                                    ...replicate,
                                    initialParameters: {
                                      target: matrix.entity.id,
                                    },
                                  });
                                }}
                              >
                                Choose source matrix
                              </button>
                            )}
                      </div>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}
      {sourceMatrices.length > 0 && (
        <section className="asset-jobs">
          <h2>Source replicant matrices</h2>
          <p className="lede">
            Replication uses the source replicant&apos;s parent vessel location
            when its stowed matrix does not report a location of its own.
          </p>
          <div className="asset-operations">
            {sourceMatrices.map((source) => (
              <button
                key={source.entity.id}
                onClick={() => {
                  onSelectEntity(source.entity);
                }}
              >
                {source.owner_name ?? source.owner ?? source.entity.id} ·{" "}
                {effectiveDeviceLocation(source, devicesByCode) ??
                  "unknown location"}
              </button>
            ))}
          </div>
        </section>
      )}
    </article>
  );
}
