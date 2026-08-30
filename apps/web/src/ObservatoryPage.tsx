import { useMemo } from "react";

import { daemonApi } from "./api";
import { descriptorCommands, type DescriptorCommand } from "./CommandPalette";
import { useDomainQuery } from "./domainQuery";
import type { DescriptorCatalog, EntityRef } from "./protocol";

export function ObservatoryPage({
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
      !snapshot.devices.some((device) =>
        device.device_type?.includes("observatory"),
      ),
  });
  const operations = useMemo(
    () =>
      descriptorCommands(descriptors).filter(
        (item) => item.descriptor.category === "observatory",
      ),
    [descriptors],
  );
  const autoProspect = operations.find(
    (operation) => operation.descriptor.kind === "observatory.auto_prospect",
  );
  const observatories = (query.data?.devices ?? []).filter((device) =>
    device.device_type?.includes("observatory"),
  );

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Operations</p>
          <h1>Galactic Observatory</h1>
          <p className="lede">
            Prospect sparse space with the runtime density planner, use the
            server&apos;s outward scan, or triangulate a known spectral
            signature.
          </p>
        </div>
        <div className="asset-operations">
          {autoProspect && (
            <button
              onClick={() => {
                onRunCommand(autoProspect);
              }}
            >
              Auto-select &amp; prospect
            </button>
          )}
          <button
            disabled={query.refreshing}
            onClick={() => void query.refresh()}
          >
            {query.refreshing ? "Refreshing…" : "Refresh"}
          </button>
        </div>
      </header>
      {query.error && <p className="inline-warning">{query.error}</p>}
      {!observatories.length ? (
        <section className="empty-state">
          No owned Galactic Observatory devices discovered.
        </section>
      ) : (
        observatories.map((device) => (
          <section className="connection-card" key={device.entity.id}>
            <h2>{device.entity.id}</h2>
            <p>
              {device.status ?? "Unknown status"} ·{" "}
              {device.location ?? device.system ?? "Unknown location"}
            </p>
            <div className="asset-operations">
              <button
                onClick={() => {
                  onSelectEntity(device.entity);
                }}
              >
                Inspect
              </button>
              {operations.map((operation) => (
                <button
                  key={operation.descriptor.kind}
                  onClick={() => {
                    onRunCommand({
                      ...operation,
                      initialParameters: { device: device.entity.id },
                    });
                  }}
                >
                  {operation.descriptor.display_name}
                </button>
              ))}
            </div>
          </section>
        ))
      )}
    </article>
  );
}
