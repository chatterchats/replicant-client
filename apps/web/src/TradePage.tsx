/* eslint-disable react-refresh/only-export-components */
import { useMemo, useState } from "react";

import { daemonApi } from "./api";
import { descriptorCommands, type DescriptorCommand } from "./CommandPalette";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  DescriptorCatalog,
  EntityRef,
  TradeItemSummary,
  TradeSnapshot,
} from "./protocol";

const empty = (snapshot: TradeSnapshot) => snapshot.controllers.length === 0;

export const tradeCommands = (descriptors: DescriptorCatalog) =>
  descriptorCommands(descriptors).filter((command) =>
    /trade/i.test(
      [
        command.descriptor.kind,
        command.descriptor.category,
        command.descriptor.display_name,
      ].join(" "),
    ),
  );

const exchange = (items: TradeItemSummary[]) =>
  items.length
    ? items
        .map((item) => String(item.quantity ?? "?") + " " + item.item)
        .join(" + ")
    : "Nothing specified";

export function TradePage(props: {
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
    slice: "trade",
    fetcher: (signal) => daemonApi.trade(signal),
    isEmpty: empty,
  });
  return <TradeContent {...query} {...props} />;
}

export function TradeContent({
  data,
  status,
  error,
  refreshing,
  refresh,
  descriptors,
  onSelectEntity,
  onOpenSystem,
  onSelectWorkflow,
  onRunCommand,
}: {
  data?: TradeSnapshot;
  status: DomainQueryStatus;
  error: string | null;
  refreshing: boolean;
  refresh: () => Promise<void>;
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const [search, setSearch] = useState("");
  const operations = useMemo(() => tradeCommands(descriptors), [descriptors]);
  const needle = search.toLowerCase();
  const controllers = (data?.controllers ?? [])
    .map((controller) => {
      const controllerMatches = [
        controller.entity.id,
        controller.shop_name,
        controller.owner_name,
        controller.owner_replicant,
        controller.system,
        controller.location,
      ].some((value) => value?.toLowerCase().includes(needle));
      return {
        ...controller,
        trades: controller.trades.filter(
          (trade) =>
            controllerMatches ||
            [
              trade.trade_code,
              trade.name,
              exchange(trade.requested),
              exchange(trade.offered),
            ].some((value) => value?.toLowerCase().includes(needle)),
        ),
        matches: controllerMatches,
      };
    })
    .filter((controller) => controller.matches || controller.trades.length);
  if (!data && status === "loading")
    return <article className="page loading-state">Loading Trade…</article>;
  if (!data && status === "error")
    return (
      <article className="page error-state">
        <h1>Trade unavailable</h1>
        <p>{error}</p>
        <button onClick={() => void refresh()}>Retry</button>
      </article>
    );

  return (
    <article className="page asset-dashboard">
      <header className="page-heading">
        <div>
          <p className="eyebrow">Missions</p>
          <h1>Trade</h1>
          <p className="lede">
            Managed player shops and current exchanges visible to this account.
          </p>
        </div>
        <button disabled={refreshing} onClick={() => void refresh()}>
          {refreshing ? "Refreshing…" : "Refresh"}
        </button>
      </header>
      {error && <p className="inline-warning">Refresh failed: {error}</p>}
      <section className="asset-operations" aria-label="Trade operations">
        <h2>Operations</h2>
        {operations.length ? (
          operations.map((command) => (
            <button
              key={command.operationClass + ":" + command.descriptor.kind}
              onClick={() => {
                onRunCommand(command);
              }}
            >
              {command.descriptor.display_name}
            </button>
          ))
        ) : (
          <p>No trade operation is registered.</p>
        )}
      </section>
      <label className="inventory-search">
        Search controllers and trades
        <input
          type="search"
          value={search}
          onChange={(event) => {
            setSearch(event.target.value);
          }}
          placeholder="Shop, owner, trade, item, or location"
        />
      </label>
      {!data?.controllers.length ? (
        <section className="empty-state">No visible trade controllers.</section>
      ) : !controllers.length ? (
        <section className="empty-state">No trades match the search.</section>
      ) : (
        controllers.map((controller) => (
          <section
            key={controller.entity.id}
            className="connection-card trade-controller"
          >
            <header className="page-heading">
              <div>
                <h2>{controller.shop_name ?? controller.entity.id}</h2>
                <p>
                  {controller.owner_name ??
                    controller.owner_replicant ??
                    "Unknown owner"}
                  {" · "}
                  {controller.location ??
                    controller.system ??
                    "Hidden location"}
                </p>
              </div>
              <span className="status-chip">
                {controller.is_local ? "local" : "network"}
              </span>
            </header>
            <div className="asset-operations">
              <button
                onClick={() => {
                  onSelectEntity(controller.entity);
                }}
              >
                Inspect
              </button>
              {controller.system && (
                <button
                  onClick={() => {
                    if (controller.system) onOpenSystem(controller.system);
                  }}
                >
                  Open System
                </button>
              )}
              {controller.workflow && (
                <button
                  onClick={() => {
                    if (controller.workflow)
                      onSelectWorkflow(controller.workflow.id);
                  }}
                >
                  Workflow · {controller.workflow.status}
                </button>
              )}
            </div>
            {!controller.trades.length ? (
              <p className="empty-state">No current trades.</p>
            ) : (
              <div className="inventory-table-wrap">
                <table className="inventory-table">
                  <thead>
                    <tr>
                      <th>Trade</th>
                      <th>Buyer gives</th>
                      <th>Buyer receives</th>
                      <th>Stock</th>
                    </tr>
                  </thead>
                  <tbody>
                    {controller.trades.map((trade) => (
                      <tr key={trade.trade_code}>
                        <td>
                          <strong>{trade.name ?? trade.trade_code}</strong>
                          <small>{trade.trade_code}</small>
                        </td>
                        <td>{exchange(trade.requested)}</td>
                        <td>{exchange(trade.offered)}</td>
                        <td>
                          {trade.current_stock ?? "?"}
                          {trade.initial_stock !== null &&
                            " / " + String(trade.initial_stock)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </section>
        ))
      )}
      <p className="table-summary">
        Viewer {data?.viewer?.id ?? "—"} · revision{" "}
        {data?.metadata.revision ?? "—"}
      </p>
    </article>
  );
}
