/* eslint-disable react-refresh/only-export-components */
import { useMemo, useState } from "react";

import { daemonApi } from "./api";
import { descriptorCommands, type DescriptorCommand } from "./CommandPalette";
import { type DomainQueryStatus, useDomainQuery } from "./domainQuery";
import type {
  BillFinderResponse,
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

const withParameters = (
  operations: DescriptorCommand[],
  kind: string,
  initialParameters: Record<string, unknown>,
): DescriptorCommand | undefined => {
  const operation = operations.find((item) => item.descriptor.kind === kind);
  return operation ? { ...operation, initialParameters } : undefined;
};

function BillFinderPanel({
  onOpenSystem,
  onSelectWorkflow,
}: {
  onOpenSystem: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
}) {
  const [trackingBeacon, setTrackingBeacon] = useState("");
  const [expandOnFind, setExpandOnFind] = useState(false);
  const [finding, setFinding] = useState(false);
  const [result, setResult] = useState<BillFinderResponse>();
  const [finderError, setFinderError] = useState<string | null>(null);

  const runFinder = async (targetSystem?: string) => {
    setFinding(true);
    setFinderError(null);
    try {
      const next = await daemonApi.findBill({
        tracking_beacon: trackingBeacon.trim() || null,
        expand: targetSystem ? true : expandOnFind,
        target_system: targetSystem ?? null,
      });
      setResult(next);
    } catch (error) {
      setFinderError(error instanceof Error ? error.message : String(error));
    } finally {
      setFinding(false);
    }
  };

  const expansionWorkflowId = result?.expansion.workflow?.id ?? null;

  return (
    <section className="connection-card bill-finder">
      <header className="page-heading">
        <div>
          <h2>Find Bill&apos;s Skunkworks</h2>
          <p>
            Read Bill&apos;s latest beacon departure vector and rank known star
            systems lying along that ray.
          </p>
        </div>
        {result && (
          <span className="status-chip">{result.confidence} confidence</span>
        )}
      </header>
      <div className="bill-finder-controls">
        <label>
          Tracking FTL beacon
          <input
            value={trackingBeacon}
            onChange={(event) => {
              setTrackingBeacon(event.target.value);
            }}
            placeholder="Auto-select monitoring SOL beacon"
          />
        </label>
        <label className="bill-finder-toggle">
          <input
            type="checkbox"
            checked={expandOnFind}
            onChange={(event) => {
              setExpandOnFind(event.target.checked);
            }}
          />
          Establish FTL connectivity when the result is unambiguous
        </label>
        <button disabled={finding} onClick={() => void runFinder()}>
          {finding ? "Tracking Bill…" : "Find Bill"}
        </button>
      </div>
      <p className="table-summary">
        Leaving the tracker blank auto-selects an owned monitoring beacon in
        SOL. If Bill departs from an event system instead, enter a beacon
        deployed in that system.
      </p>
      {finderError && <p className="inline-warning">{finderError}</p>}
      {result && (
        <>
          <div className="bill-departure-summary">
            <div>
              <span>Departure</span>
              <strong>{result.departure.origin_location}</strong>
            </div>
            <div>
              <span>Vector</span>
              <strong>
                {result.departure.vector
                  .map((value) => value.toFixed(3))
                  .join(", ")}
              </strong>
            </div>
            <div>
              <span>Observed</span>
              <strong>{result.departure.logged_at ?? "Unknown"}</strong>
            </div>
            <div>
              <span>Likely system</span>
              <strong>{result.recommended_system ?? "Ambiguous"}</strong>
            </div>
          </div>
          <p className={result.ambiguous ? "inline-warning" : "table-summary"}>
            {result.ambiguous
              ? "Several known systems fit the departure ray closely. Select the intended candidate before starting FTL expansion."
              : `Best catalogue match: ${result.recommended_system ?? "none"}.`}
          </p>
          {result.expansion.status !== "not_requested" && (
            <div className="bill-expansion-result">
              <strong>FTL expansion · {result.expansion.status}</strong>
              <span>{result.expansion.message}</span>
              {expansionWorkflowId && (
                <button
                  onClick={() => {
                    onSelectWorkflow(expansionWorkflowId);
                  }}
                >
                  Open workflow
                </button>
              )}
            </div>
          )}
          <div className="inventory-table-wrap">
            <table className="inventory-table">
              <thead>
                <tr>
                  <th>Candidate</th>
                  <th>Angle</th>
                  <th>Distance</th>
                  <th>Miss distance</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {result.candidates.map((candidate, index) => (
                  <tr key={candidate.system}>
                    <td>
                      <strong>
                        {index + 1}. {candidate.system}
                      </strong>
                      {candidate.system === result.recommended_system && (
                        <small>recommended</small>
                      )}
                    </td>
                    <td>{candidate.angular_error_deg.toFixed(3)}°</td>
                    <td>{candidate.distance_ly.toFixed(2)} LY</td>
                    <td>{candidate.cross_track_ly.toFixed(2)} LY</td>
                    <td>
                      <button
                        onClick={() => {
                          onOpenSystem(candidate.system);
                        }}
                      >
                        Open System
                      </button>
                      <button
                        disabled={finding}
                        onClick={() => void runFinder(candidate.system)}
                      >
                        Expand FTL
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </>
      )}
    </section>
  );
}

export function TradePage(props: {
  descriptors: DescriptorCatalog;
  onSelectEntity: (entity: EntityRef) => void;
  onOpenSystem: (system: string) => void;
  onSelectWorkflow: (id: string) => void;
  onRunCommand: (command: DescriptorCommand) => void;
}) {
  const query = useDomainQuery({
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
      <BillFinderPanel
        onOpenSystem={onOpenSystem}
        onSelectWorkflow={onSelectWorkflow}
      />
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
        controllers.map((controller) => {
          const createTrade = controller.is_local
            ? withParameters(operations, "trade.create", {
                controller: controller.entity.id,
              })
            : undefined;
          const configureShop = controller.is_local
            ? withParameters(operations, "trade.configure_shop", {
                controller: controller.entity.id,
              })
            : undefined;
          return (
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
                {createTrade && (
                  <button
                    onClick={() => {
                      onRunCommand(createTrade);
                    }}
                  >
                    Create trade
                  </button>
                )}
                {configureShop && (
                  <button
                    onClick={() => {
                      onRunCommand(configureShop);
                    }}
                  >
                    Configure shop
                  </button>
                )}
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
              {controller.trade_details_status === "out_of_comms" ? (
                <p className="empty-state">
                  Trade details are out of comms. Travel to this shop system or
                  establish an FTL beacon or relay there to inspect its stock.
                </p>
              ) : controller.trade_details_status !== "available" ? (
                <p className="empty-state">
                  Trade details are temporarily unavailable. The shop remains
                  visible in the network directory.
                </p>
              ) : !controller.trades.length ? (
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
                        <th>Actions</th>
                      </tr>
                    </thead>
                    <tbody>
                      {controller.trades.map((trade) => {
                        const buyTrade = withParameters(
                          operations,
                          "trade.execute",
                          {
                            controller: controller.entity.id,
                            trade_code: trade.trade_code,
                          },
                        );
                        const fulfillTrade = controller.location
                          ? withParameters(operations, "trade.fulfillment", {
                              controller: controller.entity.id,
                              trade_code: trade.trade_code,
                              shop_location: controller.location,
                            })
                          : undefined;
                        const deleteTrade = controller.is_local
                          ? withParameters(operations, "trade.delete", {
                              controller: controller.entity.id,
                              trade_code: trade.trade_code,
                            })
                          : undefined;
                        return (
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
                            <td>
                              <div className="trade-row-actions">
                                {buyTrade && (
                                  <button
                                    onClick={() => {
                                      onRunCommand(buyTrade);
                                    }}
                                    title="Execute immediately with already-staged trade criteria"
                                  >
                                    Buy
                                  </button>
                                )}
                                {fulfillTrade && (
                                  <button
                                    className="primary"
                                    onClick={() => {
                                      onRunCommand(fulfillTrade);
                                    }}
                                    title="Provision the trade, deliver buyer and cargo, buy, secure rewards, and return home"
                                  >
                                    Provision &amp; Buy
                                  </button>
                                )}
                                {deleteTrade && (
                                  <button
                                    onClick={() => {
                                      onRunCommand(deleteTrade);
                                    }}
                                  >
                                    Delete
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
            </section>
          );
        })
      )}
      <p className="table-summary">
        Viewer {data?.viewer?.id ?? "—"} · revision{" "}
        {data?.metadata.revision ?? "—"}
      </p>
    </article>
  );
}
