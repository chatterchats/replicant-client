import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import { LeaderboardsContent } from "./LeaderboardsPage";
import { MessagesContent } from "./MessagesPage";
import { NetworkContent } from "./NetworkPage";
import { ReportsContent } from "./ReportsPage";
import { StandingContent } from "./StandingPage";
import type {
  LeaderboardsSnapshot,
  MessagesSnapshot,
  NetworkSnapshot,
  ReportsSnapshot,
  StandingSnapshot,
} from "./protocol";

const metadata = { revision: 1, generated_at_ms: 2 };
const common = {
  status: "loaded" as const,
  error: null,
  refreshing: false,
  refresh: vi.fn(),
};
const device = {
  entity: { kind: "device" as const, id: "RELAY-1" },
  device_type: "relay",
  status: "active",
  ownership: "owned",
  owner: null,
  owner_name: null,
  system: "SOL",
  location: "SOL-1",
  tags: [],
  attached_to: null,
  stowed_in: null,
  controller: null,
  linked_device: null,
  attached_devices: [],
  controlled_devices: [],
  stowed_devices: [],
  attach_capacity: null,
  cargo_capacity: null,
  cargo_used: null,
  operational_capacity_percent: null,
  active_directive: null,
  directive_status: null,
  travel_destination: null,
  claim: null,
};

describe("Intelligence pages", () => {
  it("renders descriptor-driven reports and recent executions", () => {
    const data: ReportsSnapshot = {
      metadata,
      reports: [
        {
          kind: "fleet.summary",
          display_name: "Fleet summary",
          aliases: [],
          description: "Summarize the managed fleet",
          category: "fleet",
          operation_class: "report",
          risk: "none",
          applicable_to: [],
          parameters: [],
        },
      ],
      executions: [],
    };
    const html = renderToStaticMarkup(
      <ReportsContent
        {...common}
        data={data}
        entities={{}}
        onSelectEntity={vi.fn()}
      />,
    );
    expect(html).toContain("Fleet summary");
    expect(html).toContain("Run report");
  });

  it("renders inbox and relay history with sender context", () => {
    const data: MessagesSnapshot = {
      metadata,
      relays: [device.entity],
      selected_relay: "RELAY-1",
      channels: [{ name: "general", last_active: null }],
      relay_messages: [
        {
          id: 1,
          channel: "general",
          body: "Signal received",
          sender: null,
          sender_name: null,
          is_npc_or_system: true,
          current_system: null,
          created_at: null,
        },
      ],
      inbox: [],
      unread_count: 0,
      next_cursor: null,
    };
    const html = renderToStaticMarkup(
      <MessagesContent
        {...common}
        data={data}
        onRelayChange={vi.fn()}
        onSelectEntity={vi.fn()}
      />,
    );
    expect(html).toContain("Signal received");
    expect(html).toContain("NPC / system");
  });

  it("renders actual account and relay network status", () => {
    const data: NetworkSnapshot = {
      metadata,
      account_name: "Operator",
      account_status: "active",
      subscribed_channels: ["general"],
      replicants: [],
      relays: [{ device, channels: [], error: null }],
    };
    const html = renderToStaticMarkup(
      <NetworkContent {...common} data={data} onSelectEntity={vi.fn()} />,
    );
    expect(html).toContain("Operator");
    expect(html).toContain("RELAY-1");
    expect(html).toContain("not a social graph");
  });

  it("renders XP, achievements, and reputation", () => {
    const data: StandingSnapshot = {
      metadata,
      experience_points_total: 42,
      civilisation_points: null,
      achievements: [
        {
          key: "first-flight",
          title: "First flight",
          description: null,
          category: "travel",
          xp_reward: 5,
          achieved_at: null,
        },
      ],
      reputation: [],
    };
    const html = renderToStaticMarkup(
      <StandingContent {...common} data={data} />,
    );
    expect(html).toContain("First flight");
    expect(html).toContain("Not exposed");
  });

  it("renders leaderboard rows and empty/error/loading states", () => {
    const data: LeaderboardsSnapshot = {
      metadata,
      boards: [{ key: "xp", name: "XP", description: null, board_type: null }],
      selected_board: "xp",
      entries: [
        {
          rank: 1,
          replicant: { kind: "replicant", id: "R-1" },
          name: "Ada",
          designation: null,
          value: 100,
          contribution_count: null,
        },
      ],
    };
    expect(
      renderToStaticMarkup(
        <LeaderboardsContent
          {...common}
          data={data}
          onBoardChange={vi.fn()}
          onSelectEntity={vi.fn()}
        />,
      ),
    ).toContain("Ada");
    expect(
      renderToStaticMarkup(
        <ReportsContent
          {...common}
          status="loading"
          entities={{}}
          onSelectEntity={vi.fn()}
        />,
      ),
    ).toContain("Loading Reports");
    expect(
      renderToStaticMarkup(
        <MessagesContent
          {...common}
          status="error"
          error="offline"
          onRelayChange={vi.fn()}
          onSelectEntity={vi.fn()}
        />,
      ),
    ).toContain("Messages unavailable");
    expect(
      renderToStaticMarkup(
        <LeaderboardsContent
          {...common}
          data={{ ...data, boards: [], entries: [], selected_board: null }}
          status="empty"
          onBoardChange={vi.fn()}
          onSelectEntity={vi.fn()}
        />,
      ),
    ).toContain("No published leaderboards");
  });
});
