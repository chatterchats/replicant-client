import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it, vi } from "vitest";

import {
  BobNetContent,
  channelMessages,
  normalizeBobnetChannel,
} from "./BobNetPage";
import { LeaderboardsContent } from "./LeaderboardsPage";
import { filterInboxMessages, MessagesContent } from "./MessagesPage";
import { NetworkContent } from "./NetworkPage";
import { ReportsContent } from "./ReportsPage";
import { StandingContent } from "./StandingPage";
import type {
  BobnetSnapshot,
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

  it("keeps the Messages page focused on the account inbox", () => {
    const data: MessagesSnapshot = {
      metadata,
      inbox: [
        {
          id: 1,
          title: "Notice",
          body: "Signal received",
          category: "system",
          message_type: "system",
          is_read: false,
          created_at: null,
        },
      ],
      unread_count: 1,
    };
    const html = renderToStaticMarkup(
      <MessagesContent {...common} data={data} />,
    );
    expect(html).toContain("Signal received");
    expect(html).not.toContain("Relay history");
  });

  it("renders BobNet as channel chat with sender selection", () => {
    const data: BobnetSnapshot = {
      metadata,
      sources: [device],
      selected_source: "RELAY-1",
      channels: [{ name: "#general", last_active: null }],
      messages: [
        {
          id: 1,
          channel: "#general",
          body: "Signal received",
          sender: null,
          sender_name: null,
          is_npc_or_system: true,
          current_system: null,
          created_at: "2026-08-16T12:00:00Z",
        },
      ],
      replicants: [
        {
          entity: { kind: "replicant", id: "R-1" },
          name: "Ada",
          status: "active",
          location: "SOL-1",
        },
      ],
      next_cursor: null,
      total_messages: 1,
      error: null,
    };
    const html = renderToStaticMarkup(
      <BobNetContent
        {...common}
        data={data}
        includeNpcs
        onIncludeNpcsChange={vi.fn()}
        onSelectEntity={vi.fn()}
      />,
    );
    expect(html).toContain("#general");
    expect(html).toContain("Signal received");
    expect(html).toContain("Ada");
    expect(html).not.toContain("History source");
    expect(html).not.toContain("Inspect history source");

    const playersOnly = renderToStaticMarkup(
      <BobNetContent
        {...common}
        data={data}
        includeNpcs={false}
        onIncludeNpcsChange={vi.fn()}
        onSelectEntity={vi.fn()}
      />,
    );
    expect(playersOnly).not.toContain("Signal received");
    expect(playersOnly).toContain("Include NPC / system chatter");
    expect(normalizeBobnetChannel("general")).toBe("#general");
    expect(
      channelMessages(
        [
          { ...data.messages[0]!, id: 2, created_at: "2026-08-16T12:02:00Z" },
          { ...data.messages[0]!, id: 1, created_at: "2026-08-16T12:01:00Z" },
        ],
        "general",
      ).map((message) => message.id),
    ).toEqual([1, 2]);
  });

  it("sorts inbox newest-first and filters by type, unread state, and text", () => {
    const messages = [
      {
        id: 1,
        title: "Old system notice",
        body: "Nothing urgent",
        category: "system",
        message_type: "system",
        is_read: true,
        created_at: "2026-08-14T12:00:00Z",
      },
      {
        id: 2,
        title: "Delivery alert",
        body: "Conductive delivery is ready",
        category: "mission",
        message_type: "mission",
        is_read: false,
        created_at: "2026-08-16T12:00:00Z",
      },
      {
        id: 3,
        title: "System update",
        body: "Relay maintenance",
        category: "system",
        message_type: "system",
        is_read: false,
        created_at: "2026-08-15T12:00:00Z",
      },
    ];

    expect(
      filterInboxMessages(messages, "", "", false).map((message) => message.id),
    ).toEqual([2, 3, 1]);
    expect(
      filterInboxMessages(messages, "conductive", "mission", true).map(
        (message) => message.id,
      ),
    ).toEqual([2]);
    expect(
      filterInboxMessages(messages, "relay", "system", true).map(
        (message) => message.id,
      ),
    ).toEqual([3]);
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
        <MessagesContent {...common} status="error" error="offline" />,
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
