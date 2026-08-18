// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App } from "./App";
import { DaemonProvider } from "./daemon";
import type {
  AccountEventsSnapshot,
  AutofactorySnapshot,
  BlueprintsSnapshot,
  AutomationControlResponse,
  BobnetSnapshot,
  BootstrapSnapshot,
  CargoSnapshot,
  DaemonHealth,
  DescriptorCatalog,
  DevicesSnapshot,
  DirectorSnapshot,
  DirectorySnapshot,
  EntityIndexSnapshot,
  EventsSnapshot,
  GalaxySceneSnapshot,
  InventorySnapshot,
  LeaderboardsSnapshot,
  MessagesSnapshot,
  MiningSnapshot,
  NetworkSnapshot,
  OverviewSnapshot,
  RelaySnapshot,
  ReportsSnapshot,
  RuntimeSnapshot,
  SettingsSnapshot,
  SimulationsSnapshot,
  StandingSnapshot,
  SurveySnapshot,
  SystemSceneSnapshot,
  TradeSnapshot,
  TutorialsSnapshot,
} from "./protocol";

const metadata = { revision: 1, generated_at_ms: 1 };
const health: DaemonHealth = {
  status: "healthy",
  daemon_version: "test",
  detail: null,
};
const sync = {
  phase: "ready" as const,
  revision: 1,
  last_event_at_ms: null,
  detail: null,
};
const automation = {
  automatic_triggers_enabled: true,
  workflows_paused: false,
};
const snapshot: RuntimeSnapshot = {
  metadata,
  sync,
  automation,
  workflows: [],
  requirements: [],
  notifications: [],
  slice_revisions: {},
};
const entities: EntityIndexSnapshot = { metadata, entities: [] };
const descriptors: DescriptorCatalog = {
  reports: [],
  actions: [],
  workflows: [],
};
const overview: OverviewSnapshot = {
  metadata,
  health,
  sync,
  automation,
  replicants: [],
  active_travel: [],
  active_workflows: [],
  workflow_counts: [],
  attention_workflows: [],
  notifications: [],
  recent_activity: [],
};
const devices: DevicesSnapshot = { metadata, devices: [] };
const activity: AccountEventsSnapshot = { metadata, cursor: null, events: [] };
const simulations: SimulationsSnapshot = {
  metadata,
  interfaces: [],
  managed_history: [],
  account_history: [],
};
const blueprints: BlueprintsSnapshot = { metadata, blueprints: [] };
const directory: DirectorySnapshot = { metadata, query: null, replicants: [] };
const tutorials: TutorialsSnapshot = {
  metadata,
  tutorials: [],
  selected: null,
};
const inventory: InventorySnapshot = {
  metadata,
  total_quantity: 0,
  locations: [],
  resources: [],
};
const autofactories: AutofactorySnapshot = {
  metadata,
  utilization: {
    total: 0,
    busy: 0,
    available: 0,
    unavailable: 0,
    queued_units: 0,
    utilization_percent: 0,
  },
  factories: [],
};
const cargo: CargoSnapshot = {
  metadata,
  cargo_used: 0,
  cargo_capacity: 0,
  attachment_used: 0,
  attachment_capacity: 0,
  carriers: [],
};
const survey: SurveySnapshot = { metadata, missions: [], fleet: [] };
const mining: MiningSnapshot = { metadata, installations: [], workflows: [] };
const relay: RelaySnapshot = {
  metadata,
  relays: [],
  staged_relays: [],
  connected_systems: 0,
  relay_edges: [],
  expansions: [],
};
const bootstrap: BootstrapSnapshot = { metadata, missions: [] };
const events: EventsSnapshot = { metadata, events: [] };
const trade: TradeSnapshot = { metadata, viewer: null, controllers: [] };
const reports: ReportsSnapshot = { metadata, reports: [], executions: [] };
const messages: MessagesSnapshot = {
  metadata,
  inbox: [],
  unread_count: 0,
};
const bobnet: BobnetSnapshot = {
  metadata,
  sources: [],
  selected_source: null,
  channels: [],
  messages: [],
  replicants: [],
  next_cursor: null,
  total_messages: null,
  error: null,
};
const network: NetworkSnapshot = {
  metadata,
  account_name: null,
  account_status: null,
  subscribed_channels: [],
  replicants: [],
  relays: [],
};
const standing: StandingSnapshot = {
  metadata,
  experience_points_total: null,
  civilisation_points: null,
  achievements: [],
  reputation: [],
};
const leaderboards: LeaderboardsSnapshot = {
  metadata,
  boards: [],
  selected_board: null,
  entries: [],
};
const settings: SettingsSnapshot = {
  metadata,
  profile: "test",
  bind_address: "127.0.0.1:8080",
  managed_database_path: "replicant-client.sqlite",
  runtime_database_path: "replicant-runtime.sqlite",
  log_filter: "info",
  docker: false,
  api_token_source: "environment",
  daemon_settings_require_restart: true,
};
const galaxyScene: GalaxySceneSnapshot = {
  revision: 1,
  generated_at_ms: 1,
  stars: [],
  relay_edges: [],
  active_travel: [],
  signals: [],
  highlights: [],
  overlays: [],
  workflow_targets: [],
};
const automationControlResponse: AutomationControlResponse = {
  automation,
  affected_workflows: 0,
};
const director: DirectorSnapshot = {
  metadata,
  mode: "advisory",
  regions: [],
  goals: [],
  replicants: [],
  workforce: {
    total: 0,
    busy: 0,
    idle: 0,
    idle_ratio: 0,
    pending_worker_demand: 0,
    scale_up_recommended: false,
    scale_reason: null,
  },
};

function systemScene(system: string): SystemSceneSnapshot {
  return {
    system,
    revision: 1,
    generated_at_ms: 1,
    markers: [],
    active_travel: [],
    workflow_markers: [],
  };
}

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    daemonApi: {
      health: () => Promise.resolve(health),
      snapshot: () => Promise.resolve(snapshot),
      entities: () => Promise.resolve(entities),
      descriptors: () => Promise.resolve(descriptors),
      overview: () => Promise.resolve(overview),
      devices: () => Promise.resolve(devices),
      activity: () => Promise.resolve(activity),
      simulations: () => Promise.resolve(simulations),
      blueprints: () => Promise.resolve(blueprints),
      directory: () => Promise.resolve(directory),
      tutorials: () => Promise.resolve(tutorials),
      inventory: () => Promise.resolve(inventory),
      autofactories: () => Promise.resolve(autofactories),
      cargo: () => Promise.resolve(cargo),
      survey: () => Promise.resolve(survey),
      mining: () => Promise.resolve(mining),
      relay: () => Promise.resolve(relay),
      bootstrap: () => Promise.resolve(bootstrap),
      events: () => Promise.resolve(events),
      trade: () => Promise.resolve(trade),
      reports: () => Promise.resolve(reports),
      messages: () => Promise.resolve(messages),
      bobnet: () => Promise.resolve(bobnet),
      network: () => Promise.resolve(network),
      standing: () => Promise.resolve(standing),
      leaderboards: () => Promise.resolve(leaderboards),
      settings: () => Promise.resolve(settings),
      galaxyScene: () => Promise.resolve(galaxyScene),
      systemScene: (system: string) => Promise.resolve(systemScene(system)),
      history: () => Promise.resolve([]),
      director: () => Promise.resolve(director),
      reconcileDirector: () => Promise.resolve(director),
      setDirectorMode: () => Promise.resolve(director),
      setDirectorGoal: () => Promise.resolve(director),
      assignDirectorReplicant: () => Promise.resolve(director),
      controlAutomation: () => Promise.resolve(automationControlResponse),
    } satisfies Partial<typeof actual.daemonApi>,
  };
});

class MockWebSocket {
  static readonly CONNECTING = 0;
  static readonly OPEN = 1;
  static readonly CLOSING = 2;
  static readonly CLOSED = 3;
  readyState = MockWebSocket.CONNECTING;
  constructor(public url: string) {}
  addEventListener() {
    // Never emits open/message/close; App runs on the initial snapshot only.
  }
  removeEventListener() {}
  close() {}
}

function flush() {
  return new Promise((resolve) => setTimeout(resolve, 0));
}

const destinations = [
  "Overview",
  "Galaxy",
  "System",
  "Observatory",
  "Cloning",
  "Devices",
  "Inventory",
  "Autofactory",
  "Cargo",
  "Blueprints",
  "Survey",
  "Mining",
  "Relay",
  "Galaxy Events",
  "Bootstrap",
  "Trade",
  "Simulations",
  "Automations",
  "Requirements",
  "History",
  "Activity",
  "Reports",
  "Messages",
  "BobNet",
  "Network",
  "Directory",
  "Standing",
  "Leaderboards",
  "Tutorials",
  "Settings",
];

const placeholderLede =
  "Live application state is synchronized through the local daemon.";

describe("App navigation", () => {
  let root: Root;
  let container: HTMLDivElement;

  beforeEach(() => {
    vi.stubGlobal("WebSocket", MockWebSocket);
    container = document.createElement("div");
    document.body.appendChild(container);
  });

  afterEach(() => {
    act(() => {
      root.unmount();
    });
    container.remove();
    vi.unstubAllGlobals();
  });

  it("renders every navigation destination's own page, never the generic placeholder", async () => {
    await act(async () => {
      root = createRoot(container);
      root.render(
        <DaemonProvider>
          <App />
        </DaemonProvider>,
      );
      await flush();
      await flush();
    });

    for (const destination of destinations) {
      const button = Array.from(
        container.querySelectorAll<HTMLButtonElement>(".sidebar button"),
      ).find((candidate) => candidate.textContent === destination);
      expect(
        button,
        `expected a sidebar button for ${destination}`,
      ).toBeTruthy();

      await act(async () => {
        button?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
        await flush();
        await flush();
      });

      expect(
        container.querySelector("h1")?.textContent,
        `${destination} should render its own page heading`,
      ).toBeTruthy();
      expect(container.textContent).not.toContain(placeholderLede);
    }
  });
});
