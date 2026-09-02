// @vitest-environment jsdom
import { act } from "react";
import { createRoot, type Root } from "react-dom/client";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { App, relatedDeviceLabel, specializeDeviceCommand } from "./App";
import type { DescriptorCommand } from "./CommandPalette";
import { DaemonProvider } from "./daemon";
import { sharedQueryCache } from "./queryCache";
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
  DeviceSummary,
  DirectorSnapshot,
  DirectorySnapshot,
  EntitySummary,
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
  refreshes: [],
  slice_revisions: {},
};
const entities: EntityIndexSnapshot = {
  metadata,
  entities: [
    {
      entity: { kind: "replicant", id: "R1" },
      label: "R1",
      secondary_label: null,
      system: "SYS-A",
      location: "SYS-A",
      entity_type: "replicant",
      status: "active",
    },
  ],
};
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
  last_cursor: null,
  freshness: { last_refresh_at: null, stale: false, last_error: null },
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
  history_database_path: "replicant-history.sqlite",
  telemetry_database_path: "replicant-telemetry.sqlite",
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
  mining_policies: [],
  replicants: [],
  requirements: [],
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

const mockRefreshGalaxy = vi.hoisted(() =>
  vi.fn<() => Promise<void>>(() => Promise.resolve()),
);
const mockRefreshLocations = vi.hoisted(() =>
  vi.fn<(system?: string) => Promise<void>>(() => Promise.resolve()),
);
const mockEntities = vi.hoisted(() => vi.fn(() => Promise.resolve(entities)));
const mockDevices = vi.hoisted(() => vi.fn(() => Promise.resolve(devices)));
const mockMessages = vi.hoisted(() => vi.fn(() => Promise.resolve(messages)));
const mockGalaxyScene = vi.hoisted(() =>
  vi.fn((signal?: AbortSignal) => {
    void signal;
    return Promise.resolve(galaxyScene);
  }),
);

vi.mock("./api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api")>();
  return {
    ...actual,
    daemonApi: {
      health: () => Promise.resolve(health),
      snapshot: () => Promise.resolve(snapshot),
      entities: mockEntities,
      descriptors: () => Promise.resolve(descriptors),
      overview: () => Promise.resolve(overview),
      devices: mockDevices,
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
      messages: mockMessages,
      bobnet: () => Promise.resolve(bobnet),
      network: () => Promise.resolve(network),
      standing: () => Promise.resolve(standing),
      leaderboards: () => Promise.resolve(leaderboards),
      settings: () => Promise.resolve(settings),
      galaxyScene: mockGalaxyScene,
      systemScene: (system: string) => Promise.resolve(systemScene(system)),
      refreshGalaxy: mockRefreshGalaxy,
      refreshLocations: mockRefreshLocations,
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
  addEventListener(type: string, listener: EventListenerOrEventListenerObject) {
    if (type !== "open") return;
    queueMicrotask(() => {
      this.readyState = MockWebSocket.OPEN;
      const event = new Event("open");
      if (typeof listener === "function") listener(event);
      else listener.handleEvent(event);
    });
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
  "Directory",
  "Achievements",
  "Species Reputation",
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
    sharedQueryCache.clear();
    mockEntities.mockClear();
    mockDevices.mockClear();
    mockMessages.mockClear();
    mockGalaxyScene.mockClear();
    mockRefreshGalaxy.mockClear();
    mockRefreshLocations.mockClear();
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

    expect(mockEntities).toHaveBeenCalledTimes(1);
    expect(mockDevices).toHaveBeenCalledTimes(1);
    expect(mockMessages).toHaveBeenCalledTimes(1);
    expect(mockGalaxyScene).toHaveBeenCalledTimes(1);
  });

  it("cancels a page-owned galaxy request when navigation unmounts the page", async () => {
    mockGalaxyScene.mockImplementationOnce(
      () => new Promise<GalaxySceneSnapshot>(() => undefined),
    );
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

    const navigate = async (destination: string) => {
      const button = Array.from(
        container.querySelectorAll<HTMLButtonElement>(".sidebar button"),
      ).find((candidate) => candidate.textContent === destination);
      await act(async () => {
        button?.click();
        await flush();
      });
    };
    await navigate("Galaxy");
    const signal = mockGalaxyScene.mock.calls[0]?.[0];
    expect(signal?.aborted).toBe(false);

    await navigate("Overview");

    expect(signal?.aborted).toBe(true);
  });

  it("runs galaxy, targeted, and palette location refreshes", async () => {
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

    const navigate = async (destination: string) => {
      const button = Array.from(
        container.querySelectorAll<HTMLButtonElement>(".sidebar button"),
      ).find((candidate) => candidate.textContent === destination);
      await act(async () => {
        button?.dispatchEvent(new MouseEvent("click", { bubbles: true }));
        await flush();
        await flush();
      });
    };
    const buttonNamed = (name: string) =>
      Array.from(container.querySelectorAll<HTMLButtonElement>("button")).find(
        (button) => button.textContent === name,
      );

    await navigate("Galaxy");
    await act(async () => {
      buttonNamed("Refresh galaxy data")?.click();
      await flush();
    });
    expect(mockRefreshGalaxy).toHaveBeenCalledOnce();

    await navigate("System");
    await act(async () => {
      buttonNamed("Refresh system locations")?.click();
      await flush();
    });
    expect(mockRefreshLocations).toHaveBeenLastCalledWith("SYS-A");

    await act(async () => {
      window.dispatchEvent(
        new KeyboardEvent("keydown", { key: "k", ctrlKey: true }),
      );
      await flush();
    });
    await act(async () => {
      Array.from(
        container.querySelectorAll<HTMLButtonElement>(
          ".palette-results button",
        ),
      )
        .find((button) =>
          button.textContent.includes("Refresh system locations"),
        )
        ?.click();
      await flush();
    });
    const form = container.querySelector<HTMLFormElement>(".palette-form");
    const input = form?.querySelector<HTMLInputElement>("input");
    expect(input?.value).toBe("SYS-A");
    await act(async () => {
      form?.dispatchEvent(
        new SubmitEvent("submit", { bubbles: true, cancelable: true }),
      );
      await flush();
    });
    expect(mockRefreshLocations).toHaveBeenLastCalledWith("SYS-A");
  });
});

describe("device inspector labels", () => {
  it("shows related device types with their codes", () => {
    const related = {
      "device:CHILD-1": {
        entity: { kind: "device", id: "CHILD-1" },
        label: "CHILD-1",
        secondary_label: "survey_drone",
        system: "SOL",
        location: "SOL-1",
        entity_type: "survey_drone",
        status: "active",
      },
    } satisfies Record<string, EntitySummary>;

    expect(relatedDeviceLabel("CHILD-1", related)).toBe(
      "survey_drone (CHILD-1)",
    );
    expect(relatedDeviceLabel("UNKNOWN", related)).toBe("UNKNOWN");
  });

  it("limits detach targets to devices attached to the selected host", () => {
    const command = {
      operationClass: "action",
      descriptor: {
        kind: "device.detach",
        parameters: [{ name: "target" }],
      },
    } as DescriptorCommand;
    const specialized = specializeDeviceCommand(
      command,
      {
        attached_devices: ["CHILD-1"],
        available_commands: ["detach"],
      } as DeviceSummary,
      {
        "device:CHILD-1": { entity_type: "survey_drone" } as EntitySummary,
        "device:OTHER": { entity_type: "mining_drone" } as EntitySummary,
      } satisfies Record<string, EntitySummary>,
    );

    expect(specialized.descriptor.parameters[0]?.kind).toEqual({
      type: "enum",
    });
    expect(specialized.descriptor.parameters[0]?.options).toEqual([
      { value: "CHILD-1", label: "survey_drone (CHILD-1)" },
    ]);
  });

  it("limits release targets and directives to the selected controller", () => {
    const release = specializeDeviceCommand(
      {
        operationClass: "action",
        descriptor: {
          kind: "device.release",
          parameters: [{ name: "target" }],
        },
      } as DescriptorCommand,
      {
        controlled_devices: ["CHILD-1"],
      } as DeviceSummary,
      {
        "device:CHILD-1": { entity_type: "mining_drone" } as EntitySummary,
        "device:OTHER": { entity_type: "survey_drone" } as EntitySummary,
      },
    );
    const directive = specializeDeviceCommand(
      {
        operationClass: "action",
        descriptor: {
          kind: "device.set_directive",
          parameters: [{ name: "directive" }],
        },
      } as DescriptorCommand,
      { available_directives: ["gather_evenly", "patrol"] } as DeviceSummary,
    );

    expect(release.descriptor.parameters[0]?.options).toEqual([
      { value: "CHILD-1", label: "mining_drone (CHILD-1)" },
    ]);
    expect(directive.descriptor.parameters[0]?.options).toEqual([
      { value: "gather_evenly", label: "gather evenly" },
      { value: "patrol", label: "patrol" },
    ]);
    expect(directive.descriptor.parameters[0]?.kind).toEqual({ type: "enum" });
  });
});
