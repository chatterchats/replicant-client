import { describe, expect, it } from "vitest";

import {
  PROTOCOL_VERSION,
  parseAutofactoryResponse,
  parseBillFinderResponse,
  parseBobnetResponse,
  parseBootstrapResponse,
  parseCargoResponse,
  parseHealthResponse,
  parseDescriptorsResponse,
  parseDevicesResponse,
  parseEntityInspectorResponse,
  parseDirectorResponse,
  parseInventoryResponse,
  parseLeaderboardsResponse,
  parseMiningResponse,
  parseMessagesResponse,
  parseNetworkResponse,
  parseRelayResponse,
  parseEntityIndexResponse,
  parseEventsResponse,
  parseGalaxySceneResponse,
  parseLiveMessage,
  parseOverviewResponse,
  parseReportsResponse,
  parseSettingsResponse,
  parseSnapshotResponse,
  parseStandingResponse,
  parseSurveyResponse,
  parseSystemSceneResponse,
  parseTradeResponse,
  parseTriggerListResponse,
  parseWorkflowDetailResponse,
  parseWorkflowIntelligenceResponse,
} from "./protocol";

const rawDevice = {
  entity: { kind: "device", id: "D-1" },
  device_type: "future_device",
  status: "active",
  ownership: "owned",
  owner: null,
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
  attach_capacity: 2,
  cargo_capacity: 10,
  cargo_used: 3,
  operational_capacity_percent: 100,
  active_directive: null,
  directive_status: null,
  travel_destination: null,
  claim: null,
};

describe("parseDevicesResponse", () => {
  it("preserves forward-compatible device types and missing fields", () => {
    const parsed = parseDevicesResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 3, generated_at_ms: 10 },
        devices: [
          {
            entity: { kind: "device", id: "D-1" },
            device_type: "future_device",
            status: null,
            ownership: "owned",
            owner: null,
            system: null,
            location: null,
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
          },
        ],
      },
    });
    expect(parsed.payload.devices[0]).toMatchObject({
      device_type: "future_device",
      status: null,
      owner_name: null,
      claim: null,
    });
  });
});

describe("parseEntityInspectorResponse", () => {
  const summary = {
    entity: { kind: "device", id: "D-1" },
    label: "D-1",
    secondary_label: "vessel",
    system: "SOL",
    location: "EARTH",
    entity_type: "vessel",
    status: "active",
  };
  const envelope = (detail: unknown, provenance: unknown = null) => ({
    protocol_version: PROTOCOL_VERSION,
    payload: {
      metadata: { revision: 42, generated_at_ms: 1000 },
      summary,
      provenance,
      detail,
    },
  });

  it("keeps protocol version 1 and defaults missing additive device fields", () => {
    expect(PROTOCOL_VERSION).toBe(1);
    const parsed = parseEntityInspectorResponse(
      envelope({ kind: "device", detail: rawDevice }),
    );
    expect(parsed.protocol_version).toBe(1);
    expect(parsed.payload.detail).toMatchObject({
      kind: "device",
      detail: {
        device: {
          features: [],
          cargo: [],
          stow_capacity: null,
          stow_used: null,
          grace_period_remaining: null,
          upkeep_requirements: [],
          system_status: null,
        },
        deployed_at: null,
        in_control_range: null,
        settings: {},
        hosting_replicant: null,
        travel: null,
      },
    });
  });

  it("parses device, system, and location details with provenance and groups", () => {
    const provenance = {
      observed_at_ms: 900,
      stale: true,
      reachability: "local",
      source_operation: "get_device",
    };
    const device = parseEntityInspectorResponse(
      envelope(
        {
          kind: "device",
          detail: {
            ...rawDevice,
            features: ["travel"],
            cargo: [{ resource: "iron", quantity: 3 }],
            stow_capacity: 10,
            stow_used: 1,
            grace_period_remaining: 60,
            upkeep_requirements: [{ resource: "fuel" }],
            system_status: { drive: "ready" },
            deployed_at: "2026-09-04T12:00:00Z",
            in_control_range: true,
            settings: { mode: "survey" },
            hosting_replicant: { kind: "replicant", id: "R-1" },
            travel: {
              origin: "SOL-1",
              destination: "ALPHA-4",
              final_destination: "BETA-3-L4",
              departed_at: "2026-09-04T12:00:00Z",
              arrives_at: "2026-09-04T12:10:00Z",
              final_arrives_at: "2026-09-04T12:30:00Z",
              eta_seconds: 600,
              route_eta_seconds: 1800,
              stage: "surge",
              travel_type: "ftl",
            },
          },
        },
        provenance,
      ),
    );
    expect(device.payload.provenance).toEqual(provenance);
    expect(device.payload.detail).toMatchObject({
      kind: "device",
      detail: {
        device: { cargo_used: 3, stow_used: 1 },
        in_control_range: true,
        hosting_replicant: { kind: "replicant", id: "R-1" },
        travel: { final_destination: "BETA-3-L4", route_eta_seconds: 1800 },
      },
    });

    const groups = [
      {
        entity_kind: "location",
        entity_type: "planet",
        count: 54,
        statuses: [
          { status: null, count: 1 },
          { status: "scanned", count: 53 },
        ],
      },
    ];
    const system = parseEntityInspectorResponse(
      envelope({
        kind: "system",
        detail: {
          name: "Sol",
          spectral_type: "G",
          region: "Core",
          entry_point: "SOL",
          position: { x: 0, y: 0, z: 0 },
          explored: true,
          has_hub: false,
          has_ward: false,
          has_life: true,
          children: { total: 54, groups },
        },
      }),
    );
    expect(system.payload.detail).toMatchObject({
      kind: "system",
      detail: { has_hub: false, children: { total: 54, items: [], groups } },
    });

    const location = parseEntityInspectorResponse(
      envelope({
        kind: "location",
        detail: {
          location_type: "planet",
          custom_name: "Earth",
          system: "SOL",
          scanned: false,
          system_scanned: true,
          system_tags: ["home"],
          survey: {
            system_complete: false,
            planets_total: 8,
            planets_scanned: 7,
            moons_total: 1,
            moons_scanned: 0,
            moons_total_estimated: false,
          },
          environment: {
            magnetic_field: false,
            gravity_g: 0,
            atmospheric_pressure_atm: 1,
            oxygen_percent: 21,
            hydrosphere_percent: 71,
            biosphere_index: 95,
            subsurface_ocean: false,
            life_stage: "none",
          },
          contents: { total: 393, groups: [] },
        },
      }),
    );
    expect(location.payload.detail).toMatchObject({
      kind: "location",
      detail: {
        custom_name: "Earth",
        parent: null,
        survey: { system_complete: false },
        environment: {
          atmosphere: null,
          magnetic_field: false,
          gravity_g: 0,
          atmospheric_pressure_atm: 1,
          oxygen_percent: 21,
          hydrosphere_percent: 71,
          biosphere_index: 95,
          subsurface_ocean: false,
          life_stage: "none",
        },
        contents: { total: 393, items: [] },
      },
    });

    const replicant = parseEntityInspectorResponse({
      protocol_version: PROTOCOL_VERSION,
      payload: {
        metadata: { revision: 42, generated_at_ms: 1000 },
        summary: {
          entity: { kind: "replicant", id: "R-1" },
          label: "Chats-1",
          secondary_label: "R-1",
          system: "SOL",
          location: "EARTH",
          entity_type: "replicant",
          status: "stationary",
        },
        provenance: null,
        detail: {
          kind: "replicant",
          detail: {
            entity: { kind: "replicant", id: "R-1" },
            name: "Chats-1",
            status: "stationary",
            is_npc: false,
            ownership: "owned",
            system: "SOL",
            region: "Alpha",
            assigned_region: "Alpha",
            director_state: "operational",
            role_affinity: "catalogue",
            workflow_id: null,
            location: "EARTH",
            hosted_device: { kind: "device", id: "V-1" },
            travel: null,
            description: null,
            pronouns: "he/him",
            experience_points: 100,
            plan: null,
            cohort_permission: null,
          },
        },
      },
    });
    expect(replicant.payload.detail).toMatchObject({
      kind: "replicant",
      detail: {
        name: "Chats-1",
        region: "Alpha",
        assigned_region: "Alpha",
        director_state: "operational",
        role_affinity: "catalogue",
        hosted_device: { kind: "device", id: "V-1" },
        experience_points: 100,
      },
    });
  });
});

describe("parseDescriptorsResponse device bindings", () => {
  const descriptor = {
    kind: "device.travel",
    display_name: "Travel",
    aliases: [],
    description: "Travel",
    category: "devices",
    operation_class: "action",
    risk: "low",
    applicable_to: ["device"],
    parameters: [],
  };

  it("parses command bindings and defaults missing additive bindings", () => {
    const parsed = parseDescriptorsResponse({
      protocol_version: 1,
      payload: {
        reports: [],
        actions: [
          descriptor,
          {
            ...descriptor,
            kind: "device.lifecycle",
            device_commands: [
              { command: "deactivate", parameters: { command: "deactivate" } },
            ],
          },
        ],
        workflows: [],
      },
    });
    expect(parsed.payload.actions[0]?.device_commands).toEqual([]);
    expect(parsed.payload.actions[1]?.device_commands).toEqual([
      { command: "deactivate", parameters: { command: "deactivate" } },
    ]);
  });
});

describe("parseDirectorResponse", () => {
  it("parses durable shared requirements and legacy snapshots without newer goal kinds", () => {
    const base = {
      metadata: { revision: 3, generated_at_ms: 10 },
      mode: "advisory",
      regions: [],
      goals: [],
      replicants: [],
      workforce: {
        total: 0,
        busy: 0,
        idle: 0,
        idle_ratio: 1,
        pending_worker_demand: 0,
        scale_up_recommended: false,
        scale_reason: null,
      },
    };
    const legacy = parseDirectorResponse({
      protocol_version: 1,
      payload: base,
    });
    expect(legacy.payload.goals).toEqual([]);
    expect(legacy.payload.mining_policies).toEqual([]);
    expect(legacy.payload.catalogue_policies).toEqual([]);
    expect(legacy.payload.requirements).toEqual([]);
    expect(legacy.payload.workforce).toMatchObject({
      operational: 0,
      in_transit: 0,
      unavailable: 0,
    });
    expect(legacy.payload.workforce.regions).toEqual([]);
    const legacyWithGoal = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        ...base,
        goals: [
          {
            id: "expand_mining_ops:alpha",
            kind: "expand_mining_ops",
            region: "alpha",
            status: "waiting",
            objective: "Expand mining operations",
            blocker: null,
            next_action: null,
            progress_current: 0,
            progress_total: 0,
            active_workflows: [],
            enabled: true,
          },
        ],
      },
    });
    expect(legacyWithGoal.payload.goals[0]?.kind).toBe("expand_mining_ops");

    const parsed = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        ...base,
        workforce: {
          ...base.workforce,
          regions: [
            {
              region: "beta",
              bootstrap_target: 2,
              assigned: 3,
              incoming: 1,
              operational: 2,
              in_transit: 0,
              busy: 1,
              desired_ordinary_capacity: 4,
              scale_up_suppressed: true,
              scale_up_suppression_reason: "manufacturing home unavailable",
              manufacturing_home: null,
              manufacturing_home_reason: "No regional factory yet",
            },
          ],
        },
        requirements: [
          {
            id: "worker:beta:catalogue",
            kind: "worker_capacity",
            status: "pending",
            region: "beta",
            target: "beta: +2 catalogue worker(s)",
            priority: 500,
            requesters: [
              {
                goal_id: "enhance_star_catalogue:beta",
                reason: "survey backlog",
                priority: 500,
              },
            ],
            active_workflows: [],
          },
        ],
      },
    });
    expect(parsed.payload.requirements[0]).toMatchObject({
      id: "worker:beta:catalogue",
      kind: "worker_capacity",
      region: "beta",
      priority: 500,
    });
    expect(parsed.payload.workforce.regions).toEqual([
      {
        region: "beta",
        bootstrap_target: 2,
        assigned: 3,
        incoming: 1,
        operational: 2,
        in_transit: 0,
        busy: 1,
        desired_ordinary_capacity: 4,
        scale_up_suppressed: true,
        scale_up_suppression_reason: "manufacturing home unavailable",
        manufacturing_home: null,
        manufacturing_home_reason: "No regional factory yet",
      },
    ]);
  });

  it("accepts the Blueprint Acquisition standing goal", () => {
    const parsed = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 4, generated_at_ms: 20 },
        mode: "advisory",
        regions: [],
        goals: [
          {
            id: "blueprint_acquisition",
            kind: "blueprint_acquisition",
            region: null,
            status: "active",
            objective: "Learn missing blueprints from owned devices",
            blocker: null,
            next_action:
              "Sacrifice owned service_bot DEVICE-1 at Autofactory FACTORY-1",
            progress_current: 8,
            progress_total: 9,
            active_workflows: [],
            enabled: true,
          },
        ],
        replicants: [],
        requirements: [],
        workforce: {
          total: 0,
          busy: 0,
          idle: 0,
          idle_ratio: 1,
          pending_worker_demand: 0,
          scale_up_recommended: false,
          scale_reason: null,
        },
      },
    });

    expect(parsed.payload.goals[0]?.kind).toBe("blueprint_acquisition");
  });

  it("accepts the Maintain System Hubs standing goal", () => {
    const parsed = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 4, generated_at_ms: 20 },
        mode: "automatic",
        regions: [],
        goals: [
          {
            id: "maintain_system_hubs:alpha",
            kind: "maintain_system_hubs",
            region: "alpha",
            status: "active",
            objective: "Keep every operational System Hub in alpha supplied",
            blocker: null,
            next_action: "move structural 400 to SCEPTURUM-7-L4",
            progress_current: 2,
            progress_total: 3,
            active_workflows: ["WF-HUB"],
            enabled: true,
          },
        ],
        replicants: [],
        requirements: [],
        workforce: {
          total: 0,
          busy: 0,
          idle: 0,
          idle_ratio: 1,
          pending_worker_demand: 0,
          scale_up_recommended: false,
          scale_reason: null,
        },
      },
    });

    expect(parsed.payload.goals[0]?.kind).toBe("maintain_system_hubs");
  });
  it("accepts the Salvage Recovery standing goal", () => {
    const parsed = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 4, generated_at_ms: 20 },
        mode: "automatic",
        regions: [],
        goals: [
          {
            id: "salvage_recovery:alpha",
            kind: "salvage_recovery",
            region: "alpha",
            status: "active",
            objective: "Recover discovered regional salvage",
            blocker: null,
            next_action:
              "Continue the active regional salvage recovery campaign",
            progress_current: 0,
            progress_total: 2,
            active_workflows: ["WF-SALVAGE"],
            enabled: true,
          },
        ],
        replicants: [],
        requirements: [],
        workforce: {
          total: 0,
          busy: 0,
          idle: 0,
          idle_ratio: 1,
          pending_worker_demand: 0,
          scale_up_recommended: false,
          scale_reason: null,
        },
      },
    });

    expect(parsed.payload.goals[0]?.kind).toBe("salvage_recovery");
  });

  it("accepts a disabled regional Asteroid Diversion goal", () => {
    const parsed = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 5, generated_at_ms: 30 },
        mode: "automatic",
        regions: [],
        goals: [
          {
            id: "asteroid_diversion:alpha",
            kind: "asteroid_diversion",
            region: "alpha",
            status: "waiting",
            objective: "Divert incoming asteroids threatening regional systems",
            blocker: null,
            next_action: "Waiting for a threatening asteroid",
            progress_current: 0,
            progress_total: 0,
            active_workflows: [],
            enabled: false,
          },
        ],
        replicants: [],
        requirements: [],
        workforce: {
          total: 0,
          busy: 0,
          idle: 0,
          idle_ratio: 1,
          pending_worker_demand: 0,
          scale_up_recommended: false,
          scale_reason: null,
        },
      },
    });

    expect(parsed.payload.goals[0]).toMatchObject({
      kind: "asteroid_diversion",
      region: "alpha",
      status: "waiting",
      objective: "Divert incoming asteroids threatening regional systems",
      enabled: false,
    });
  });
  it("accepts the Stranded Device Recovery standing goal", () => {
    const parsed = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 6, generated_at_ms: 40 },
        mode: "automatic",
        regions: [],
        goals: [
          {
            id: "stranded_device_recovery:alpha",
            kind: "stranded_device_recovery",
            region: "alpha",
            status: "active",
            objective: "Recover stranded owned devices to regional System Hubs",
            blocker: null,
            next_action:
              "Recover stranded device DEVICE-1 from BELT-1 to ALPHA-HUB",
            progress_current: 0,
            progress_total: 1,
            active_workflows: ["WF-RECOVERY"],
            enabled: true,
          },
        ],
        replicants: [],
        requirements: [],
        workforce: {
          total: 0,
          busy: 0,
          idle: 0,
          idle_ratio: 1,
          pending_worker_demand: 0,
          scale_up_recommended: false,
          scale_reason: null,
        },
      },
    });

    expect(parsed.payload.goals[0]).toMatchObject({
      id: "stranded_device_recovery:alpha",
      kind: "stranded_device_recovery",
      region: "alpha",
      status: "active",
      objective: "Recover stranded owned devices to regional System Hubs",
      blocker: null,
      next_action: "Recover stranded device DEVICE-1 from BELT-1 to ALPHA-HUB",
      progress_current: 0,
      progress_total: 1,
      active_workflows: ["WF-RECOVERY"],
      enabled: true,
    });
  });

  it("accepts every Stranded Device Recovery lifecycle state", () => {
    const statuses = ["waiting", "active", "blocked", "satisfied"] as const;
    const parsed = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 8, generated_at_ms: 50 },
        mode: "advisory",
        regions: [],
        goals: statuses.map((status, index) => ({
          id: `stranded_device_recovery:region-${String(index)}`,
          kind: "stranded_device_recovery",
          region: `region-${String(index)}`,
          status,
          objective: "Recover stranded owned devices to regional System Hubs",
          blocker:
            status === "blocked" ? "Placement authority is incomplete" : null,
          next_action:
            status === "active"
              ? "Recover stranded device DEVICE-1 from BELT-1 to ALPHA-HUB"
              : null,
          progress_current: status === "satisfied" ? 1 : 0,
          progress_total: 1,
          active_workflows: status === "active" ? ["WF-RECOVERY"] : [],
          enabled: status !== "waiting",
        })),
        replicants: [],
        requirements: [],
        workforce: {
          total: 0,
          busy: 0,
          idle: 0,
          idle_ratio: 1,
          pending_worker_demand: 0,
          scale_up_recommended: false,
          scale_reason: null,
        },
      },
    });

    expect(parsed.payload.goals.map((goal) => goal.status)).toEqual(statuses);
    expect(parsed.payload.goals[2]?.blocker).toBe(
      "Placement authority is incomplete",
    );
    expect(parsed.payload.goals[1]?.active_workflows).toEqual(["WF-RECOVERY"]);
  });

  it("accepts the Unserviced Resources standing goal", () => {
    const parsed = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 7, generated_at_ms: 50 },
        mode: "automatic",
        regions: [],
        goals: [
          {
            id: "unserviced_resources:alpha",
            kind: "unserviced_resources",
            region: "alpha",
            status: "active",
            objective:
              "Establish AMI transport service for producing regional resources",
            blocker: null,
            next_action:
              "Establish AMI shuttle service from ALPHA-BELT to ALPHA-HUB",
            progress_current: 0,
            progress_total: 1,
            active_workflows: ["WF-TRANSPORT"],
            enabled: true,
          },
        ],
        replicants: [],
        requirements: [],
        workforce: {
          total: 0,
          busy: 0,
          idle: 0,
          idle_ratio: 1,
          pending_worker_demand: 0,
          scale_up_recommended: false,
          scale_reason: null,
        },
      },
    });

    expect(parsed.payload.goals[0]).toMatchObject({
      id: "unserviced_resources:alpha",
      kind: "unserviced_resources",
      region: "alpha",
      status: "active",
      progress_current: 0,
      progress_total: 1,
    });
  });
});

describe("mission projection parsers", () => {
  const workflow = {
    id: "WF-1",
    kind: "survey.route",
    status: "running",
    current_step: "traveling",
    revision: 2,
    updated_at_ms: 10,
  };

  it("parses structured Survey progress and partial Mining sets", () => {
    const survey = parseSurveyResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 3, generated_at_ms: 10 },
        missions: [
          {
            workflow,
            replicant: "R-1",
            vessel: "V-1",
            center: "SOL",
            phase: "traveling",
            completed_systems: 2,
            total_systems: 4,
            next_system: "VEGA",
            controller: null,
            drones: [],
          },
        ],
        fleet: [rawDevice],
      },
    });
    expect(survey.payload.missions[0]?.next_system).toBe("VEGA");

    const mining = parseMiningResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 3, generated_at_ms: 10 },
        installations: [
          {
            id: "SOL/SOL-1",
            system: "SOL",
            location: "SOL-1",
            controller: null,
            miners: [],
            survey_controller: null,
            survey_drones: [],
            maintenance_device: null,
            missing: ["mining controller"],
            status: "partial",
          },
        ],
        workflows: [{ ...workflow, kind: "mining.expansion" }],
      },
    });
    expect(mining.payload.installations[0]?.missing).toEqual([
      "mining controller",
    ]);
  });

  it("parses Relay coverage and Bootstrap mission progress", () => {
    const relay = parseRelayResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 4, generated_at_ms: 10 },
        relays: [{ ...rawDevice, device_type: "ftl_relay" }],
        staged_relays: [],
        connected_systems: 2,
        relay_edges: [{ from: "SOL", to: "VEGA" }],
        expansions: [
          {
            workflow: { ...workflow, kind: "relay.expansion" },
            replicant: "R-1",
            hub: "SOL-1",
            targets: ["VEGA"],
            phase: "deploying",
            completed_stops: 1,
            total_stops: 2,
            next_system: "VEGA",
            pending_relays: 0,
          },
        ],
      },
    });
    expect(relay.payload.relay_edges[0]).toEqual({ from: "SOL", to: "VEGA" });
    expect(relay.payload.expansions[0]?.next_system).toBe("VEGA");

    const bootstrap = parseBootstrapResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 5, generated_at_ms: 10 },
        missions: [
          {
            mission_id: "BOOT-1",
            execution_id: "EXEC-1",
            region: "beta",
            source_hub: "SOL-1",
            target_system: "VEGA",
            target_location: "VEGA-ENTRY",
            phase: "completed",
            reserved_devices: 10,
            loaded_devices: 10,
            capital_system: "VEGA",
            selected_sites: 5,
            warnings: [],
            completed: true,
            updated_at_ms: 10,
          },
        ],
      },
    });
    expect(bootstrap.payload.missions[0]).toMatchObject({
      mission_id: "BOOT-1",
      completed: true,
      selected_sites: 5,
    });
  });
});

describe("asset projection parsers", () => {
  it("parses Autofactory queues and utilization", () => {
    const parsed = parseAutofactoryResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 4, generated_at_ms: 10 },
        utilization: {
          total: 1,
          busy: 1,
          available: 0,
          unavailable: 0,
          queued_units: 2,
          utilization_percent: 33.333333333333336,
        },
        factories: [
          {
            device: rawDevice,
            availability: "busy",
            queue_capacity: 4,
            queued_units: 2,
            current_job: {
              device_type: "relay",
              quantity: 1,
              eta_seconds: 60,
              tags: [],
            },
            queued_jobs: [],
          },
        ],
      },
    });
    expect(parsed.payload.factories[0]?.current_job?.device_type).toBe("relay");
    expect(parsed.payload.utilization.utilization_percent).toBeCloseTo(33.3333);
  });

  it("parses capability-based cargo rows", () => {
    const parsed = parseCargoResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 5, generated_at_ms: 10 },
        cargo_used: 3,
        cargo_capacity: 10,
        attachment_used: 1,
        attachment_capacity: 2,
        carriers: [
          {
            device: rawDevice,
            attachment_used: 1,
            resources: [{ resource: "silicates", quantity: 3 }],
          },
        ],
      },
    });
    expect(parsed.payload.carriers[0]?.resources[0]).toEqual({
      resource: "silicates",
      quantity: 3,
    });
  });
});

describe("parseInventoryResponse", () => {
  it("parses typed location and resource aggregates", () => {
    const parsed = parseInventoryResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 7, generated_at_ms: 10 },
        total_quantity: 12,
        locations: [
          {
            owner_kind: "location",
            owner: "EARTH",
            system: "SOL",
            region: "solzone",
            location: "EARTH",
            total_quantity: 12,
            resources: [{ resource: "silicates", quantity: 12 }],
          },
        ],
        resources: [
          {
            resource: "silicates",
            total_quantity: 12,
            distribution: [
              {
                owner_kind: "location",
                owner: "EARTH",
                system: "SOL",
                region: "solzone",
                location: "EARTH",
                quantity: 12,
              },
            ],
          },
        ],
      },
    });
    expect(parsed.payload.resources[0]?.total_quantity).toBe(12);
    expect(parsed.payload.locations[0]?.owner_kind).toBe("location");
    expect(parsed.payload.locations[0]?.region).toBe("solzone");
  });

  it("rejects untyped quantities", () => {
    expect(() =>
      parseInventoryResponse({
        protocol_version: 1,
        payload: {
          metadata: { revision: 1, generated_at_ms: 1 },
          total_quantity: "12",
          locations: [],
          resources: [],
        },
      }),
    ).toThrow("Invalid inventory total");
  });
});

describe("parseHealthResponse", () => {
  it("accepts the versioned daemon health shape", () => {
    expect(
      parseHealthResponse({
        protocol_version: 1,
        payload: { status: "healthy", daemon_version: "0.1.0", detail: null },
      }),
    ).toMatchObject({ protocol_version: 1, payload: { status: "healthy" } });
  });

  it("rejects an untyped backend response", () => {
    expect(() => parseHealthResponse({ status: "healthy" })).toThrow(
      "Unsupported daemon protocol version",
    );
  });

  it("parses the mirrored snapshot and tagged delta contracts", () => {
    expect(
      parseSnapshotResponse({
        protocol_version: 1,
        payload: {
          metadata: { revision: 4, generated_at_ms: 10 },
          sync: {
            phase: "ready",
            revision: 12,
            last_event_at_ms: null,
            detail: null,
          },
          automation: {
            automatic_triggers_enabled: true,
            workflows_paused: false,
          },
          workflows: [],
          notifications: [],
          requirements: [
            {
              id: "relay-sol",
              name: "SOL relay coverage",
              target: "relay infrastructure",
              scope: "system SOL",
              desired: 2,
              actual: 1,
              in_progress: 1,
              missing: 0,
              workflow_id: "workflow-1",
              status: "running",
            },
          ],
        },
      }).payload.metadata.revision,
    ).toBe(4);
    expect(
      parseLiveMessage({
        protocol_version: 1,
        revision: 5,
        delta: {
          type: "entity_upsert",
          data: {
            entity: { kind: "device", id: "D-1" },
            value: {
              entity: { kind: "device", id: "D-1" },
              label: "D-1",
              secondary_label: "mining_drone",
              system: "SOL",
              location: "EARTH",
              entity_type: "mining_drone",
              status: "idle",
            },
          },
        },
      }).delta.type,
    ).toBe("entity_upsert");
  });

  it("parses the versioned settings snapshot shape", () => {
    const parsed = parseSettingsResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 7, generated_at_ms: 12 },
        profile: "default",
        bind_address: "127.0.0.1:8080",
        managed_database_path: "replicant-client.sqlite",
        history_database_path: "replicant-history.sqlite",
        telemetry_database_path: "replicant-telemetry.sqlite",
        runtime_database_path: "replicant-runtime.sqlite",
        log_filter: "info",
        docker: false,
        api_token_source: "unset",
        daemon_settings_require_restart: false,
      },
    });

    expect(parsed.payload).toMatchObject({
      profile: "default",
      api_token_source: "unset",
      daemon_settings_require_restart: false,
    });
  });

  it("parses typed entity index snapshots", () => {
    const index = parseEntityIndexResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 4, generated_at_ms: 10 },
        entities: [
          {
            entity: { kind: "replicant", id: "R-1" },
            label: "R-1",
            secondary_label: "Ada",
            system: "SOL",
            location: "EARTH",
            entity_type: null,
            status: "idle",
          },
        ],
      },
    }).payload;
    expect(index.metadata.revision).toBe(4);
    expect(index.entities[0]?.location).toBe("EARTH");
  });

  it("parses typed overview snapshots", () => {
    const overview = parseOverviewResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 4, generated_at_ms: 10 },
        health: { status: "healthy", daemon_version: "test", detail: null },
        sync: {
          phase: "ready",
          revision: 4,
          last_event_at_ms: null,
          detail: null,
        },
        automation: {
          automatic_triggers_enabled: true,
          workflows_paused: false,
        },
        replicants: [],
        active_travel: [],
        active_workflows: [],
        workflow_counts: [{ status: "running", count: 2 }],
        attention_workflows: [],
        notifications: [],
        recent_activity: [],
      },
    }).payload;
    expect(overview.workflow_counts).toEqual([{ status: "running", count: 2 }]);
  });

  it("rejects unknown protocol versions and delta variants", () => {
    expect(() =>
      parseLiveMessage({ protocol_version: 2, revision: 1, delta: {} }),
    ).toThrow("Unsupported daemon protocol version");
    expect(() =>
      parseLiveMessage({
        protocol_version: 1,
        revision: 1,
        delta: { type: "raw_upstream_event", data: {} },
      }),
    ).toThrow("Unsupported live delta");
  });

  it("parses an application-owned galaxy scene", () => {
    const scene = parseGalaxySceneResponse({
      protocol_version: 1,
      payload: {
        revision: 7,
        generated_at_ms: 8,
        stars: [
          {
            id: "SOL",
            name: null,
            spectral_type: "G",
            region: "solzone",
            position: { x: 0, y: 1, z: 2 },
            exploration: "explored",
            current: true,
            has_hub: true,
            has_life: true,
            has_relay: false,
          },
        ],
        relay_edges: [],
        active_travel: [],
        signals: [],
        highlights: [],
        overlays: [],
        workflow_targets: [],
      },
    }).payload;
    expect(scene.stars[0]?.id).toBe("SOL");
    expect(scene.stars[0]?.position.z).toBe(2);
    expect(scene.stars[0]?.region).toBe("solzone");
  });

  it("parses an application-owned system scene", () => {
    const scene = parseSystemSceneResponse({
      protocol_version: 1,
      payload: {
        system: "SOL",
        revision: 7,
        generated_at_ms: 8,
        markers: [
          {
            id: "SOL-1",
            label: "SOL-1",
            kind: "planet",
            entity: { kind: "location", id: "SOL-1" },
            location: "SOL-1",
            parent: null,
            in_habitable_zone: true,
            position: { x: 1.25, y: 2.5 },
            count: 1,
          },
        ],
        active_travel: [],
        workflow_markers: [],
      },
    }).payload;
    expect(scene.system).toBe("SOL");
    expect(scene.markers[0]?.entity.kind).toBe("location");
    expect(scene.markers[0]?.in_habitable_zone).toBe(true);
    expect(scene.markers[0]?.position.x).toBe(1.25);
  });

  it("parses durable trigger status", () => {
    const triggers = parseTriggerListResponse({
      protocol_version: 1,
      payload: {
        triggers: [
          {
            id: "trigger-1",
            name: "hourly survey",
            condition: { kind: "schedule", interval_seconds: 3600 },
            target: {
              operation_class: "workflow",
              kind: "survey.route",
              parameters: {},
            },
            enabled: true,
            created_at_ms: 1,
            updated_at_ms: 2,
            last_fired_at_ms: null,
            next_run_at_ms: 3,
            last_error: null,
            revision: 0,
          },
        ],
      },
    }).payload;
    expect(triggers[0]?.condition.kind).toBe("schedule");
    expect(triggers[0]?.next_run_at_ms).toBe(3);
  });

  it("parses structured event progress and unknown labels", () => {
    const event = parseEventsResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 1, generated_at_ms: 2 },
        events: [
          {
            designation: "EVT-1",
            title: "Anomaly",
            event_type: "future_type",
            category: "future_category",
            tier: null,
            system: "SOL",
            location: "SOL-1",
            description: null,
            criteria: [
              {
                name: "supply",
                complete: false,
                requirements: [
                  {
                    kind: "resource",
                    item: "iron",
                    required: 10,
                    completed: 4,
                    remaining: 6,
                  },
                ],
              },
            ],
            rewards: {
              resources: [],
              devices: [],
              xp: null,
              civilisation_points: null,
              completion_achievement: null,
            },
            status: "active",
            discovered_at: null,
            completed_at: null,
          },
        ],
      },
    }).payload.events[0];
    expect(event?.event_type).toBe("future_type");
    expect(event?.category).toBe("future_category");
    expect(event?.criteria[0]?.requirements[0]?.remaining).toBe(6);
  });

  it("parses trades with missing optional fields", () => {
    const trade = parseTradeResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 1, generated_at_ms: 2 },
        viewer: null,
        controllers: [
          {
            entity: { kind: "device", id: "TC-1" },
            shop_name: null,
            description: null,
            is_local: false,
            owner_name: null,
            owner_replicant: null,
            system: null,
            location: null,
            total_stock: null,
            trade_count: null,
            trades: [
              {
                trade_code: "TRD-1",
                name: null,
                current_stock: null,
                initial_stock: null,
                requested: [],
                offered: [],
                created_at: null,
              },
            ],
            workflow: null,
          },
        ],
      },
    }).payload.controllers[0];
    expect(trade?.shop_name).toBeNull();
    expect(trade?.trade_details_status).toBe("available");
    expect(trade?.trades[0]?.current_stock).toBeNull();
  });

  it("parses Bill finder candidates and optional expansion workflow", () => {
    const result = parseBillFinderResponse({
      protocol_version: 1,
      payload: {
        metadata: { revision: 1, generated_at_ms: 2 },
        departure: {
          tracking_beacon: "FEB51E1B",
          replicant_code: "A8F48B26",
          vessel_code: "6BE43B4B",
          vessel_type: "racing_vessel",
          origin_location: "SOL-5-L4",
          origin_system: "SOL",
          logged_at: "2026-08-21T10:57:44-04:00",
          vector: [0.97, 0.1, -0.2],
        },
        candidates: [
          {
            system: "VEGA",
            angular_error_deg: 0.2,
            distance_ly: 24,
            projected_distance_ly: 23.9,
            cross_track_ly: 0.08,
          },
        ],
        recommended_system: "VEGA",
        confidence: "high",
        ambiguous: false,
        expansion: {
          status: "not_requested",
          target_system: null,
          workflow: null,
          message: "FTL expansion was not requested.",
        },
      },
    }).payload;
    expect(result.departure.origin_system).toBe("SOL");
    expect(result.candidates[0]?.system).toBe("VEGA");
    expect(result.recommended_system).toBe("VEGA");
  });

  it("parses typed intelligence snapshots", () => {
    const metadata = { revision: 1, generated_at_ms: 2 };
    const envelope = (payload: object) => ({ protocol_version: 1, payload });
    expect(
      parseReportsResponse(envelope({ metadata, reports: [], executions: [] }))
        .payload.reports,
    ).toEqual([]);
    expect(
      parseMessagesResponse(
        envelope({
          metadata,
          inbox: [],
          unread_count: null,
        }),
      ).payload.unread_count,
    ).toBeNull();
    expect(
      parseBobnetResponse(
        envelope({
          metadata,
          sources: [rawDevice],
          selected_source: "D-1",
          channels: [{ name: "#general", last_active: null }],
          messages: [
            {
              id: 1,
              channel: "#general",
              body: "hello",
              sender: "R-1",
              sender_name: "Ada",
              is_npc_or_system: false,
              current_system: "SOL",
              created_at: null,
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
        }),
      ).payload.messages[0]?.sender_name,
    ).toBe("Ada");
    expect(
      parseNetworkResponse(
        envelope({
          metadata,
          account_name: null,
          account_status: null,
          subscribed_channels: [],
          replicants: [],
          relays: [],
        }),
      ).payload.relays,
    ).toEqual([]);
    expect(
      parseStandingResponse(
        envelope({
          metadata,
          experience_points_total: 12,
          civilisation_points: null,
          achievements: [],
          reputation: [],
        }),
      ).payload.experience_points_total,
    ).toBe(12);
    expect(
      parseLeaderboardsResponse(
        envelope({
          metadata,
          boards: [
            { key: "xp", name: null, description: null, board_type: null },
          ],
          selected_board: "xp",
          entries: [],
        }),
      ).payload.selected_board,
    ).toBe("xp");
  });
});

describe("workflow intelligence protocol", () => {
  const metadata = { revision: 7, generated_at_ms: 42 };
  const reservation = {
    allocation_id: "ALLOC-1",
    workflow_id: "WF-1",
    work_item_id: "ITEM-1",
    requirement_key: "material:structural",
    kind: "material",
    resource: "structural",
    pool_identity: "inventory:location:HUB-1:structural",
    entity: null,
    capabilities: ["structural"],
    quantity: 400,
    region: "Alpha",
    system: "HUB",
    location: "HUB-1",
    created_at_ms: 10,
    updated_at_ms: 20,
  };
  const target = {
    workflow_id: "WF-1",
    kind: "event",
    key: "EVT-42",
    system: "THYFFAWFF",
    location: "THYFFAWFF-3-L4",
    active: true,
    created_at_ms: 11,
    updated_at_ms: 21,
  };

  it("parses active reservation and target projections", () => {
    const parsed = parseWorkflowIntelligenceResponse({
      protocol_version: 1,
      payload: { metadata, reservations: [reservation], targets: [target] },
    }).payload;
    expect(parsed.reservations[0]?.quantity).toBe(400);
    expect(parsed.reservations[0]?.resource).toBe("structural");
    expect(parsed.targets[0]?.key).toBe("EVT-42");
    expect(parsed.targets[0]?.active).toBe(true);
  });

  it("keeps workflow detail backwards compatible while preserving released target history", () => {
    const base = {
      summary: {
        id: "WF-1",
        kind: "event.campaign",
        status: "running",
        current_step: "planning",
        revision: 3,
        updated_at_ms: 30,
      },
      schema_version: 1,
      parameters: {},
      wait_reason: null,
      parent_id: null,
      claims: [],
      created_at_ms: 1,
      finished_at_ms: null,
      error: null,
    };
    const old = parseWorkflowDetailResponse({
      protocol_version: 1,
      payload: base,
    }).payload;
    expect(old.reservations).toEqual([]);
    expect(old.targets).toEqual([]);

    const detailed = parseWorkflowDetailResponse({
      protocol_version: 1,
      payload: {
        ...base,
        reservations: [reservation],
        targets: [{ ...target, active: false }],
      },
    }).payload;
    expect(detailed.targets[0]?.active).toBe(false);
  });
});
