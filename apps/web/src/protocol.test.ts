import { describe, expect, it } from "vitest";

import {
  parseAutofactoryResponse,
  parseBobnetResponse,
  parseBootstrapResponse,
  parseCargoResponse,
  parseHealthResponse,
  parseDevicesResponse,
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
  parseSnapshotResponse,
  parseStandingResponse,
  parseSurveyResponse,
  parseSystemSceneResponse,
  parseTradeResponse,
  parseTriggerListResponse,
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

describe("parseDirectorResponse", () => {
  it("parses durable shared requirements and defaults old snapshots to none", () => {
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
    expect(legacy.payload.requirements).toEqual([]);

    const parsed = parseDirectorResponse({
      protocol_version: 1,
      payload: {
        ...base,
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
    expect(trade?.trades[0]?.current_stock).toBeNull();
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
