---
title: "Event catalogue"
source_url: "https://replicant.space/docs/api/events/catalogue/"
crawled_at: "2026-08-11T15:11:28.592839+00:00"
---

API · Events

# Event catalogue

Every event type that your replicants can produce. Each entry shows the event name and an example payload. All events share the common envelope described in the event logs page.

See [event logs](../logs/index.md) for the shared envelope fields (*id*, *version*, *category*, *device_code*, *star*, etc.). This page documents only the *payload* object for each event type.

AMI digest events (*ami.mining.digest*, *ami.survey.digest*, *ami.transport.digest*) have their own structure documented on the [AMI digests](../ami-digests/index.md) page.

### *ami.adopted*

Devices were adopted into a controller's fleet. For periodic status digests, see [AMI digests](../ami-digests/index.md).

Example

response payload

```
{
  "devices": [
    { "device_code": "A1B2C3D4", "device_type": "mining_drone" },
    { "device_code": "E5F60718", "device_type": "mining_drone" }
  ]
}
```

### *ami.assembled*

An AMI controller assembled devices to its destination.

Example

response payload

```
{
  "destination": "SOL-4",
  "assembled_count": 6
}
```

### *ami.launched*

A controller launched its fleet to execute a directive.

Example

response payload

```
{
  "directive_status": "active",
  "evaluated": true,
  "devices_deployed": 4
}
```

### *ami.released*

Devices were released from a controller's fleet.

Example

response payload

```
{
  "devices": [
    { "device_code": "A1B2C3D4", "device_type": "mining_drone" }
  ]
}
```

### *ami.withdrawn*

A controller withdrew (recalled) its fleet.

Example

response payload

```
{
  "directive_paused": true,
  "devices_recalled": 4
}
```

### *blueprint.unlocked*

A new device blueprint was unlocked for your account.

Example

response payload

```
{
  "device_type": "ftl_relay",
  "short_description": "Chunky FTL network hardware for interstellar remote control.",
  "description": "The FTL relay is a heavy rack-like device built from stacked circuit planes, ceramic...",
  "resources": {
    "structural": 80,
    "conductive": 120,
    "silicates": 100,
    "carbon": 20,
    "volatiles": 10,
    "rares": 40
  },
  "components": null,
  "print_time": 600,
  "requires_autofactory": false
}
```

### *bobnet.new*

A new BobNet message was posted to a channel you subscribe to.

Example

response payload

```
{
  "id": 4821,
  "replicant_name": "Bob-1",
  "replicant_code": "57F0F6C8",
  "current_star": "SOL",
  "channel": "general",
  "message": "Anyone found rares in this system?"
}
```

### *device.attached*

A device was attached to a carrier.

Example

response payload

```
{
  "target_code": "A1B2C3D4",
  "target_type": "mining_drone"
}
```

### *device.changed_owner*

A device was transferred between replicants. Both sender and receiver get an event.

Example

response payload

```
{
  "from_replicant": "57F0F6C8",
  "to_replicant": "8AFE4482",
  "direction": "sent"          // "sent" or "received"
}
```

### *device.compacted*

A modular device finished compacting.

Example

response payload

```
{
  // empty response
}
```

### *device.compacting*

A modular device started compacting.

Example

response payload

```
{
  "completes_at": "2026-08-06T15:22:00Z"
}
```

### *device.decommissioned*

A device was decommissioned. May recover resources or discover a blueprint.

Example

response payload

```
{
  "resources_recovered": { "structural": 50 },
  "blueprint_discovered": null
}
```

### *device.deployed*

A device was deployed from a vessel to a location.

Example

response payload

```
{
  "deployed_from_device_code": "7FE981A0"
}
```

### *device.detached*

A device was detached from a carrier.

Example

response payload

```
{
  "target_code": "A1B2C3D4",
  "target_type": "mining_drone"
}
```

### *device.unfurled*

A modular device finished unfurling.

Example

response payload

```
{
  // empty response
}
```

### *device.unfurling*

A modular device started unfurling.

Example

response payload

```
{
  "completes_at": "2026-08-06T15:22:00Z"
}
```

### *device.stowed*

A device was stowed back into a vessel.

Example

response payload

```
{
  "stowed_in_device_code": "7FE981A0"
}
```

### *directive.cleared*

A directive was cleared from a controller.

Example

response payload

```
{
  "previous_directive": "mine_belt"
}
```

### *directive.completed*

A directive finished its work.

Example

response payload

```
{
  "directive": "mine_belt"
}
```

### *directive.paused*

A directive was paused.

Example

response payload

```
{
  "directive": "mine_belt"
}
```

### *directive.resumed*

A paused directive was resumed.

Example

response payload

```
{
  "directive": "mine_belt"
}
```

### *directive.set*

A directive was assigned to an AMI controller.

Example

response payload

```
{
  "directive": "mine_belt",
  "configuration": {
    "resource": "structural",
    "desired": 2000
  }
}
```

### *diversion.activated*

A propulsor locked onto an incoming object.

Example

response payload

```
{
  "object_designation": "SOL-OBJ-3",
  "size_class": "large"
}
```

### *diversion.deactivated*

A propulsor was deactivated.

Example

response payload

```
{
  "device_code": "A1B2C3D4"
}
```

### *diversion.diverted*

An asteroid was fully diverted away from its target.

Example

response payload

```
{
  "object_designation": "SOL-OBJ-3",
  "outcome": "diverted"       // "diverted" or "partial"
}
```

### *diversion.impacted*

An asteroid impacted a body.

Example

response payload

```
{
  "object_designation": "SOL-OBJ-3"
}
```

### *diversion.partial*

An asteroid was partially diverted but still poses a threat.

Example

response payload

```
{
  "object_designation": "SOL-OBJ-3",
  "outcome": "diverted"       // "diverted" or "partial"
}
```

### *event.completed*

A location event was completed. Rewards are included when non-zero.

Example

response payload

```
{
  "designation": "NUNKA-4-EVT-001",
  "location": "NUNKA-4",
  "event_type": "mineral_shortage",
  "tier": 1,
  "rewards": {
    "xp": 450,
    "resources": { "volatiles": 150 },
    "civilisation_points": 1
  },
  "consumed": {
    "devices": [
      { "device_code": "A1B2C3D4", "device_type": "mining_drone" }
    ],
    "resources": { "minerals": 500 }
  }
}
```

### *event.discovered*

A location event was discovered by your replicant at a planet or moon.

Example

response payload

```
{
  "designation": "NUNKA-4-EVT-001",
  "location": "NUNKA-4",
  "event_type": "mineral_shortage",
  "tier": 1,
  "title": "Mineral Shortage",
  "description": "The colony is running low on building materials...",
  "criteria": [
    {
      "name": "default",
      "devices": [],
      "resources": { "structural": 200, "conductive": 100 }
    }
  ]
}
```

### *experience.gained*

XP was awarded to a replicant. The *source* field indicates why.

Example

response payload

```
{
  "source": "location_event",  // see source values below
  "amount": 450
}
```

| Source | Description |
| --- | --- |
| *scan* | First scan of a celestial body. |
| *scan_system_completion_bonus* | Bonus for scanning every body in a system. |
| *location_event* | Completing a location event. |
| *print* | Completing a device print. |
| *achievement* | Unlocking an achievement. |
| *megastructure* | Contributing to a megastructure. |
| *diversion* | Participating in an asteroid diversion. |
| *finale_victory* | Victory in a finale event. |

### *hub.activated*

A system hub was activated.

Example

response payload

```
{
  "star": "CHAMAKUY",
  "location": "CHAMAKUY-4"
}
```

### *hub.destroyed*

A system hub was destroyed.

Example

response payload

```
{
  "star": "CHAMAKUY",
  "location": "CHAMAKUY-4"
}
```

### *megastructure.contributed*

Devices were contributed to a megastructure project.

Example

response payload

```
{
  "megastructure_designation": "DATACENTRE-001",
  "accepted_count": 2,
  "contributed_devices": [
    { "device_code": "A1B2C3D4", "device_type": "mining_drone" },
    { "device_code": "E5F60718", "device_type": "service_bot" }
  ]
}
```

### *message.new*

A new message was delivered to your account inbox.

Example

response payload

```
{
  "message_id": 4812,
  "message_type": "achievement",
  "title": "Event completed: Mineral Shortage",
  "body": "The civilisation at SOL-4 has received your assistance..."
}
```

### *mining.retargeted*

A mining device switched to a different resource type at its current site.

Example

response payload

```
{
  "location_type": "belt",       // "belt" or "salvage"
  "location": "SOL-BELT-1",
  "site": "SOL-BELT-1-SITE-3",
  "old_resource": "volatiles",
  "new_resource": "structural",
  "availability": "high",
  "density": "moderate",
  "cycle_time_seconds": 52
}
```

### *mining.started*

A device began mining at a belt or salvage site.

Example

response payload

```
{
  "location_type": "belt",       // "belt" or "salvage"
  "location": "SOL-BELT-1",
  "site": "SOL-BELT-1-SITE-3",
  "resource_type": "structural",
  "availability": "high",
  "density": "moderate",
  "cycle_time_seconds": 52
}
```

### *mining.stopped*

A device stopped mining. Includes the total quantity mined during the session.

Example

response payload

```
{
  "location": "SOL-BELT-1",
  "resource_type": "structural",
  "quantity_mined": 340
}
```

### *print.started*

A device print job was started by a replicant, vessel, or autofactory.

Example

response payload

```
{
  "device_type": "mining_drone",
  "print_mode": "vessel",    // "vessel", or "autofactory"
  "completes_at": "2026-08-02T14:30:00Z",
  "tags": ["fleet-a", "miner"]  // present when tags were set on the print job
}
```

### *print.completed*

A device print job finished.

Example

response payload

```
{
  "device_type": "parallax_array",
  "new_device_code": "A1B2C3D4",
  "print_mode": "autofactory",  // "vessel", or "autofactory"
  "consumed_device_codes": ["E5F6G7H8", "J9K0L1M2", "N3P4Q5R6"],  // present when components were consumed
  "tags": ["fleet-a", "miner"]  // present when tags were set on the print job
}
```

### *prospect.completed*

A galactic observatory completed a deep-space prospect, potentially discovering new star systems.

Example

response payload

```
{
  "origin": "SOL",
  "stars_generated": 3,
  "stars": ["CHAMAKUY", "NERIDA", "VOSSK"]
}
```

### *relay.activated*

An FTL relay was activated at a location.

Example

response payload

```
{
  "star": "CHAMAKUY",
  "location": "CHAMAKUY-BELT-1"
}
```

### *replicant.transferred*

A replicant was transferred to a new host vessel.

Example

response payload

```
{
  "old_host": "7FE981A0",
  "new_host": "A1B2C3D4"
}
```

### *salvage.depleted*

A salvage site was fully depleted of resources.

Example

response payload

```
{
  "site": "SOL-4-SAL-1"
}
```

### *salvage.discovered*

A salvage site was discovered at a location.

Example

response payload

```
{
  "designation": "SOL-4-SAL-1",
  "location": "SOL-4",
  "salvage_type": "wrecked_relay",
  "name": "Wrecked Relay",
  "resources": { "volatiles": 120, "structural": 80 }
}
```

### *scan.completed*

A scan finished. The report contains the full resource site breakdown for the scanned body. The *report* key matches the scan type (*belt*, *planet*, or *moon*).

Example

response payload

```
{
  "scan_target": "NUNKA-BELT-1",
  "scan_type": "belt",
  "report": {
    "belt": {
      "density": "moderate",
      "designation": "NUNKA-BELT-1",
      "resource_sites": [
        {
          "designation": "NUNKA-BELT-1-SITE-17",
          "name": "NUNKA-BELT-1 Site 17",
          "resources": {
            "carbon": {
              "availability": "high",
              "depletion_pct": 100,
              "original": 20,
              "remaining": 0
            },
            "structural": {
              "availability": "moderate",
              "depletion_pct": 0,
              "original": 57,
              "remaining": 57
            }
          },
          "site_index": 17,
          "tracked_by": "00AC58D1"
        }
      ]
    }
  }
}
```

### *scan.started*

A device began scanning a celestial body.

Example

response payload

```
{
  "scan_target": "SOL-4",
  "scan_type": "planet",       // "planet", "moon", or "belt"
  "eta_seconds": 45
}
```

### *search.completed*

A search finished. The report contains the discovered site and its resource totals.

Example

response payload

```
{
  "search_target": "NUNKA-BELT-1",
  "search_type": "belt",
  "report": {
    "site": "NUNKA-BELT-1-SITE-2",
    "resources": {
      "structural": 949,
      "conductive": 63,
      "silicates": 158,
      "carbon": 190,
      "volatiles": 21,
      "rares": 27
    }
  }
}
```

### *search.started*

A device began searching a belt for new resource sites to mine.

Example

response payload

```
{
  "search_target": "SOL-BELT-1",
  "search_type": "belt",
  "eta_seconds": 30
}
```

### *simulation.abandoned*

A simulation was abandoned by the player.

Example

response payload

```
{
  "simulation_id": 17,
  "scenario_code": "mining_rush"
}
```

### *simulation.completed*

A simulation was completed with a score.

Example

response payload

```
{
  "simulation_id": 17,
  "scenario_code": "asteroid_strike",
  "score_seconds": 842,
  "resources_mined": 1200,
  "devices_printed": 4
}
```

### *simulation.expired*

A simulation expired before the player completed it.

Example

response payload

```
{
  "simulation_id": 17,
  "scenario_code": "mining_rush"
}
```

### *simulation.started*

A simulation scenario was started.

Example

response payload

```
{
  "simulation_id": 17,
  "scenario_code": "mining_rush",
  "starting_star": "VIRTSOL"
}
```

### *site.depleted*

A resource site was fully depleted. Note: there is no *site.discovered* event, since a site is always discovered in the *search.completed* event.

Example

response payload

```
{
  "site": "SOL-BELT-1-SITE-12"
}
```

### *story.awakened*

A new replicant was awakened (created via replication).

Example

response payload

```
{
  "new_replicant_code": "8AFE4482",
  "new_replicant_name": "Bob-2",
  "host_device_code": "7FE981A0"
}
```

### *story.hint*

A story hint was triggered, pointing to something interesting.

Example

response payload

```
{
  "hint": "salvage_site",
  "planet": "SOL-4",
  "designation": "SOL-4-SAL-1"
}
```

### *system.body_renamed*

A celestial body was given a custom name via a hub.

Example

response payload

```
{
  "body_type": "planet",
  "designation": "CHAMAKUY-4",
  "new_name": "New Eden"
}
```

### *system.devices_halted*

Devices in a system were halted (e.g. by a hub being destroyed).

Example

response payload

```
{
  "star": "CHAMAKUY",
  "devices_halted": 12
}
```

### *system.entry_point_set*

A hub set the system's entry point for arriving vessels.

Example

response payload

```
{
  "star": "CHAMAKUY",
  "entry_point": "CHAMAKUY-4-L4"
}
```

### *system.object_detected*

A hub or beacon detected an incoming object (asteroid) on a collision course.

Example

response payload

```
{
  "object_designation": "SOL-OBJ-2",
  "size_class": "large",
  "impact_target": "SOL-4",
  "discovery_source": "hub"    // "hub" or "beacon"
}
```

### *teleport.completed*

A replicant arrived at their destination.

Example

response payload

```
{
  "destination_star": "CHAMAKUY",
  "new_host_code": "7FE981A0"
}
```

### *teleport.failed*

A teleport failed because the target matrix was missing on arrival.

Example

response payload

```
{
  "reason": "target matrix missing",
  "target_matrix_code": "8AFE4482"
}
```

### *teleport.started*

A replicant began teleporting to another star system.

Example

response payload

```
{
  "source_star": "SOL",
  "destination_star": "CHAMAKUY",
  "target_matrix_code": "8AFE4482"
}
```

### *trade.completed*

A trade was fulfilled. Both buyer and seller get an event with their respective *role*. The envelope *device_code* and *device_type* identify the trade controller (owned by the seller).

Example

response buyer payload

```
{
  "trade_code": "TRD-007C22",
  "trade_name": "Structural for Volatiles",
  "role": "buyer",
  "rewards_received": {
    "resources": { "structural": 200 },
    "devices": ["A1B2C3D4"]
  }
}
```

response seller payload

```
{
  "trade_code": "TRD-007C22",
  "trade_name": "Structural for Volatiles",
  "role": "seller",
  "remaining_stock": 9,
  "criteria_received": {
    "resources": { "conductive": 10 },
    "devices": []
  }
}
```

### *trade.created*

A new trade was listed at a trading post.

Example

response payload

```
{
  "trade_code": "TRD-007C22",
  "name": "Structural for Volatiles",
  "stock": 10
}
```

### *trade.deleted*

A trade listing was removed.

Example

response payload

```
{
  "trade_code": "TRD-007C22",
  "name": "Structural for Volatiles",
  "remaining_stock": 3
}
```

### *transport.collected*

A transport drone collected resources from a location into its cargo.

Example

response payload

```
{
  "resources": { "structural": 200, "volatiles": 50 },
  "total": 250,
  "cargo_after": 250,
  "cargo_capacity": 500
}
```

### *transport.delivered*

A transport drone delivered resources to a location.

Example

response payload

```
{
  "resources": { "structural": 200 },
  "total": 200,
  "cargo_after": 0,
  "cargo_capacity": 500
}
```

### *travel.arrived*

A device arrived at its destination.

Example

response payload

```
{
  "attached_devices": [
    "8879C667"
  ],
  "destination": "OCRUX-BELT-1",
  "origin": "OCRUX-5",
  "recalling": false,
  "travel_type": "cruise"
}
```

### *travel.cancelled*

A device's travel was cancelled. It will return to its origin.

Example

response payload

```
{
  "travel_type": "surge",
  "origin": "SOL",
  "destination": "CHAMAKUY",
  "return_time_seconds": 60,
  "attached_devices": ["A1B2C3D4"]  // if carrying attached devices
}
```

### *travel.departed*

A device departed for a destination. Fields vary by travel type. Multi-leg journeys include a *legs* array.

Example

response single-leg payload

```
{
  "travel_type": "cruise",     // "surge", "surge_hop", or "cruise"
  "origin": "SOL-4",
  "destination": "SOL-BELT-1",
  "distance_au": 1.82,         // cruise only
  "distance_ly": 4.37,         // surge only
  "travel_time_seconds": 120,
  "arrives_at": "2026-07-20T14:02:00Z",
  "attached_devices": ["A1B2C3D4"]  // if carrying attached devices
}
```

response multi-leg payload

```
{
  "travel_type": "surge",
  "origin": "SOL-4",
  "destination": "CHAMAKUY-3",
  "travel_time_seconds": 185,
  "attached_devices": ["A1B2C3D4"],  // if carrying attached devices
  "legs": [
    { "type": "surge_hop", "to": "SOL-4-L4" },
    { "type": "surge", "to": "CHAMAKUY" },
    { "type": "cruise", "to": "CHAMAKUY-3" }
  ]
}
```

### *triangulation.complete*

A spectral triangulation completed. The *direction* is a 3D vector pointing from the *target* coordinates toward the signal source.

Example

response payload

```
{
  "signature": "a3f7c2e8b1d94f06",
  "target": [5000, 14000, 100],
  "direction": [0.4, 0.9, 0.0]
}
```

### *triangulation.failed*

A triangulation failed because the signature was no longer available when the observation completed.

Example

response payload

```
{
  "signature": "a3f7c2e8b1d94f06",
  "target": [5000, 14000, 100],
  "reason": "signature_not_found"
}
```

### *triangulation.started*

A galactic observatory began a spectral triangulation routine.

Example

response payload

```
{
  "signature": "a3f7c2e8b1d94f06",
  "target": [5000, 14000, 100],
  "completes_at": "2026-08-05T13:30:00Z"
}
```

### *ward.activated*

A system ward was activated, locking down mining and location events for all except the ward owner.

Example

response payload

```
{
  // empty response
}
```

### *ward.deactivated*

A system ward was deactivated, releasing the system lock.

Example

response payload

```
{
  // empty response
}
```
