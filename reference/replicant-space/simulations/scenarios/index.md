---
title: "Scenarios"
source_url: "https://replicant.space/docs/simulations/scenarios/"
crawled_at: "2026-07-28T00:53:12.501250+00:00"
---

Simulations

# Scenarios

Each simulation scenario has a predefined configuration with an objective, a time limit, and an entry cost. View what's available, understand the objectives, and choose which one to run.

## Listing scenarios

Different datacentres may offer different scenarios. View the available scenarios available from the replicant interface by calling the interface's simulate endpoint.

GET /v1/devices/{interface_code}/simulate   200 OK

```
$ curl GET https://api.replicant.space/v1/devices/A1B2C3D4/simulate \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "scenarios": [
    {
      "code": "mining_rush",
      "name": "Mining Rush",
      "description": "Mine 1,000 resources as fast as you can.",
      "long_description": "A small cluster of ~10 stars with you in the middle...",
      "version": 1,
      "objective_type": "yield_quota",
      "objective_target": 1000,
      "entry_cost": { "device_type": "compute_core", "quantity": 1 },
      "timeout_hours": 24
    },
    {
      "code": "mining_sprint",
      "name": "Mining Sprint",
      "description": "Mine 10,000 resources. Scale up or burn out.",
      "long_description": "A small cluster of ~10 stars around you...",
      "version": 1,
      "objective_type": "yield_quota",
      "objective_target": 10000,
      "entry_cost": { "device_type": "compute_core", "quantity": 2 },
      "timeout_hours": 48
    },
    // ... more scenarios
  ]
}
```

## Available scenarios

Different scenarios are available at different datacentre locations. The following table lists the scenarios currently available in the game, along with their objectives, entry costs, and time limits.

| Scenario | Objective | Cost | Time limit |
| --- | --- | --- | --- |
| **Mining Rush** | Mine 3,000 total resources | 1 compute core | 4 hours |
| **Mining Sprint** | Mine 10,000 total resources | 2 compute cores | 8 hours |
| **Resource Hunter** | Accumulate 5,000 resources across all locations | 1 compute core | 8 hours |
| **Hunter Gatherer** | Gather 3,000 resources at a single location | 1 compute core | 8 hours |

These scenarios are currently available at the **MIRFAKA-OBJ-1** datacentre. Additional datacentres with different scenario selections will be available in future.

## Entry cost

Simulations cost *compute* devices (compute_core, processing_array, etc) to enter. The required number of devices must be deployed at the same location as the replicant interface before starting the simulation.

## Starting loadout

Every scenario gives you a fresh set of devices to work with. You'll usually get a heaven vessel and some drones. You can't access anything in the real world while plugged in.

You will appear at the centre of a virtual star cluster. Depending on the scenario, some or all stars will have asteroid belts. Some scenarios may also guarantee salvage sites nearby.

## Speed modifiers

Scenarios run faster than the real world. Travel, mining, scanning, and printing will all operate at higher speeds. This is so you can focus on your bootstrap logic without waiting around too long for prints and scans to finish.
