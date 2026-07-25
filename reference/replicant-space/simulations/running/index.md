---
title: "Running a simulation"
source_url: "https://replicant.space/docs/simulations/running/"
crawled_at: "2026-07-24T18:16:35.518185+00:00"
---

Simulations

# Running a simulation

Enter a simulation by posting to the interface with the scenario and replicant code. The replicant matrix is plugged into the interface, a virtual star region is generated, and the clock starts ticking.

## Entering a simulation

POST /v1/devices/{interface_code}/simulate   200 ok

```
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4/simulate \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "scenario": "mining_rush",
      "replicant_code": "7A303E2A"
    }'
```

response response

```
{
  "simulation_id": 42,
  "scenario_code": "mining_rush",
  "scenario_name": "Mining Rush",
  "starting_star": "VIRTUHOMAM",
  "starting_location": "VIRTUHOMAM-3-L4",
  "seed": 2043,
  "devices": [
    { "device_code": "SIM8F2A1", "device_type": "heaven_vessel" },
    { "device_code": "SIM3B7C2", "device_type": "mining_drone" },
    { "device_code": "SIM3B7C3", "device_type": "mining_drone" },
    { "device_code": "SIM3B7C4", "device_type": "mining_drone" },
    { "device_code": "SIM9D4E5", "device_type": "survey_drone" }
  ],
  "timeout_hours": 24
}
```

The response gives you the `simulation_id` (used to abandon later), the virtual starting star, and the full list of devices in your starting loadout. Keep track of the simulation ID.

## Restrictions during a simulation

While plugged in, your replicant is fully inside the simulation. Their vessel stays at the interface location in the real world, and they cannot interact with anything outside the virtual region.

### Device access

Any devices under your replicant's control in the real world are now out of range. Your replicant can't operate them while plugged in. If you have [replicant cooperation](../../api/accounts/index.md) enabled, your other replicants can continue to use your devices during the run.

### Progression

Simulations do not award XP, do not contribute to achievements, and do not track interstellar distance. You can print anything from your known blueprints, but you won't gain the XP.

## Viewing active simulations

Check what simulations are currently running on the interface. This shows your own active simulations along with any other players' in progress.

GET /v1/devices/{interface_code}/simulate/active   200 OK

```
$ curl GET https://api.replicant.space/v1/devices/A1B2C3D4/simulate/active \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "simulations": [
    {
      "simulation_id": 42,
      "scenario_code": "mining_rush",
      "scenario_name": "Mining Rush",
      "replicant_name": "Bob",
      "started_at": "2026-07-10T14:30:00",
      "is_mine": true,
      "timeout_hours": 24
    },
    {
      "simulation_id": 69,
      "scenario_code": "resource_hunter",
      "scenario_name": "Resource Hunter",
      "replicant_name": "Alice-7",
      "started_at": "2026-07-10T12:15:00",
      "is_mine": false,
      "timeout_hours": 24
    }
  ]
}
```

The `is_mine` flag tells you which simulations belong to your account. Other players' simulations show their replicant name and scenario but you cannot interact with them.
