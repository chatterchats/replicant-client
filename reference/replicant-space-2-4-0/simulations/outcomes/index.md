---
title: "Outcomes & history"
source_url: "https://replicant.space/docs/simulations/outcomes/"
crawled_at: "2026-08-07T00:51:31.942769+00:00"
---

Simulations

# Outcomes & history

A simulation ends one of three ways: you complete the objective, you abandon it, or it times out. In all cases your replicant is restored to the real world and the virtual region is cleaned up.

## Completion

When you meet the scenario's objective (e.g. mine 1,000 resources), the simulation completes automatically. Your time is recorded as *score_seconds*. This is the time taken from entry to completion. The system also tracks how many resources you mined and how many devices you printed during the run.

Faster completion times rank higher on the [leaderboards](../leaderboards/index.md). You'll receive a message with your finishing time and how it compares to your personal best.

## Abandoning

If you want to quit a simulation early, send a `DELETE` request to the interface with the simulation ID you received when you entered.

DELETE /v1/devices/{interface_code}/simulate/{simulation_id}   200 OK

```
$ curl -X DELETE https://api.replicant.space/v1/devices/A1B2C3D4/simulate/42 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "status": "abandoned",
  "simulation_id": 42
}
```

Abandoned simulations do not record a score and do not appear on the leaderboard. The entry cost is not refunded.

## Timeout

Each scenario has a time limit. If the clock runs out before you complete the objective, the simulation wraps up and your replicant is booted out.

Like abandonment, timed-out simulations do not record a score. You'll receive a message letting you know the run expired.

## What happens on exit

Regardless of how the simulation ends:

- Your replicant is unplugged from the interface, restored to their original vessel.
- All simulation devices (your starting loadout and anything you printed) vanish.
- The virtual star region and everything in it is cleaned away.
- Any in-progress actions (printing, travel, mining) are cancelled.

## Simulation history

View all your past simulation runs. The good, the abandoned, and the timed out.

GET /v1/accounts/simulations   200 OK

```
$ curl GET https://api.replicant.space/v1/accounts/simulations \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "simulations": [
    {
      "id": 42,
      "scenario_code": "mining_rush",
      "scenario_name": "Mining Rush",
      "started_at": "2026-07-10T14:30:00",
      "completed_at": "2026-07-10T15:12:34",
      "abandoned_at": null,
      "timed_out_at": null,
      "score_seconds": 2554,
      "resources_mined": 1247,
      "devices_printed": 4
    },
    {
      "id": 38,
      "scenario_code": "hunter_gatherer",
      "scenario_name": "Hunter Gatherer",
      "started_at": "2026-07-09T10:00:00",
      "completed_at": null,
      "abandoned_at": "2026-07-09T11:45:00",
      "timed_out_at": null,
      "score_seconds": null,
      "resources_mined": 612,
      "devices_printed": 2
    }
  ]
}
```
