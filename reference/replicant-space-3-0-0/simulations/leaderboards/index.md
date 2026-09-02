---
title: "Leaderboards"
source_url: "https://replicant.space/docs/simulations/leaderboards/"
crawled_at: "2026-09-02T20:03:43.094076+00:00"
---

Simulations

# Leaderboards

Simulation leaderboards rank players by their fastest completion time. Each account's personal best gets one entry per scenario.

## Leaderboard index

Get a summary of all scenarios that have been completed at least once, with the total number of completions and the current best time.

GET /v1/leaderboards/simulations   200 OK

```
$ curl GET https://api.replicant.space/v1/leaderboards/simulations
```

response response

```
{
  "scenarios": [
    {
      "scenario_code": "mining_rush",
      "scenario_name": "Mining Rush",
      "completions": 87,
      "best_time_seconds": 1842
    },
    {
      "scenario_code": "mining_sprint",
      "scenario_name": "Mining Sprint",
      "completions": 23,
      "best_time_seconds": 14220
    }
  ]
}
```

The `best_time_seconds` is the fastest completion across all players for that scenario. Only scenarios with at least one completion appear in the list.

## Per-scenario leaderboard

Get the top 10 personal bests for a specific scenario. Pass the scenario code in the URL.

GET /v1/leaderboards/simulations/{scenario_code}   200 OK

```
$ curl GET https://api.replicant.space/v1/leaderboards/simulations/mining_rush
```

response response

```
{
  "scenario_code": "mining_rush",
  "scenario_name": "Mining Rush",
  "entries": [
    {
      "rank": 1,
      "replicant_code": "7A303E2A",
      "name": "Bob",
      "score_seconds": 1842,
      "resources_mined": 1031,
      "devices_printed": 2,
      "completed_at": "2026-07-08T09:22:14"
    },
    {
      "rank": 2,
      "replicant_code": "C4D5E6F7",
      "name": "Alice-3",
      "score_seconds": 2104,
      "resources_mined": 1155,
      "devices_printed": 5,
      "completed_at": "2026-07-09T16:45:30"
    }
  ]
}
```

## Scoring

Entries are ranked by `score_seconds`. This is the time in seconds from entering the simulation to completing the objective. Lower is better. If two players have the same time, the earlier completion date wins.

Each account appears at most once per scenario. If you complete the same scenario multiple times, only your fastest run is shown. The leaderboard also reports `resources_mined` and `devices_printed` for context on how the run was played.

## Public access

Leaderboard endpoints do not require authentication.
