---
title: "Tutorials"
source_url: "https://replicant.space/docs/tutorials/"
crawled_at: "2026-09-02T20:03:43.474946+00:00"
---

Getting Started

# Tutorials

A guided set of objectives to introduce new players to the game one step at a time. Start by getting to know your devices, then start mining at the belt, then print new devices and try out different game mechanics.

## How it works

When you register, the first tutorial starts automatically. Each tutorial is a list of objectives - scanning a system, deploying a drone, or travelling to a belt. Complete the steps in order to learn the game.

The tutorial system watches what you do and ticks off your objectives as you hit them. Call the tutorials endpoint to see your next step.

## List tutorials

Fetch all tutorials with their current progress.

GET /v1/tutorials   200 OK

```
$ curl https://api.replicant.space/v1/tutorials \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "tutorials": [
    {
      "slug": "bootstrap",
      "name": "Bootstrap",
      "description": "Learn the basics of commanding your replicant.",
      "order": 1,
      "completed": false,
      "current_step": 0,
      "total_steps": 9
    },
    {
      "slug": "exploring_belt",
      "name": "Exploring the Belt",
      "description": "Search for more mining sites and scan the belt.",
      "order": 2,
      "completed": false,
      "current_step": 0,
      "total_steps": 4
    },
    ...
  ]
}
```

## Tutorial detail

Fetch a single tutorial by slug to see the full list of steps, which ones you've completed, and which one is current.

GET /v1/tutorials/{slug}   200 OK

```
$ curl https://api.replicant.space/v1/tutorials/bootstrap \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "slug": "bootstrap",
  "name": "Bootstrap",
  "description": "Learn the basics of commanding your replicant.",
  "current_step": 3,
  "completed": false,
  "steps": [
    {
      "key": "check_messages",
      "description": "Check your messages",
      "hint": "GET /messages to view your inbox.",
      "completed": true,
      "current": false
    },
    {
      "key": "check_events",
      "description": "Check your events feed",
      "hint": "GET /events to see your game activity.",
      "completed": true,
      "current": false
    },
    {
      "key": "scan_system",
      "description": "Scan your new star system.",
      "hint": "POST /replicants/:code/scan to scan the system.",
      "completed": true,
      "current": false
    },
    {
      "key": "check_vessel",
      "description": "Check your vessel and list of stowed devices.",
      "hint": "GET /replicants/:code to see your replicant information.",
      "completed": false,
      "current": true
    },
    ...
  ]
}
```

Each step has a `hint` that tells you which API endpoint to hit. The `current` flag marks the step you're working on.

## The tutorials

1. **Bootstrap** - check your messages and events, scan the system, travel to the belt, deploy drones and start mining resources.
2. **Exploring the Belt** - scan the belt to see detailed numbers and search for additional resource sites.
3. **Exploring the System** - scan planets and moons, discover salvage.
4. **Mining Salvage** - send a drone to a salvage site and deplete it fully.
5. **Moving Resources** - deploy a transport drone and start consolidating resources.
6. **Helping Civilisation** - discover a location event and help a local civilisation.
7. **Returning to Society** - deploy your FTL slingshot and teleport to SOL to join the rest of us!

After completing the final tutorial, your account is released into the wider game. You'll be able to interact with other players, explore the full star catalogue, and engage with the ongoing story.
