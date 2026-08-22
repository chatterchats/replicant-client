---
title: "Blueprints"
source_url: "https://replicant.space/docs/concepts/blueprints/"
crawled_at: "2026-08-07T00:51:31.256273+00:00"
---

Core Concepts

# Blueprints

Everything you can make in the galaxy is described by a blueprint. Blueprints unlock as you play the game and do new things, via the achievement system. Fly around enough and you'll come up with a faster design; have enough resources and you'll work out how to move more, and so on.

## Blueprint families

- **Basic drones** - survey, mining, transport, maintenance.
- **AMI controllers** - scanning, searching, mining strategies, transport routes, fleet control.
- **Interstellar transport** - surge plates and platforms, mobile fleets, cargo freighters.
- **Autofactories** - large in-system manufacturing rigs.
- **System control** - system hubs, FTL relays and beacons, propulsors.
- **Replicant hardware** - matrices, containers, vessels.
- **Other** - lots more blueprints unlocked as needed.

## Starter set

Every new account starts with the same handful of blueprints - enough to bootstrap a basic mining and survey operation, build the first autofactory, and replicate.

| Device | Print time | Carbon | Conductive | Rares | Silicates | Structural | Volatiles |
| --- | --- | --- | --- | --- | --- | --- | --- |
| *ftl_beacon* stow | 1m 40s | - | 15 | 5 | 10 | 20 | - |
| *mining_drone* cruise, mine, stow | 10m | 25 | 50 | - | 25 | 100 | - |
| *survey_drone* cruise, survey, stow | 5m | 10 | 30 | 5 | 15 | 60 | - |
| *compute_core* stow | 3m 20s | 2 | 35 | 15 | 55 | 8 | - |
| *autofactory* cruise, print, modular | 40m | 150 | 400 | 80 | 200 | 800 | 50 |
| *empty_replicant_matrix* stow | 4h | 50 | 400 | 200 | 100 | 200 | 50 |
| *matrix_container* cruise, cradle | 1h 20m | 80 | 200 | 60 | 100 | 400 | 40 |
| *heaven_vessel* surge, cruise, system_scan, mine, cradle, print, census | 8h | 400 | 1000 | 300 | 500 | 2000 | 200 |

## Listing what you have

GET /v1/blueprints   200 OK

```
$ curl https://api.replicant.space/v1/blueprints \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "blueprints": [
    {
      "device_type": "maintenance_drone",
      "short_description": "Repairs your devices once they lose enough operational capacity.",
      "description": "This is a solid metallic box with an extendable pair of sensor lenses at the top and an articulated arm on either side. When it reaches a damaged device, the drone evaluates the repair, powers the target down, and opens its front panel to reveal tools, spare parts and interesting bits of salvage. Its patrol directive works by pinging all devices in the system, then cruising to anything that requires maintenance.",
      "directives": [
        "patrol"
      ],
      "features": [
        "cruise",
        "repair",
        "ami",
        "stow"
      ],
      "print_time": 270,
      "resources": {
        "carbon": 35,
        "conductive": 80,
        "rares": 20,
        "silicates": 40,
        "structural": 150,
        "volatiles": 10
      },
      "attach_capacity": 0,
      "cargo_capacity": 0,
      "stow_capacity": 0
    },
    ...
  ]
}
```

Your listing includes every blueprint that you've unlocked so far. Blueprint knowledge is shared between your replicants, so you only need one of them to unlock something for it to be available for the rest. You start with just a handful at the beginning, but they will quickly unlock.

When you first play the game, use this as a way to learn the core mechanics properly before taking on the big manufacturing projects.

## Learning by taking them apart

We are engineers after all. We can figure out how stuff is made if we get our metaphorical hands on a device and poke at it for long enough. Decommissioning a device at one of your autofactories will automatically give you the blueprint to print more copies, although you do lose the original through this process.

Many different devices are needed to complete megastructures - knowledge can be shared by trading unique devices with other players.

## Print costs and times

Every blueprint declares a resource cost and a base print time. Larger devices take longer to print and tie up your printer for the duration. Your vessel can't do anything else while printing, so you'll want to deploy an autofactory and manage print queues. If you're short on resources at the location, the factory will wait until more are delivered.

System Hubs are particularly complicated to print, and since the calculations to maintain the mesh FTL network between them, the more you make, the longer they take.
