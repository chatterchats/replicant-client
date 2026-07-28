---
title: "AMI Overview"
source_url: "https://replicant.space/docs/ami/"
crawled_at: "2026-07-28T00:53:10.283642+00:00"
---

AMI Controllers

# AMI Overview

Artificial Machine Intelligence controllers let you hand off the busywork. Pick a directive, give the controller some drones, and focus on something else.

## What an AMI controller is

Physically, an AMI controller is a floating PC - a stack of networking gear and a generous amount of RAM bolted to a small cruise frame. They don't do work themselves; they coordinate the devices you assign to them. A controller holds a fleet, runs a directive, and reports back when something interesting happens.

Each controller type ships with its own catalogue of advanced directives tuned to a particular kind of work - surveying, mining, transport, fleet movement, and so on. You pick the directive that fits the job, hand the controller a fleet, and it figures out the sequencing.

Controllers can be pre-configured while they are stowed - or while deployed - setting a directive, adopting devices and then launching the swarm.

## Common commands

Every AMI controller responds to the same baseline command set, regardless of type.

- `adopt` - bring a device into this controller's fleet.
- `release` - hand a device back to direct control.
- `launch` - deploy the fleet and start executing the current directive.
- `withdraw` - recall the fleet and pause execution.
- `assemble` - bring the fleet home to the controller's current location without ending the directive. Will attempt to use [taxi plates](../interstellar/moving-devices/index.md#taxi-plates) where possible.

POST /v1/devices/{code}   200 OK

```
# adopt drones into the controller's fleet
$ curl -X POST https://api.replicant.space/v1/devices/SC74F210 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "adopt", "devices": ["A1B2C3D4","E5F60718"]}'
```

## Directive commands

Directives are what make a controller useful. You set one, the controller executes it across the fleet, and you can pause or swap it as needed.

- `set_directive` - configure a new directive and its parameters.
- `clear_directive` - drop the current directive entirely.
- `deactivate` - pause a directive without clearing the configuration.
- `activate` - resume a stopped directive from where it left off.

POST /v1/devices/{code}   200 OK

```
# point a controller at a directive and hand it some drones
$ curl -X POST https://api.replicant.space/v1/devices/SC74F210 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "survey_system",
      "configuration": {
        "planets": "all",
        "moons": "all",
        "recall": true
      }
    }'
```

## Launching your fleet

Controllers can adopt most other non-AMI devices. You can adopt, release and set directives while your devices are stowed in your vessel, then launch the controller and watch it carry out your directive.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SC74F210 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "launch"}'
```

response 200 response

```
{
  "device_code": "175B5AD3",
  "assigned_devices": {
    "already_deployed": [],
    "deployed": [
      "E811348D",
      "6414BB30",
      "ADA90515",
      "494140A7",
      "6DABF041"
    ],
    "failed": [],
    "skipped": []
  },
  "status": "launched"
}
```

When you're done, recall the fleet with the `withdraw` command. Every deployed device is brought back to the controller and any in-flight work (mining, scanning, tracking) is stopped.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SC74F210 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "withdraw"}'
```

response 200 response

```
{
  "device_code": "175B5AD3",
  "assigned_devices": {
    "already_stowed": [],
    "failed": [],
    "recalled": [
      "E811348D",
      "6414BB30",
      "494140A7",
      "ADA90515",
      "6DABF041"
    ],
    "stopped_mining": [],
    "stopped_scanning": [
      "E811348D",
      "6414BB30",
      "494140A7",
      "ADA90515",
      "6DABF041"
    ],
    "stopped_tracking": []
  },
  "status": "withdrawn"
}
```

## Controller types

- [Survey controller](survey-controller/index.md) - coordinate survey drones across a system.
- [Mining controller](mining-controller/index.md) - run a mining operation across multiple sites.
- [Transport controller](transport-controller/index.md) - move resources between locations on a schedule.
- [Fleet controller](fleet-controller/index.md) - move a fleet of surge-capable devices between systems together. Special NPC reward.
