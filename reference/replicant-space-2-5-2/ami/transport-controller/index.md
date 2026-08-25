---
title: "AMI Transport Controller"
source_url: "https://replicant.space/docs/ami/transport-controller/"
crawled_at: "2026-08-25T15:34:30.272693+00:00"
---

AMI Controllers

# AMI Transport Controller

Hand off transport drones and the controller handles the routes - resource trades, continuous supply lines, interstellar deliveries, or system-wide clean-up jobs.

## Directives

- `delivery` - move a specific quantity of resources from one location to another, then stop.
- `shuttle` - continuously move all resources between two in-system locations.
- `ferry` - the interstellar version of shuttle, between locations in different systems.
- `consolidate` - sweep resources from everywhere in the system to a single delivery location.

## Configuring each directive

### Delivery

The most convenient directive for one-shot trades or completing location events. Define collection and delivery locations and the exact amounts to be moved. Once the requirement is satisfied the controller deactivates.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/TC04F100 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "delivery",
      "configuration": {
        "route": {
          "collect": "SOL-BELT-1",
          "deliver": "SOL-3-L4"
        },
        "requirement": {
          "carbon": 50,
          "silicates": 100
        }
      }
    }'
```

### Shuttle

A continuous in-system supply line - useful for ferrying mined resources from a belt to an autofactory location, for example. Provide an optional `priority` list of resources to move first if you need them sooner.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/TC04F100 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "shuttle",
      "configuration": {
        "collect": "SOL-BELT-1",
        "deliver": "SOL-3-L4",
        "priority": ["carbon","rares"]
      }
    }'
```

### Ferry

The interstellar variant of shuttle. Same configuration shape, with the collection and delivery locations in different systems. Requires the two systems to be linked over the [FTL network](../../ftl-relays/index.md). Cruise transports without their own surge drive will need a supply of [surge plates](../../interstellar/moving-devices/index.md#taxi-plates) tagged `taxi` at both ends of the route.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/TC04F100 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "ferry",
      "configuration": {
        "collect": "TARAZEDAR-BELT-1",
        "deliver": "SOL-3-L4",
        "priority": ["rares","conductive"]
      }
    }'
```

### Consolidate

For cleaning up a system with resources scattered all over the place. The controller identifies every location with available resources and dispatches transports to bring them back to the delivery location. Drones will hop between source locations to fill up before returning, so capacity isn't wasted on half-empty trips.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/TC04F100 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "consolidate",
      "configuration": {
        "deliver": "SOL-3-L4",
        "priority": []
      }
    }'
```

## How directives work

Every tick, the controller reviews the available drones and the configured directive, then issues the right sequence of collect and deposit commands to keep the route flowing. Drones added to the controller during operation are picked up automatically on the next tick.
