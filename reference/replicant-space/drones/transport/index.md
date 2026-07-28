---
title: "Transport drone"
source_url: "https://replicant.space/docs/drones/transport/"
crawled_at: "2026-07-28T00:53:12.142357+00:00"
---

Drones

# Transport drone

Move resources between locations. Three sizes to choose from depending on how much you're shifting and how far.

## Sizes

| Type | Cargo capacity |
| --- | --- |
| Transport Drone | 20 |
| Transport Hauler | 80 |
| Cargo Freighter | 500 |

The Cargo Freighter also carries a surge drive, making it the only transport in this family that's suitable for interstellar routes on its own.

## Interstellar transport without a freighter

If you'd rather not run freighters, deploy [surge plates](../../interstellar/moving-devices/index.md#taxi-plates) in the source and destination systems tagged `taxi`. Transport drones and haulers will hop between them for the interstellar leg of the route.

## Commands

Transports take two commands: `collect_resources` at the source and `deposit_resources` at the destination. Send the drone to the relevant location first.

POST /v1/devices/{code}   200 OK

```
# pick up specific resources from the current location
$ curl -X POST https://api.replicant.space/v1/devices/7FE981A0 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "collect_resources", "resources": {"carbon": 50, "structural": 30}}'
```

The `resources` object is a per-type breakdown of what to load from the local stockpile.

POST /v1/devices/{code}   200 OK

```
# dropping the whole cargo - omit resources to empty the hold
$ curl -X POST https://api.replicant.space/v1/devices/7FE981A0 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "deposit_resources"}'
```

For deposits, the `resources` field is optional. Pass a breakdown to drop specific amounts, or omit it to empty the entire cargo hold at the current location.

## Running a fleet

Once you've got more than a few transport drones running routes, hand them off to a [Transport Controller](../../ami/transport-controller/index.md) to keep your transport routes and stockpiles organised automatically.
