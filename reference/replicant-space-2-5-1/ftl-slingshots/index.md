---
title: "FTL Slingshots"
source_url: "https://replicant.space/docs/ftl-slingshots/"
crawled_at: "2026-08-22T22:43:51.720599+00:00"
---

Infrastructure

# FTL Slingshots

A high-energy subspace transmitter that fires your consciousness to a remote location in a single pulse. No relay network required - just a linked empty matrix at the other end.

## How this differs from teleporting

Standard [teleportation](../api/replicants/teleport/index.md) relies on the FTL relay network. Both the source and destination systems need to be connected with a chain of [FTL relays](../ftl-relays/index.md). That's fine when you have an established network, but useless if you need to reach somewhere remote.

The slingshot bypasses all of that. It spools a tightly focused quantum data stream to a pre-linked empty matrix at a remote location, pushing the full matrix state across in a single high-energy pulse. If the target matrix exists and the slingshot is operational, you're going.

## How do I get one?

New players will find one stowed in their vessel when they start the game. This is preconfigured to an empty replicant matrix in the SOL Oort cloud. There's another one there preconfigured back to your starter vessel. Have fun bouncing back and forth. Although they do explode a little bit when you use them. Plan for maintenance. Decommission at an autofactory to learn the blueprint.

Players who existed before this feature was released can retrieve a one-time slingshot from the SOL-OORT equipment locker.

## Requirements for a slingshot action

- A deployed FTL slingshot at your current location.
- An empty replicant matrix at the destination, stowed in a vessel.
- The slingshot's operational capacity must be at least 80%.

## Linking a target

Before using a slingshot, it needs to be linked to an empty replicant matrix device. The two need to be at the same location when this occurs to form a quantum superpositional bond between the lattices. After that, the two devices can be moved anywhere in the galaxy - the quantum entanglement connection is maintained.

Use the device patch configuration to establish the link.

PATCH /v1/devices/{slingshot_code}   200 OK

```
# link the slingshot to an empty matrix at the destination
$ curl -X PATCH https://api.replicant.space/v1/devices/SL3FA90B \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "configuration": {
        "linked_device": "0799A49D"
      }
    }'
```

response 200 response

```
{
  "device_code": "SL3FA90B",
  "tags": [],
  "linked_device": "0799A49D"
}
```

## Firing the slingshot

To teleport via slingshot, use the standard teleport endpoint but pass the slingshot's device code as the `target` instead of a matrix code. The slingshot resolves the linked matrix automatically.

POST /v1/replicants/{code}/teleport   200 OK

```
# teleport via slingshot - pass the slingshot code as the target
$ curl -X POST https://api.replicant.space/v1/replicants/8AFE4482/teleport \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"target": "SL3FA90B"}'
```

response 200 response

```
{
  "status": "teleporting",
  "source_star": "SOL",
  "destination_star": "POLIBUS",
  "started_at": "2026-05-10T14:30:00+01:00",
  "completes_at": "2026-05-10T14:30:30+01:00",
  "offline_seconds": 30,
  "target_matrix_code": "0799A49D"
}
```

## Maintenance

Each transfer dumps enormous thermal energy through the signal cores. After firing, the slingshot's operational capacity drops to 5%. You'll need a [maintenance drone](../drones/maintenance/index.md) to repair it before the next use - the minimum threshold for firing is 80%.
