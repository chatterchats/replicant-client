---
title: "Moving devices"
source_url: "https://replicant.space/docs/interstellar/moving-devices/"
crawled_at: "2026-08-11T15:11:29.874638+00:00"
---

Interstellar Transport

# Moving devices

Most devices don't have their own surge drive. To move one between systems you attach it to a surge-capable carrier, send the carrier, and detach at the other end.

## How it works

Smaller devices like drones and AMI controllers fit in your vessel's stow space and travel with you. Everything bigger needs a ride. The family of surge carriers exists for exactly this. You attach the device, send a travel command, and the attached device travels with it. At the other end, detach and you're done.

## Large devices

Autofactories, galactic observatories, system hubs, and other large devices are too big to attach directly to a surge carrier. To move them between systems, use the `compact` command first. This collapses the device into a transportable state so it can be attached to a carrier like any other device. Once it arrives, issue `unfurl` to reassemble it.

Both commands take 30% of the device's original print time to complete, so plan around the downtime. The device is offline for the entire compact/unfurl process.

## The carrier family

All four carriers share the same `attach`/`travel`/`detach` interface. They differ in how many devices they can couple at once:

| Device | Capacity |
| --- | --- |
| `surge_plate` | 1 device |
| `surge_platform` | 4 devices |
| `surge_carrier` | 9 devices |
| `mobile_fleet` | 36 devices |

The surge plate is the simplest member of the family and the only one that supports [taxi mode](index.md#taxi-plates) (see below). The larger carriers are strictly manual.

## Attaching

Send the `attach` command to the carrier with the target device's code. Both devices need to be at the same location.

POST /v1/devices/{carrier_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SP120A4F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "attach",
      "device": "7FE981A0"
    }'
```

## Travelling

Use the standard `travel` command on the carrier. Every attached device travels with it as a single unit.

POST /v1/devices/{carrier_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SP120A4F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "travel",
      "destination": "BOOP"
    }'
```

## Detaching

Once it arrives, release every attached device with a single `detach` command.

POST /v1/devices/{carrier_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SP120A4F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "detach"}'
```

## Taxi mode (surge plate only)

By default a plate only moves when you tell it to. Add the `taxi` [tag](../../api/devices/tagging/index.md) and it joins the pool of plates that local [transport controllers](../../ami/transport-controller/index.md#ferry) can use for the *ferry* directive. Ensure you have enough surge plates so your transports don't get stranded.

PATCH /v1/devices/{plate_code}   200 OK

```
$ curl -X PATCH https://api.replicant.space/v1/devices/SP120A4F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"configuration": {"add_tags": ["taxi"]}}'
```

Remove the `taxi` tag any time to take the plate out of the rotation.
