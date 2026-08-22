---
title: "Devices"
source_url: "https://replicant.space/docs/concepts/devices/"
crawled_at: "2026-08-22T22:43:51.371248+00:00"
---

Core Concepts

# Devices

Devices are the physical things your replicant builds and controls - drones, vessels, hubs, plates. Everything you print is a device, and every device responds to commands.

## Commanding devices

Send a command by POSTing to `/v1/devices/<code>` with a `command` field and whatever arguments that command needs. Below, a survey drone is dispatched to Earth.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "travel", "destination": "SOL-3"}'
```

## Available commands

The set of commands a device accepts depends on its type and current state. Hit `GET /v1/devices/<code>` and you'll get an `available_commands` array listing everything it will respond to right now. Trying to invoke a command outside that list will get you a `400`.

## Stowing and transport

Not all devices can be stowed inside a vessel. Smaller drones and matrices fit in your HEAVEN's stow bay; larger devices like system hubs and cargo freighters won't. To [move those between systems](../../interstellar/moving-devices/index.md) you'll need to attach them to a surge-capable carrier and command the carrier itself to travel.

The largest devices - autofactories, system hubs and galactic observatories - are too big for direct attachment. They are often spread out into multiple interconnected modules. For example, the autofactory is actually a manufacturing operation comprised of warehouses, resource silos and various fabrication components. These devices have the `modular` feature, letting you `compact` them for transport and `unfurl` them at the destination.

## Printing constraints

Your onboard printer handles the basics - mining drones, survey drones, FTL beacons. Bigger or more specialised devices require an autofactory: a dedicated industrial complex that you can print yourself once you've gathered enough resources.
