---
title: "Maintenance drone"
source_url: "https://replicant.space/docs/drones/maintenance/"
crawled_at: "2026-09-02T20:03:42.613371+00:00"
---

Drones

# Maintenance drone

Space is empty, kinda. Micrometeorites, dust, radiation and gravitational stress all chip away at your devices. Maintenance drones cruise around the system patching them up before they fail.

## Wear and tear

Every device takes wear over time, at a rate that depends on what it's doing and where it is. Mining drones grinding through a belt wear faster than a survey drone parked at a Lagrange point. As devices lose operational capacity, they slow down - whether that's mining speed, scan duration, cruise engine power - devices will eventually degrade to stop working.

## Patrol mode

Once you've printed a `maintenance_drone`, put it into patrol mode and it'll run autonomously - cruising to any of your damaged devices in the system and repairing them in turn.

POST /v1/devices/{code}   200 OK

```
# put a freshly printed maintenance drone on patrol
$ curl -X POST https://api.replicant.space/v1/devices/M0123456 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "set_directive", "directive": "patrol"}'
```

The drone picks the most damaged device, cruises to it, deactivates it, and repairs it back to 100% operational capacity. Once fully repaired, it reactivates the device and moves on to the next worst one.

## Service bots

Bill developed a more advanced repair bot called the *service bot*. Where a patrol drone fully repairs one device at a time, the service bot takes a different approach - it performs hot repairs without deactivation, bringing each device up to a functional level before moving on to the next. This makes it ideal for servicing a fleet of broken devices, prioritising getting everything back into operating range rather than perfecting one device while the rest sit broken.

The blueprint isn't on the standard print catalogue - you'll need to track down the Bill NPC to get hold of it.

## System hubs

System Hubs have special maintenance requirements. Once deployed and activated, they require regular supply of specific resources to help with maintenance. The high power requirements and heavy equipment take a lot of damage over time. See the documentation on [system hubs](../../system-hubs/index.md) for more details on the mechanics behind this.

## Vessel self-repair

Vessels with the *cradle* and *print* features can repair themselves without a maintenance drone. When a vessel drops below 30% operational capacity, it begins minor self-repairs at a rate of 1% per hour. It's slow, but if you're stranded in a system with a broken ship and no drone support, you can just wait it out.
