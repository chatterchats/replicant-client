---
title: "Travel"
source_url: "https://replicant.space/docs/concepts/drives/"
crawled_at: "2026-08-07T00:51:31.402349+00:00"
---

Core Concepts

# Travel

There are two ways to travel in the galaxy. Cruise within a system, Surge between. Each has its own physics and its own rules.

## Cruise drives

Cruise drives generate thrust from one side of the device - typically the bottom of your HEAVEN vessel. They are cheap, robust, in-system tech. Internal attitude thrusters drop you out of orbit, the cruise drive applies constant acceleration, and at some point along the journey the device flips and thrusts in reverse to decelerate.

Each cruise trip is composed of four parts:

1. Leave orbit
2. Accelerate to the mid-point
3. Flip and decelerate
4. Enter new orbit

Most devices you construct will have a built-in cruise drive to hold orbital positions and manoeuver between locations in-system.

## Surge drives

Surge drives generate a gravitational field at the front of the device and push on subspace to move between star systems. They *cannot* be used near significant gravity. By convention, there are three surge-capable locations in a system that we find ourselves comfortable using the drive: the Oort cloud, the Kuiper belt, and the system's entry point (see below). Any attempt to travel with a surge-capable device from a different location will first involve a cruise to the nearest surge point.

### Entry points

First arrival into a system drops you at the Oort cloud (or Kuiper belt for younger stars). After scanning, you'll discover the system's entry point. This is usually the L5 Lagrange point of a planet close to the asteroid belt. Once you've planted a System Hub, future surges arrive there directly with assistance from braking lasers and navigational sync.

### The "Surge Hop"

Travelling between two surge-capable locations in the same system is achieved with a little trick that an early replicant learned, where we can fire up the Surge drive in a carefully calculated short-lived burst to cover the distance. Most commonly used when arriving in a system and wanting to travel to the belt. A quick hop to the nearby L5 entry point then a short cruise to the belt.

## Multi-hop travel

Your vessel has two drives. You don't need to worry about which one to use, the internal systems handle that for you. If you want to travel from `PLOMBO-1` to `SCARAB-6`, you can just set your destination and the navigation system will calculate and execute flight plan for you. The only time you'll care about this detail is if you cancel your travel mid-flight. You'll end up at the nearest hop on the trip.

If for some reason you want to take a specific route, you can supply a list of locations to the navigation with `"via": [ "SOL-1", "SOL-2", ...]`

## Examples

Send the `travel` command to a device to move it to a new location. The response gives you the full route the navigation system worked out, so you can see each leg and how long it'll take.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/A397C84B \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "travel",
      "destination": "ZHANGA-1"
    }'
```

response travel response

```
{
  "device_code": "A397C84B",
  "departed_at": "2026-05-22T20:05:46+01:00",
  "arrives_at": "2026-05-22T20:06:44+01:00",
  "origin": "ZHANGA-BELT-1",
  "destination": "ZHANGA-1",
  "destination_type": "planet",
  "route": [
    {
      "active": true,
      "distance_au": 2.899,
      "from": "ZHANGA-BELT-1",
      "leg": 1,
      "time_seconds": 57.6,
      "to": "ZHANGA-1",
      "type": "cruise"
    }
  ],
  "status": "travel_initiated",
  "total_time_seconds": 57.6,
  "progress_percent": 0.0,
}
```

Or kick off a trip for a replicant with a single call.

POST /v1/replicants/{code}/travel   200 OK

```
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/travel \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"destination": "TARAZEDAR-2"}'
```

response travel response

```
{
  "departed_at": "2026-05-10T17:04:17+01:00",
  "arrives_at": "2026-05-10T17:05:00+01:00",
  "origin": "TARAZEDAR-KUIPER",
  "destination": "TARAZEDAR-2",
  "destination_type": "planet",
  "route": [
    {
      "leg": 1,
      "active": true,
      "from": "TARAZEDAR-KUIPER",
      "to": "TARAZEDAR-4-L4",
      "time_seconds": 30,
      "type": "surge_hop"
    },
    {
      "leg": 2,
      "from": "TARAZEDAR-4-L4",
      "to": "TARAZEDAR-2",
      "time_seconds": 12.7,
      "type": "cruise"
    }
  ],
  "status": "travel_initiated",
  "total_time_seconds": 42.7,
  "progress_percent": 12.1
}
```
