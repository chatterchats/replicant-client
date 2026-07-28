---
title: "Asteroids"
source_url: "https://replicant.space/docs/api/locations/asteroids/"
crawled_at: "2026-07-28T00:53:11.230763+00:00"
---

API · Locations

# Asteroids

An asteroid is a system object like any other location. The interesting ones are on a collision course with a populated body - divert them in time and you'll be thanked for it.

## Endpoint

`GET /v1/locations/{code}`

Asteroids are system objects (see [locations](../../../concepts/locations/index.md) for the wider model). Scan a system and any inbound asteroids show up alongside planets, moons and belts.

## Example

GET /v1/locations/{code}   200 OK

```
$ curl https://api.replicant.space/v1/locations/MUROPE-OBJ-1 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "location": "MUROPE-OBJ-1",
  "location_type": "object",
  "object": {
    "active_plates": 6,
    "current_thrust_per_hour": 6.0,
    "designation": "MUROPE-OBJ-1",
    "discovered_at": "2026-06-06T20:50:47+01:00",
    "impact_eta": "2026-06-09T20:50:47+01:00",
    "impact_likelihood": 99.5,
    "impact_target": "MUROPE-3",
    "object_type": "incoming_asteroid",
    "orbital_distance_au": 2.5,
    "progress_pct": 0.5,
    "required_strength": 48.0,
    "size_class": "medium",
    "status": "active"
  }
}
```

## Diversion

If `impact_target` is set, the asteroid is on a collision course with that body. `required_strength` tells you how much deflection effort is needed to push it clear - this scales with the asteroid's `size_class` and how close `impact_eta` is. You'll want to spot this early if you don't have good manufacturing nearby.

Bring propulsor devices to the asteroid's location, deploy them, and activate them. Each running propulsor plate accumulates thrust over time.

Once detected, the impact likelihood will be set to 100%, and as time goes by, with propulsors diverting, the likelihood will reduce. Once it drops to 0%, rewards will be granted.

## Rewards

A successful diversion grants all replicants that helped a permanent mining bonus in the system. The civilisations affected were so chuffed to avoid potential extinction, they broadcasted some hints 'n tips on the belt.

Oh by the way, there might be more coming... If you successfully divert an asteroid, you might want to drop an FTL beacon at the intended impact location. Fate has a funny way of playing with coincidence. Successful diversion bonuses stack.
