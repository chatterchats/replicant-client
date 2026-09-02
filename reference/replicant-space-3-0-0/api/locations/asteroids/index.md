---
title: "Asteroids"
source_url: "https://replicant.space/docs/api/locations/asteroids/"
crawled_at: "2026-09-02T20:03:41.154110+00:00"
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
$ curl https://api.replicant.space/v1/locations/DELTA-OBJ-3 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "location": "DELTA-OBJ-3",
  "location_type": "object",
  "object": {
    "active_propulsors": 1,
    "approach_angle": 8.7,
    "approach_speed": 0.79,
    "composition": "carbonaceous",
    "current_thrust_per_hour": 4.0,
    "designation": "DELTA-OBJ-3",
    "discovered_at": "2026-09-01T22:18:55+01:00",
    "impact_eta": "2026-09-02T22:03:55+01:00",
    "impact_likelihood": 100.0,
    "impact_target": "DELTA-3",
    "mass_class": "large",
    "object_type": "incoming_asteroid",
    "orbital_distance_au": 35.71,
    "progress_pct": 0.0,
    "required_strength": 168.0,
    "status": "active"
  }
}
```

## Diversion

If `impact_target` is set, the asteroid is on a collision course with that body. `required_strength` tells you how much deflection effort is needed to push it clear - this scales with the asteroid's `mass_class` and how close `impact_eta` is. You'll want to spot this early if you don't have good manufacturing nearby.

Bring propulsor devices to the asteroid's location, deploy them, and activate them. Each running propulsor plate accumulates thrust over time.

Once detected, the impact likelihood will be set to 100%, and as time goes by, with propulsors diverting, the likelihood will reduce. Once it drops to 0%, rewards will be granted.

## Rewards

A successful diversion grants all replicants that helped a permanent mining bonus in the system. The civilisations affected were so chuffed to avoid potential extinction, they broadcasted some hints 'n tips on the belt.

Oh by the way, there might be more coming... If you successfully divert an asteroid, you might want to drop an FTL beacon at the intended impact location. Fate has a funny way of playing with coincidence. Successful diversion bonuses stack.

## Intentional impacts

Not all asteroids are threats - some are opportunities. Kuiper Belt objects can be found using the `detect_object` command on a sensor array or vessel, then deliberately launched at a planet using a **vector charge**. Once inbound, propulsors can `increase` or `decrease` the rock's speed, while trajectory deflectors can `steepen` or `shallow` the approach angle. The same propulsor `diversion` mode used for defensive asteroid diversion also works here if you change your mind.

Different compositions (ice, rock, carbonaceous, metallic, mixed) produce different effects on impact, and the approach angle matters - a steep impact hits the surface while a shallow one skims through the atmosphere. On airless bodies, a shallow approach is a fly-by. See [dropping rocks](../terraforming/impacts/index.md) for the full mechanics, composition tables, and API examples.
