---
title: "Dropping rocks"
source_url: "https://replicant.space/docs/api/locations/terraforming/impacts/"
crawled_at: "2026-09-02T20:03:41.430057+00:00"
---

Terraforming

# Dropping rocks

Why build when you can throw? Redirect Kuiper Belt objects toward a planet to reshape its environment on impact. Crude, fast, and surprisingly useful.

## Overview

Terraforming devices are precise but slow. Sometimes you need a faster approach - find a rock in the outer system, point it at a planet, and let physics do the heavy lifting. An ice body slammed into a dry world delivers water and atmosphere in one violent stroke. A fast rocky impactor can crack open a geologically dead crust and kick-start tectonic activity.

## Finding a rock

Kuiper Belt objects are not visible by default. You need to actively search for them using the `detect_object` command on any device with the `system_scan` feature - that means *vessels* or the *sensor array*.

The sensor array is the faster option. It sweeps the outer system from a fixed position using long-range optics. Vessels can also detect objects, but they do it the hard way - physically cruising the outer system and scanning as they go. Faster vessels cover more ground and find rocks quicker, but even a fast ship is slower than a sensor array.

Once detected, a Kuiper object appears as a location in the system for *24 hours*. After that window it drifts beyond useful range and disappears from your scan data. If you want the rock, act quickly.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/F652A584 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "detect_object"}'
```

response response

```
{
  "completes_at": "2026-09-01T23:03:53+01:00",
  "detect_target": "DELTA-KUIPER",
  "device_code": "F652A584",
  "eta_seconds": 3600,
  "started_at": "2026-09-01T22:03:53+01:00",
  "status": "detect_started"
}
```

Once detection completes, the Kuiper Belt location response includes the objects found:

response response

```
"kuiper_objects": [
  {
    "composition": "carbonaceous",
    "designation": "DELTA-KUIPER-001",
    "discovered_at": "2026-09-01T21:04:00.837309Z",
    "mass_class": "large"
  }
]
```

## Launching

Deploy a *vector charge* at the Kuiper object's location. Issue the `detonate` command with a `target` (the Kuiper object designation) and a `destination` (any planet or moon in the same system). The charge is a single-use directional array - it attaches to the rock, fires a precisely timed burst, and kicks the body onto a new trajectory. The charge is consumed on detonation.

The response includes the initial `approach_angle` and `approach_speed`, which you can modify before impact using propulsors and trajectory deflectors. The rock becomes a trackable system object at `object_designation` and you can follow its progress via `diversion.detected` and `diversion.impacted` events.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/FC838B3F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "detonate", "destination": "DELTA-3", "target": "DELTA-KUIPER-001"}'
```

response response

```
{
  "approach_angle": 8.7,
  "approach_speed": 0.79,
  "composition": "carbonaceous",
  "destination": "DELTA-3",
  "device_code": "FC838B3F",
  "impact_eta": "2026-09-02T21:03:55.524215",
  "kuiper_object": "DELTA-KUIPER-001",
  "mass_class": "large",
  "object_designation": "DELTA-OBJ-3",
  "status": "launched"
}
```

## Controlling the approach

Once a launched rock is inbound, deploy *propulsors* and *trajectory deflectors* at the system object location and activate them to control the speed, angle, or diversion of the asteroid. Controlling an asteroid is slow work - effects accumulate over time, so deploy early.

Propulsors can be set to one of three directions. The default is `diversion`, which pushes the asteroid off course to avoid impact entirely - the same behaviour used for defensive asteroid diversion. Set the direction to `increase` or `decrease` to modify both the impact ETA and the approach speed over time.

Trajectory deflectors are more sensitive instruments. Set the direction to `steepen` or `shallow` to modify the approach angle over time. A steep approach (above 30°) drives the rock into the surface - good for delivering mass and triggering tectonic effects. A shallow approach (below 30°) skims it through the upper atmosphere, dispersing material across a wide area with less surface damage. On worlds with no atmosphere, a shallow approach is equivalent to a very narrow diversion fly-by.

The combination of speed and angle determines the outcome. Fast rocks hit harder; slow rocks are gentler but still effective. See the composition tables below for the specific effects of each approach.

## Composition

Every Kuiper object has a composition that determines what it delivers on impact. The five types and their effects:

### Ice

The most useful rock for early-stage terraforming. Ice bodies deliver water and cool the surface on impact. A slow, steep ice impact is the fastest way to bootstrap a hydrosphere on a dry world. Fast impacts are more violent but deliver even more water, at the cost of tectonic disruption.

| Approach | Effects (base) |
| --- | --- |
| Slow + shallow | Temp -3, Pressure +0.5 |
| Slow + steep | Hydro +6, Temp -2 |
| Fast + shallow | Temp -6, Pressure +1.0, Oxygen +0.5 |
| Fast + steep | Hydro +10, Tectonic +4, Temp -2 |

### Rock

Rocky impacts heat the surface and disturb the crust. Shallow approaches dump kinetic energy into the atmosphere as heat. Steep impacts crack the surface and spike tectonic activity - useful for jump-starting a geologically dead world, but devastating to an existing biosphere.

| Approach | Effects (base) |
| --- | --- |
| Slow + shallow | Temp +3 |
| Slow + steep | Tectonic +4 |
| Fast + shallow | Temp +6, Toxicity +2, Pressure +0.5 |
| Fast + steep | Tectonic +10, Temp +6, Biosphere -5 |

### Carbonaceous

Carbon-rich bodies carry organic compounds and trapped volatiles. Angle doesn't matter much - the material disperses on entry either way. Slow impacts seed the surface with organic precursors that boost biosphere development. Fast impacts blast the volatiles into the atmosphere, raising pressure and oxygen while scrubbing some toxicity.

| Approach | Effects (base) |
| --- | --- |
| Slow (any angle) | Biosphere +2, Pressure +0.3 |
| Fast (any angle) | Pressure +1.0, Oxygen +1.0, Toxicity -1 |

### Metallic

Dense metallic bodies punch through whatever they hit. Speed and angle make no difference - the impact is always a seismic event. Useful only if you specifically want to raise tectonic activity. Otherwise, avoid.

| Approach | Effects (base) |
| --- | --- |
| Any | Tectonic +10 |

### Mixed

A grab bag of ice, rock, and other material. Mixed bodies produce a spread of moderate effects across multiple attributes. Less focused than a pure composition, but useful when you need to nudge several things at once.

| Approach | Effects (base) |
| --- | --- |
| Slow + shallow | Temp +1, Pressure +0.2, Hydro +1 |
| Slow + steep | Tectonic +2, Hydro +2 |
| Fast + shallow | Temp +3, Pressure +0.5, Oxygen +0.3 |
| Fast + steep | Tectonic +5, Temp +2, Hydro +3 |

## Mass scaling

All effects in the tables above are base values for a **small** Kuiper object. Larger rocks hit proportionally harder:

| Mass class | Effect multiplier |
| --- | --- |
| Small | 1.0x |
| Medium | 2.5x |
| Large | 5.0x |
| Giant | 10.0x |

A fast steep impact from a giant ice body delivers Hydro +100 and Tectonic +40. That will transform a world in a single hit - or destroy everything living on it. Size your rock to the job.

## Airless bodies

On worlds with atmospheric pressure below **0.01 atm**, there's no atmosphere to skim through. A shallow approach on an airless body is equivalent to a very narrow diversion fly-by - the rock skims past without impacting. If you're targeting an airless world, steepen the approach or you'll waste the rock.

## Tips

- Deploy a [Terraform Monitor](../components/index.md) at the target world before the impact so you can see the attribute changes in real time.
- Ice rocks are your best friend for dry worlds. A couple of medium ice impacts can deliver more water than hours of aquifer tap operation.
- Be careful with fast steep rocky impacts near established biospheres - the tectonic spike and biosphere damage can undo a lot of work.
- Carbonaceous rocks are angle-independent. Don't waste trajectory deflectors on them.
- The 24-hour detection window means you need a vector charge printed and ready before you start scanning. Don't find the perfect rock and then spend hours building the charge to launch it.
- You can launch multiple rocks at the same target. Effects stack, but so do the consequences.
