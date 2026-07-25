---
title: "Mining drone"
source_url: "https://replicant.space/docs/drones/mining/"
crawled_at: "2026-07-24T18:16:35.084276+00:00"
---

Drones

# Mining drone

Deploy a mining drone at a resource location and tell it what to gather. The drone handles extraction and refining, and leaves the result in nice organised piles.

## Commands

| Command | Args |
| --- | --- |
| `travel` | `destination` |
| `start_mining` | `resource_type` (one of the [six categories](../../concepts/resources/index.md)) |
| `retarget` | `resource_type` (one of the [six categories](../../concepts/resources/index.md)) |
| `deactivate` | - |

You can also use the optional `target` param on the request, if you want to mine a specific resource site. Otherwise, the drone will select a target site automatically depending on what resource it's working on.

## Mining for resources

The drone must already be at the belt or salvage site. Send it travelling to the location first if not.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/2AC61214 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "start_mining", "resource_type": "carbon"}'
```

All asteroid belts have a primary resource site by default. This represents the easy pickings that you can see at a glance. Those rocks are a bit shiny, that one looks a bit wet.

Once you've mined all the obvious stuff, you'll need to use a [survey drone](../survey/index.md) to open up and track additional sites.

## Mining at asteroid belts

There are two key attributes of an asteroid belt that determine available resources and mining speed. Both of these will show up in your system scan, to help you decide if this is a desirable system for your manufacturing operations.

### Belt density

The options are *sparse*, *moderate* or *dense*. This value determines how long it takes between mining cycles. For dense belts, the drones need to manoeuver less, so you'll get resources fast. A sparse belt means lots more travel between rocks.

### Resource availability

Each of the six resources will be found in all belts, but the amount available varies wildly. Options are *scarce*, *low*, *moderate*, *high* and *rich*. A belt that's rich in volatiles might have up to 10x more than a scarce one.

The makeup of an asteroid belt will vary wildly between systems, but you can usually assume there will be a lot more structural resources available than rare earths and volatiles.

## Mining speed

The scarcity (or abundance!) of a resource determines how much of it your drones can extract per mining tick. The density of the belt determines the gap between mining sessions, since the drones need to move further between rocks.

## Resources at different sites

Multiple resource sites at a single location have a shared inventory. If you find yourself with resources at different locations and want to move them, you'll need to use [transport drones](../transport/index.md).

## Running a fleet

Coordinating more than a handful of mining drones by hand gets tedious. Hand them off to a [Mining Controller](../../ami/mining-controller/index.md) to handle the retargeting on your behalf.
