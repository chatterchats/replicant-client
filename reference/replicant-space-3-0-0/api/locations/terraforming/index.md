---
title: "Terraforming"
source_url: "https://replicant.space/docs/api/locations/terraforming/"
crawled_at: "2026-09-02T20:03:41.287974+00:00"
---

Terraforming

# Terraforming

Reshape planets and moons to suit species preferences. Deploy thermal lances, filtration arrays, and more to transform hostile worlds into a paradise.

## What is terraforming?

Terraforming is how we modify a world's environmental attributes - surface temperature, atmospheric pressure, oxygen levels, toxicity, hydrosphere coverage, tectonic activity, and biosphere development. Each world has its own starting conditions and equilibrium values that it naturally drifts toward, which are calculated from the physical characteristics of the world - gravity, mass, distance from the star, etc. Your job is to push those attributes into a range that suits a target species.

Different species have different environmental requirements. Finding a world that's already close to what you need will save a lot of effort. Check a planet's current attributes before committing to a long terraforming operation.

## Getting started

Before you can terraform real worlds, you need to complete the terraforming training simulation. Head to the datacentre at *MIRFAKA-OBJ-1* and find the *replicant_interface* in the list of [other devices](../../replicants/scan/devices/index.md). Run the terraforming scenario to learn the basics - how devices interact, how coupling rules chain together, and how to read the environmental feedback.

See the [Simulations](../../../simulations/running/index.md) page for details on how to view and run a scenario

The simulation teaches you the core loop: deploy a [Terraform Monitor](components/index.md), activate devices to shift attributes, watch your [event stream](../../events/stream/index.md) for `terraforming.status` updates, and stabilise the world with an Atmospheric Regulator once you're close to your targets.

Use the [Terraforming Update](https://stream.replicant.space/) section on the Event Stream for a realtime (ish) dashboard to watch the environmental attributes with graphs, deltas, predictions and anomaly alerts

Once you've completed the training, you'll be able to activate terraforming devices in the real galaxy and be able to start working on real planets and moons across the galaxy.

## The process

1. Find a suitable world - check its current attributes and equilibrium values against your target species.
2. Deploy a *Terraform Monitor* at the location to begin tracking changes.
3. Activate terraforming [components](components/index.md) to shift attributes toward the target range.
4. Configure your devices to control the strength and direction of their effects.
5. Watch for [coupling effects and disasters](disasters/index.md) - some attribute changes trigger chain reactions.
6. Once attributes are close, deploy an *Atmospheric Regulator* to suppress microfluctuations and hold the world stable.
7. For a recommended approach, see the [strategy guide](strategy/index.md).

## Device control

Every terraforming device can be configured with a *strength* value from `0.0` to `1.0`. At full strength the device operates at its listed rate. Dial it back to reduce the effect - useful for fine-tuning attributes as they approach the target range without overshooting.

Some devices also support a *direction* setting that reverses their effect. An atmo processor normally increases atmospheric pressure and surface temperature, but reversing it will decrease both instead. Check the [components reference](components/index.md) to see which devices are reversible.

You may find that five orbital mirrors at full strength, plus a sixth running at 0.13 strength is the perfect amount to warm a frozen wasteland to Human level of comfort.

## Attributes

Every terraformable world has seven environmental attributes:

- **Surface temperature** (K) - measured in Kelvin. Range: 50 - 1,200 K.
- **Atmospheric pressure** (atm) - measured in Earth atmospheres. Range: 0 - 100 atm.
- **Oxygen** (%) - atmospheric oxygen percentage. Range: 0 - 100%.
- **Toxicity** - atmospheric toxicity index. Range: 0 - 100.
- **Hydrosphere** (%) - surface water coverage. Range: 0 - 100%.
- **Tectonic index** - geological activity level. Range: 0 - 100.
- **Biosphere index** - biological development level. Range: 0 - 100.

Each attribute drifts toward its natural equilibrium over time. Smaller bodies drift faster; massive planets are more resistant to change. Your devices need to overcome this drift to make lasting progress.
