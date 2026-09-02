---
title: "Components"
source_url: "https://replicant.space/docs/api/locations/terraforming/components/"
crawled_at: "2026-09-02T20:03:41.314630+00:00"
---

Terraforming

# Components

The devices used to reshape a world. Each component targets specific attributes, with side effects that can help or hinder your progress.

## Quick reference

| Device | Primary effect | Side effects | Reversible |
| --- | --- | --- | --- |
| Terraform Monitor | Tracking only | - | - |
| Orbital Mirror | Temp +0.15 | - | - |
| Thermal Lance | Temp +0.4 | Tectonic +0.1 Pressure +0.02 | - |
| Orbital Shade | Temp -0.15 | - | - |
| Cryo Disperser | Temp -0.4 | Pressure +0.05 | - |
| Atmo Processor | Pressure +0.08 | Temp +0.03 | Yes |
| Gas Separator | Oxygen +0.5 | Toxicity -0.1 | Yes |
| Filtration Array | Toxicity -0.8 | Oxygen +0.05 Pressure -0.01 | - |
| Aquifer Tap | Hydro +0.6 | Pressure +0.02 | Yes |
| Bio Seeder | Biosphere +0.3 | Oxygen +0.1 | - |
| Atmospheric Regulator | Stabilisation | - | - |

All terraforming components require an autofactory to print and are unlocked by completing the terraforming training simulation at *MIRFAKA-OBJ-1*.

## Terraform Monitor

The first device to deploy at any terraforming site. It takes direct environmental readings and emits *terraforming.status* events to your [event stream](../../../events/stream/index.md). Without a monitor active, you won't have visibility into what's happening on the surface. Activate one before using any other terraforming components.

Note: a player account can only have up to 5 terraform monitors running simultaneously.

## Temperature control

### Orbital Mirror

Redirects sunlight onto the planetary surface. Clean and predictable with no side effects - the cheapest entry point for heating a cold world.

- Primary: surface temperature +0.15

### Thermal Lance

Drives sustained thermal passes across the surface, boiling frost and baking volatiles out of regolith. Faster than orbital mirrors but the concentrated heat disturbs the crust and vents additional gas.

- Primary: surface temperature +0.4
- Side effects: tectonic index +0.1, atmospheric pressure +0.02

### Orbital Shade

Blocks incoming sunlight to cool the surface. Clean with no side effects, but the large panel surface demands significant silicates.

- Primary: surface temperature -0.15

### Cryo Disperser

Releases cryogenic payloads into the upper atmosphere, drawing heat from the surrounding gas as the material expands and falls. Faster than orbital shades but the injected material adds atmospheric mass.

- Primary: surface temperature -0.4
- Side effects: atmospheric pressure +0.05

## Atmospheric control

### Atmo Processor

Skims the upper atmosphere, compressing and metering gas to build pressure. Moderate resource cost for a reliable pressure-builder.

- Primary: atmospheric pressure +0.08
- Side effects: surface temperature +0.03
- Reversible - set direction to decrease both pressure and temperature

### Gas Separator

Separates oxygen-bearing compounds from the atmosphere using electrochemical cell stacks. The most resource-intensive atmospheric component, but the only dedicated oxygen generator.

- Primary: oxygen +0.5
- Side effects: toxicity -0.1
- Reversible - set direction to decrease oxygen and increase toxicity

### Filtration Array

Deploys scrubber units that pull gas, aerosols, and particulates through molecular traps to capture contaminants. Cheap to build and effective - your primary tool against toxic atmospheres.

- Primary: toxicity -0.8
- Side effects: oxygen +0.05, atmospheric pressure -0.01

## Surface and biosphere

### Aquifer Tap

Fires penetrator probes into the crust to draw subsurface water to the surface. Expensive, but the only dedicated hydro generator.

- Primary: hydrosphere +0.6
- Side effects: atmospheric pressure +0.02
- Reversible - set direction to drain surface water and reduce pressure

### Bio Seeder

Fires dispersal pods carrying microbial cultures into the atmosphere, seeding biological activity across the surface. The slowest component and the most demanding in preconditions.

- Primary: biosphere +0.3
- Side effects: oxygen +0.1

Bio seeders will not function unless all of the following conditions are met:

- Surface temperature between 200 K and 350 K
- Atmospheric pressure above 0.3 atm
- Oxygen above 10%
- Toxicity below 20
- Hydrosphere above 10%

## Atmospheric Regulator

The final piece. A central control core that coordinates all seven terraforming subsystems, dynamically countering microfluctuations to hold the world stable.

The regulator is created with one of each terraforming device as a component: orbital mirror, orbital shade, atmo processor, gas separator, filtration array, aquifer tap, bio seeder, and a processing array. A little extra compute power is used to monitor and adjust microfluctuations in the environment. Activate one of these to finish your terraforming session and lock the environment to your new state.

## Diminishing returns

Stacking multiple devices of the same type gives diminishing returns. The effective rate per device scales as `(1 + ln(count)) / count`. Two thermal lances aren't twice as effective as one - the second adds less than the first. Plan your device mix rather than brute-forcing with duplicates.
