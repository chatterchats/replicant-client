---
title: "Disasters"
source_url: "https://replicant.space/docs/api/locations/terraforming/disasters/"
crawled_at: "2026-09-02T20:03:41.383027+00:00"
---

Terraforming

# Disasters

Environmental attributes are coupled. Push one too far and others will follow - sometimes catastrophically.

## Coupling rules

Attributes don't exist in isolation. When certain thresholds are crossed, coupling effects kick in and start changing other attributes automatically. Some of these are manageable. Some will destroy your progress if left unchecked.

| Rule | Trigger | Effects | Causal |
| --- | --- | --- | --- |
| Greenhouse runaway | Pressure > 10 atm | Temp +1.0 | Yes |
| Evaporation | Temp > 373 K | Hydro -1.5, Pressure +0.05 | No |
| Freezing | Temp < 200 K | Hydro -0.8, Pressure -0.03 | No |
| CO₂ condensation | Temp < 150 K | Pressure -0.1 | No |
| Volcanic outgassing | Tectonic > 60 | Toxicity +1.5, Temp +0.3 | Yes |
| Geological violence | Tectonic > 80 | Biosphere -3.0 | No |
| Toxic suppression | Toxicity > 30 | Biosphere -0.8 | No |
| Hyperoxic fire | O₂ > 35% | Biosphere -1.2 | No |
| Ocean dampening | Hydro > 90% | Pressure +0.03, Tectonic -0.5 | No |
| Biological scrubbing | Biosphere > 50 | O₂ +0.5, Toxicity -0.8 | No |

### Greenhouse runaway

The most dangerous coupling effect. If atmospheric pressure exceeds **10 atm**, surface temperature will rise at +1.0. This is a causal rule - it fires at full strength regardless of how far the temperature has drifted from equilibrium. A greenhouse runaway will rapidly cook the planet and is very difficult to recover from. Keep pressure under control.

### Evaporation

When surface temperature exceeds **373 K** (100°C), the hydrosphere begins to boil off at -1.5, and atmospheric pressure rises slightly. Cool the planet before you lose your water.

### Freezing

Below **200 K**, the hydrosphere freezes at -0.8 and atmospheric pressure drops. Cold worlds lose both their water and their air.

### CO₂ condensation

Below **150 K**, atmospheric gases begin condensing out, dropping pressure at -0.1 on top of the freezing effects.

### Volcanic outgassing

When the tectonic index exceeds **60**, volcanic activity pumps toxicity at +1.5 and raises surface temperature at +0.3. This is a causal rule - it always fires at full strength. Worlds with high tectonic activity need filtration arrays running constantly.

### Geological violence

A tectonic index above **80** is catastrophic for life. The biosphere takes -3.0 of damage. Any biological progress you've made will be wiped out quickly. Stabilise tectonics before attempting bio seeding.

### Toxic suppression

Toxicity above **30** suppresses biological development at -0.8. Even if other preconditions for bio seeders are met, high toxicity will erode your biosphere faster than seeders can build it.

### Hyperoxic fire

Oxygen levels above **35%** trigger spontaneous fires that damage the biosphere at -1.2. More oxygen isn't always better - keep it in the target range for your species.

### Ocean dampening

A hydrosphere above **90%** slowly increases atmospheric pressure at +0.03 and suppresses tectonic activity at -0.5. Mostly benign, but watch the pressure on already high-pressure worlds.

### Biological scrubbing

A healthy biosphere (above **50**) starts cleaning the atmosphere - increasing oxygen at +0.5 and reducing toxicity at -0.8. This is the payoff for getting biology established: the planet starts helping you.

## Coupling scaling

Most coupling effects scale with how far the target attribute has drifted from its equilibrium. A small deviation produces a weak effect; a large deviation produces a strong one. The effect ramps from 0% at 5% deviation to full strength at 25% deviation.

**Causal rules are the exception.** Greenhouse runaway and volcanic outgassing bypass scaling entirely and always fire at full strength once triggered. These are the ones to watch.

## Anomalies

Random events that can disrupt your terraforming progress. Each tick has a small chance of triggering an anomaly. You will see a warning in the status updates before impact, giving you time to prepare.

| Anomaly | Condition | Warning | Duration | Minor | Dramatic |
| --- | --- | --- | --- | --- | --- |
| Volcanic Eruption | Tectonic > 40 | 12 ticks | 6 ticks | Toxicity +2, Temp +1 | Toxicity +7, Temp +4, Pressure +1.5 |
| Solar Flare | None | 6 ticks | 3 ticks | Temp +2 | Temp +7, Biosphere -1, Hydro -2 |
| Cryovolcanic Eruption | Temp < 200 K | 12 ticks | 4 ticks | Hydro +1.5, Pressure +1, Temp -1 | Hydro +4, Pressure +2.5, Temp -2.5 |
| Toxic Bloom | Biosphere > 20 | 18 ticks | 8 ticks | Toxicity +3 | Toxicity +6.5 |
