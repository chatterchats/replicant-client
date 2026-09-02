---
title: "Strategy"
source_url: "https://replicant.space/docs/api/locations/terraforming/strategy/"
crawled_at: "2026-09-02T20:03:41.446401+00:00"
---

Terraforming

# Strategy

A recommended approach to terraforming. Work the attributes in the right order and you'll avoid most disasters.

## Choose your world wisely

Every planet and moon has different starting conditions and a different equilibrium it drifts toward. A world that's already close to your target species' requirements will take far less work than one at the opposite extreme. Check attributes before you commit - a hot desert is easier to cool than a [Venus analogue](https://www.reddit.com/r/askscience/comments/x6eenm/why_is_the_pressure_on_venus_so_high/).

Smaller bodies drift faster toward equilibrium, meaning your devices need to work harder to hold changes in place. Massive planets are sluggish but more stable once moved. Consider body mass when choosing your target.

## Step 1: Temperature

Surface temperature is the first priority. It couples into almost everything - too hot and you lose your hydrosphere to evaporation, too cold and everything freezes. Get temperature into a safe range before worrying about anything else.

For hot worlds, deploy *orbital shades* for clean cooling or *cryo dispersers* for faster results (watch the pressure side effect). For cold worlds, use *orbital mirrors* or *thermal lances* (watch the tectonic side effect on lances).

Target the 200-350 K band initially. This avoids both the freezing coupling (below 200 K) and the evaporation coupling (above 373 K), and satisfies the bio seeder precondition later.

## Step 2: Pressure

Once temperature is under control, focus on atmospheric pressure. The critical threshold is *10 atm* - above that, greenhouse runaway kicks in and temperature will spiral upward at +1.0/tick regardless of anything else. This is a causal coupling: it fires at full strength and will undo all your temperature work.

Use *atmo processors* to raise pressure on thin-atmosphere worlds. If the pressure is too high, you'll need to cool the planet and wait for equilibrium drift to bring it down, or manage the coupling carefully.

Target at least 0.3 atm (bio seeder precondition) but stay well below 10 atm.

## Step 3: Toxicity and oxygen

With temperature and pressure stable, clean the air. Deploy *filtration arrays* to scrub toxicity - you need it below 20 for bio seeders. Use *gas separators* to raise oxygen above 10%.

Watch for volcanic outgassing if the tectonic index is above 60 - it will pump toxicity faster than your filtration can clean it.

Don't push oxygen above 35% - hyperoxic fires will damage any biosphere you've built.

## Step 4: Hydrosphere

Deploy *aquifer taps* to release subsurface water. You need at least 10% coverage for bio seeders. Make sure temperature is stable first - above 373 K your water will evaporate as fast as you release it.

## Step 5: Biosphere

The final and slowest phase. *Bio seeders* only function when all preconditions are met: temperature 200-350 K, pressure above 0.3 atm, oxygen above 10%, toxicity below 20, and hydrosphere above 10%.

Biological seeding is slow at +0.3/tick, so once your hydrosphere and oxygen levels are above 10%, start your seeders early and let them run alongside your other work.

Once the biosphere reaches 50, biological scrubbing kicks in - the planet starts producing oxygen and reducing toxicity on its own. This is the tipping point where the world begins to sustain itself.

## Step 6: Regulate

When all attributes are close to your target species' requirements, deploy an *Atmospheric Regulator* to lock everything in place. The regulator suppresses small per-tick changes from drift, devices, and coupling effects - preventing microfluctuations from pushing attributes out of range.

The regulator cannot suppress larger changes. Ensure your terraforming devices are configured so the deltas are as low as possible. Keep an eye on your `terraforming.status` events even after regulation is active - a volcanic eruption or solar flare can still disrupt a stabilised world.

## General tips

- Deploy a Terraform Monitor first. No terraforming blind.
- Work in order: temperature, pressure, air quality, water, biology. Each step creates the conditions for the next.
- Avoid stacking too many of the same device - diminishing returns mean a diverse device mix is more effective than brute force.
- Watch for causal couplings (greenhouse runaway, volcanic outgassing). They fire at full strength and can spiral out of control.
- Smaller bodies are harder to hold. If your devices are fighting equilibrium drift constantly, consider a larger world.
- Check the species requirements before you start. Different species want very different conditions - there's no universal target.
