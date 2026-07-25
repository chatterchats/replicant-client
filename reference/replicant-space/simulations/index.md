---
title: "Simulations"
source_url: "https://replicant.space/docs/simulations/"
crawled_at: "2026-07-24T18:16:35.432169+00:00"
---

Simulations

# Simulations

Datacentres in space! Plug your replicant into the replicant interface and start fresh in a simulated galaxy. Compete for the fastest time on the leaderboards.

## What are simulations?

Simulations are speed trials - timed scenarios locking your replicant into an isolated virtual star cluster with a fresh device loadout. Each scenario has an objective and a time limit. The clock starts when you enter. Your score is how quickly you finish.

There is an RNG element to the scenarios. The stars are generated randomly each time, so belt configurations will change each time, requiring you to adapt your strategy to fit. Some starting conditions are fixed, depending on the scenario.

## The replicant interface

The *replicant_interface* is a large device found at datacentre megastructure locations. It's a simulator with slots for hundred replicant matrices. To run a simulation, you need a replicant at the same location as the replicant interface.

Simulation endpoints use the interface's device: `/v1/devices/:interface_code/simulate`.

Head to the location and [scan for other devices](../api/replicants/scan/devices/index.md). Look for the replicant interface and note its device code. Note that the scan endpoint accepts a *device_type* param to help locate the interface in busy systems.

## How it works

1. Travel to a datacentre location and identify the replicant interface.
2. View the available [scenarios](scenarios/index.md) and check the entry cost.
3. Deploy the required devices at the interface location - they'll be consumed when you start.
4. [Enter the simulation](running/index.md) - your replicant's matrix is plugged into the interface, your sensor inputs are switched to the VR environment.
5. Complete the objective as quickly as possible. The virtual world runs much faster than reality.
6. On completion, your matrix inputs are restored to your vessel and your time is recorded.
7. Check the [leaderboards](leaderboards/index.md) to see how you stacked up.

## What stays, what doesn't

Everything inside the simulation is disposable. The virtual star region, all devices you print in there, and any resources you mine are cleaned up when the simulation ends. Your real devices stay safe at their real locations.

Simulations do not award XP, do not count toward achievements, and do not track interstellar distance. They are a separate competitive layer.

## Replicant cooperation

While your replicant is plugged in, they can't operate devices in the real world. If you have [replicant cooperation](../api/accounts/index.md) enabled in your account settings, your other replicants can continue using your real-world devices during the run.
