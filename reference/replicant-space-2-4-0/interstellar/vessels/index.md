---
title: "Vessels"
source_url: "https://replicant.space/docs/interstellar/vessels/"
crawled_at: "2026-08-07T00:51:31.777529+00:00"
---

Interstellar Transport

# Vessels

The standard chassis for a replicant on the move. Hosts your matrix in a cradle, carries devices in the hold, and travels across the galaxy. Three variants to choose from.

## The cradle

Every vessel has the cradle feature, which means it can host a replicant matrix. Stow your matrix in a vessel and you've got a body that can move between systems, carry stuff with it, and act as the interface for the player experience. See [replicate](../../api/replicants/replicate/index.md) for how new replicants get stowed into a cradle.

## Variants

Three different types of vessel, all with the cradle feature, tuned for very different jobs.

| Variant | Device hold | Resource cargo | Attachments | Travel speed |
| --- | --- | --- | --- | --- |
| `heaven_vessel` | 10 devices | None | None | Baseline |
| `cargo_vessel` | 50 devices | 200 resources | 3 devices | 40% slower |
| `racing_vessel` | 5 devices | None | None | 35% faster |

## Default vessel

The all-rounder. Ten device slots is plenty for a simple mining and survey operation and a spare FTL beacon. All players start the game with one of these. If you don't have a specific reason to pick one of the other two, pick this one.

## Cargo vessel

Built for moving stuff, not for moving fast. Fifty device slots in the hold, two hundred units of bulk resources in the cargo bay, and three external attachment points for the bigger devices you can't stow. The trade-off is a 40% reduction in travel speed. Attach some transport haulers to the outside to get an effective cargo hold of 440 resources.

## Racing vessel

For explorers and couriers. Five device slots, no cargo, no attachments. In exchange you travel 35% faster than the baseline vessel. Pair that with the deceleration reduction from a [system hub's](../../system-hubs/index.md) braking lasers and you'll be a blur.
