---
title: "Resources"
source_url: "https://replicant.space/docs/concepts/resources/"
crawled_at: "2026-08-22T22:43:51.504706+00:00"
---

Core Concepts

# Resources

Our 3D printers are pretty advanced. We can make use of anything we find. All resources are categorised into six different types.

## The six categories

- **Structural** - frames, mounting, chassis.
- **Conductive** - wiring, PCBs, comms.
- **Silicates** - insulation, shielding, ceramics.
- **Carbon** - composites, radiator panels, heat pipes.
- **Volatiles** - coolants, fluids, consumables.
- **Rare earth minerals** - advanced chips, sensors, batteries.

Every blueprint expresses its cost as a combination of these six. Our tech is advanced enough to use all sorts of raw materials to construct devices.

## Where to find stuff

The fastest way is a salvage site. It's always easier to extract refined resources from some old bit of tech than extracting it from a rock. You'll can find salvage at planets and moons in a system when you perform a scan. You won't find all resource types at salvage sites - they tend to be more specialised.

Otherwise, it's belts. Asteroid belts have all six categories at varying ratios. Every belt has a *density* (how long it takes to move between rocks) and each resource has a *scarcity* (how much is found per cycle). Some belts are rare-earth rich; others are heavily structural (big dumb rocks). Deploy your mining drones and keep an eye on the location inventory.

response GET /v1/locations/TARAZEDAR-BELT-1   200 OK

```
{
  "location": "TARAZEDAR-BELT-1",
  "location_type": "belt",
  "belt": {
    "density": "sparse",
    "designation": "TARAZEDAR-BELT-1",
    "inner_radius_au": 0.6,
    "outer_radius_au": 0.9,
    "resources": {
      "carbon": "rich",
      "conductive": "scarce",
      "rares": "low",
      "silicates": "low",
      "structural": "moderate",
      "volatiles": "high"
    }
  },
  "resource_sites": [
    {
      "designation": "TARAZEDAR-BELT-1-SITE-0",
      "name": "TARAZEDAR-BELT-1 Primary Site",
      "resources_remaining_pct": {
        "carbon": 100,
        "conductive": 100,
        "rares": 100,
        "silicates": 100,
        "structural": 100,
        "volatiles": 100
      },
      "site_index": 0
    }
  ]
}
```

## Resource sites

Belts are basically infinite, but your ability to track and mine them isn't. You'll see from the above example that there is a primary site available for mining. This is effectively the easy pickings that you can identify from a simple glance. Once you mine that dry, it will regenerate slowly over time. Deploy a survey drone at the belt and perform a search command. This will take some time, but once finished, the drone will have identified a new site.

We call it a site, but it's technically a collection of rocks that are all in motion. The survey drone remains tracking the site while you mine. You can open multiple sites at the same time with multiple survey drones. Don't deactivate the survey drone or send it elsewhere or the site will close automatically.

Sites take longer to search for over time, with diminishing returns. For a sustainable operation, you'll want to balance the number of survey drones and mining drones carefully. Sites will regenerate given enough time.

You don't need to specify the site when commanding your mining drones, they'll look for their resource types across all open sites at the location.

## System Hub locking

Once a System Hub is active in a system, mining in that system is locked to the hub's owner. Other players can't strip-mine your belts. The cost of that protection is hub maintenance - see [System Hubs](../../system-hubs/index.md) for the full picture.

## Decommissioning

Send any device you built the `decommission` command and it will cruise to the nearest autofactory for disassembly to recover most of its resource cost. Recovery rates are around 60% - there's always some loss to wear, components that shed mass, and the fuel cost of the recycle itself.
