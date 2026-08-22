---
title: "Device details"
source_url: "https://replicant.space/docs/api/devices/retrieve/"
crawled_at: "2026-08-22T22:43:50.139737+00:00"
---

API · Devices

# Device details

Devices are at the core of Replicant Space. They come in all shapes and sizes, from tiny beacons to huge galactic observatories. Some have space to stow other devices in a hold, others have cargo space for resources, some can be attached together. A range of different features, abilities and drives.

## Endpoint

`GET /v1/devices/{code}`

## Example

GET /v1/devices/{code}   200 OK

```
$ curl https://api.replicant.space/v1/devices/B58FCC78 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "device_code": "B58FCC78",
  "device_type": "mining_drone",
  "replicant_code": "4BBA7CBE",
  "location": "TARAZEDAR-BELT-1",
  "features": [
    "cruise",
    "mine",
    "stow"
  ],
  "available_commands": [
    "change_owner",
    "deactivate",
    "decommission",
    "deploy",
    "recall",
    "retarget",
    "start_mining",
    "stow",
    "travel"
  ],
  "operational_capacity": 67.0,
  "status": "idle"
}
```

## Wear and tear

Space is empty, kinda. While it doesn't look like there's much in the gap between planets and moons, there's always micrometeorites, space dust, exposure to high levels of radiation and gravitational stresses. Different devices take different amounts of wear depending on what they are doing. Mining drones will need regular maintenance.

## Stowage

This is a specialised type of storage for smaller device types, preserving them in a safe manner for transport. Devices that are stowed can be pre-configured before deployment. Recalling a remote device will send it back to your replicant vessel to be stowed.

## Cargo

Some devices will have specific cargo space, this is for storing resources for transport. Devices with particularly large cargo holds require more complicated designs, to ensure the structural integrity during long trips.

## Attachments

Some of the larger devices can be attached to each other for a variety of purposes. Some vessels will have a number of attachment slots, which can be useful for increasing the cargo space. The Mobile Fleet, for example, generates a large enough Surge field to attach up to 36 additional devices to it for interstellar travel.

## Large devices

Devices like the Autofactory, System Hub and Galactic Observatory are too large to be attached directly to a surge carrier. These devices have the `modular` feature, which grants the `compact` and `unfurl` commands. Compact the device first, attach it to a carrier for interstellar travel, then unfurl it at the destination. Each operation takes 30% of the device's original print time.

## Status values

- `stowed` - stowed in another device.
- `idle` - deployed with no active task.
- `travelling` - currently in motion.
- `cruising` - moving by cruise transport.
- `surging` - moving by surge transport.
- `recalling` - travelling back to its recall destination.
- `recall_waiting` - waiting to begin or resume recall travel.
- `decommissioning` - travelling to be decommissioned.
- `collecting` - collecting cargo at the pickup location.
- `depositing` - unloading transported cargo at the delivery location.
- `waiting_for_surge_plate` - waiting for a surge plate before transport can continue.
- `mining (<resource>)` - mining the named resource.
- `prospecting` - prospecting for new stars.
- `tracking` - tracking a resource site.
- `scanning` - running a scan.
- `monitoring` - monitoring for signals at a location.
- `printing (<device_type>)` - printing the named device type.
- `waiting_for_resources` - waiting for resources required by the print queue.
- `repairing` - repairing a device.
- `diverting` - diverting an asteroid.
- `patrolling` - patrolling a system looking for repairs.
- `coordinating` - coordinating adopted devices.
- `relaying` - FTL relay is active.
- `inactive` - FTL relay is inactive.
- `compacting` - large device is being compacted for transport.
- `compacted` - large device is compacted and ready for attachment.
- `unfurling` - large device is being reassembled after transport.
