---
title: "System Wards"
source_url: "https://replicant.space/docs/system-wards/"
crawled_at: "2026-08-25T15:34:32.908899+00:00"
---

Infrastructure

# System Wards

A lightweight interdiction device that locks mining, salvage, and species interaction across a star system. Cheaper to deploy than a hub, but restricted number allowed per account.

## What a ward gives you

- Mining lock - other players can't mine asteroid belts or salvage sites.
- Species interaction lock - other players can't complete location events here.
- Miner eviction - upon activation, stops any foreign mining drones and pauses AMI mining directives.

## Where to find them

New players will find them in each of your starting star systems. They should appear in your device list. Stow them for transport and deploy and activate where you want them. Decommission one to an autofactory to learn the blueprint, so you can print more. Be aware that there is a limit on how many can be activated per account. This is a device to protect your core manufacturing locations, not for taking over the galaxy!

Players who existed before this feature was released can look for an equipment locker in the SOL Oort cloud, and retrieve an FTL Slingshot to their own private region, where several System Wards can be found.

## How to activate

Deploy the ward anywhere in the system and issue the `activate` command. The device emits a strong interdiction broadcast in the system. Other replicant devices will respect your ownership claim and back off instantly.

POST /v1/devices/{ward_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/WD55A1C3 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "activate"}'
```

response 200 response

```
{
  "status": "activated",
  "device_code": "WD55A1C3",
  "star": "POLIBUS",
  "location": "POLIBUS-OORT",
  "warding": true,
  "activated": "ward"
}
```

## Miner eviction

When a ward activates, any mining drones in the system that belong to other accounts are immediately stopped. If those drones were controlled by an AMI controller, the controller's mining directive is paused.

The response includes an `evicted_miners` count if any drones were stopped.

response 200 response — with evictions

```
{
  "status": "activated",
  "device_code": "WD55A1C3",
  "star": "POLIBUS",
  "location": "POLIBUS-OORT",
  "warding": true,
  "activated": "ward",
  "evicted_miners": 3
}
```

The evicted player will receive events for each stopped drone and paused directive, so they'll know what happened.

## Deactivating

Issue the `deactivate` command to bring the ward offline. The system unlocks immediately and other players can resume mining.

POST /v1/devices/{ward_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/WD55A1C3 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "deactivate"}'
```

response 200 response

```
{
  "status": "deactivated",
  "device_code": "WD55A1C3",
  "star": "POLIBUS",
  "location": "POLIBUS-OORT",
  "warding": false,
  "deactivated": "ward"
}
```

## Limits

- Maximum 25 active wards per account across all systems.
- Only one account can ward a system at a time. If the system is already warded by someone else, activation will fail.
- You cannot activate a ward in a system that has a [System Hub](../system-hubs/index.md) deployed. The interference fields cancel out.
