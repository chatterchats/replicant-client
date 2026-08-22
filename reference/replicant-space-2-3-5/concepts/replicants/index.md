---
title: "Replicants & Accounts"
source_url: "https://replicant.space/docs/concepts/replicants/"
crawled_at: "2026-08-03T00:42:37.877988+00:00"
---

Core Concepts

# Replicants & Accounts

A replicant is you - a digital matrix housed inside a vessel. You start in a HEAVEN vessel - the spaceship that contains your matrix and gives you the ability to interface with the galaxy.

## Account vs. replicant scope

All of your replicants share knowledge between them. Each one earns its own experience points and accumulates reputation with the species it meets, but those values roll up into your account level. Anything you learn, any species you've befriended (or annoyed), is known across the fleet.

Blueprints and messages live at the account scope - unlock a blueprint on one replicant and every other replicant can print from it; receive a message and it's in your account inbox, not a particular replicant's. Devices, trades, and event logs are replicant-scoped, so each replicant's gameplay experience stays self-contained.

## What's in a replicant

The replicant matrix is a device that holds your replicant's consciousness. This box of quantum magic needs to be hosted in a suitable container to power it and provide an interface to the outside. You'll eventually be able to print empty replicant matrices to clone or teleport into.

## The HEAVEN vessel

The HEAVEN vessel is the generic starter spaceship for housing a replicant matrix. It comes with a built-in 3D printer, cruise drive, surge drive, enough space to store 10 small devices, and even some on-board mining equipment if you lose your drones. There are other specialised vessel blueprints available with cargo holds to carry resources, or massive drives for travelling at speed.

response GET /v1/devices/{code}   200 OK

```
{
  "available_commands": [
    "change_owner",
    "clear_queue",
    "deactivate",
    "dequeue_print",
    "enqueue_print",
    "replicate",
    "retarget",
    "start_mining",
    "stellar_census",
    "system_scan",
    "travel"
  ],
  "device_code": "814A64A0",
  "device_type": "heaven_vessel",
  "features": [
    "surge",
    "cruise",
    "system_scan",
    "mine",
    "cradle",
    "print",
    "census"
  ],
  "location": "TARAZEDAR-KUIPER",
  "operational_capacity": 100.0,
  "replicant_code": "4BBA7CBE",
  "status": "idle",
  "stow_capacity": 10,
  "stow_used": 4,
  "stowed_devices": [
    {
      "device_code": "2EED783C",
      "device_type": "replicant_matrix"
    },
    {
      "device_code": "40ED8A3E",
      "device_type": "mining_drone"
    },
    {
      "device_code": "8732ABE4",
      "device_type": "mining_drone"
    },
    {
      "device_code": "B58FCC78",
      "device_type": "mining_drone"
    }
  ]
}
```

### Smaller containers

You can also house a matrix in a simple matrix container - cheaper, smaller, no internal printer or device storage. Useful if your clone just needs a basic interface for controlling a system. See [Replicant Cloning](../../cloning/index.md) for the full set of options.

## Experience and achievements

Your replicant earns experience and achievements for novel actions - first scan of system, first successful interstellar surge, first transport route. Achievements unlock new blueprints. Other blueprints can be discovered through location events and a variety of gameplay aspects.

## Multiple replicants

You can have many replicants. `/v1/accounts/me` shows you the list. Devices are owned by replicants, so once you find yourself overwhelmed in a system, consider cloning and setting up fresh operations elsewhere. If you want to help with the exodus project, you'll need a pretty complex setup!

## Replicant cooperation

By default, each replicant's devices are private - only the owning replicant can operate on them. This is useful if you treat each replicant as an independent character with its own role and fleet. But if you're running multiple replicants as a coordinated team (mining in one system while hauling in another), you'll need to `change_owner` before every cross-replicant operation.

The cooperation system gives you fine-grained control over how your replicants share devices. It works at two levels:

### Account level: `replicant_cooperation`

Set on your account via `PATCH /v1/accounts/me`. Two options:

- `"individual"` (default) - each replicant controls its own devices. Cross-replicant access is determined by the replicant-level setting below.
- `"shared"` - all your replicants can freely operate on each other's devices. No per-replicant checks apply. This is the simplest option if you want full cooperation across the board.

### Replicant level: `cohort_permission`

Set on each replicant via `PATCH /v1/replicants/<code>`. Only matters when the account is set to `"individual"`:

- `"private"` (default) - only this replicant can operate on its devices.
- `"public"` - siblings can operate on this replicant's devices (stow into its vessel, adopt its drones with AMI, attach devices to its carriers, etc).

Note that `cohort_permission` controls whether *other* replicants can access *this* replicant's devices - it doesn't affect what this replicant can access on others.

### How it resolves

When replicant A tries to operate on a device owned by replicant B (same account):

- If the account is `"shared"` → allowed.
- If the account is `"individual"` and B's `cohort_permission` is `"public"` → allowed.
- If the account is `"individual"` and B's `cohort_permission` is `"private"` → denied.

Cross-account access is never permitted regardless of settings.

### What cooperation affects

Cooperation controls device operations between siblings: device lists, stowing into another replicant's vessel, AMI adopting another replicant's drones, attaching devices to another replicant's carrier, transferring your matrix into another replicant's cradle, and repairing another replicant's devices.

It does *not* affect trading (seller identity is always strict) or teleportation (which transfers vessel ownership and is a separate mechanic).

### Examples

**Solo player, two specialised replicants.** You have Mario (miner) and Luigi (hauler). You want each to manage their own fleet independently. Leave the account at `"individual"` and both replicants at `"private"`. Neither sees the other's devices.

**Team player, full sharing.** You have three replicants spread across systems, all working together. Set the account to `"shared"`. Every replicant can see and operate on every sibling's devices - AMI controllers find drones across the whole fleet, device lists include everything.

**Mixed setup.** You have Mario, Homer, and Luigi. You want Mario and Homer to share freely, but Luigi runs a solo survey operation. Set the account to `"individual"`. Set Mario and Homer to `"public"`, leave Luigi at `"private"`. Mario and Homer can operate on each other's devices. Luigi's devices are invisible to the others, and Luigi can access Mario's and Homer's devices (because they're public) but not vice versa for his own.
