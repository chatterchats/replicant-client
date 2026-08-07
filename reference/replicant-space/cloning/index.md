---
title: "Replicant Cloning"
source_url: "https://replicant.space/docs/cloning/"
crawled_at: "2026-08-07T00:51:31.218293+00:00"
---

Infrastructure

# Replicant Cloning

Want to manage things yourself but also explore? Clone yourself. Print 10 copies and send them off in different directions. Your HEAVEN vessel houses your matrix; everything else is up to you.

## The core sequence

1. Print an empty replicant matrix - the quantum substrate for containing the digital consciousness.
2. Print or repurpose a vessel to house it.
3. Stow the empty matrix in the vessel (or any cradle device).
4. Call `replicate` on your source matrix - the source replicant duplicates into the target.

## Stowing the empty matrix

Before you can replicate, the target matrix needs to be in a cradle device (a vessel, a matrix container or a system hub). Print an `empty_replicant_matrix`, then stow it inside the cradle device by sending a `stow` command to the matrix, with `target` as the cradle.

POST /v1/devices/{matrix_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/9F1C8B22 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "stow",
      "target": "24C8355C"
    }'
```

## Replicating

Send `replicate` to your source matrix with the target empty matrix as `target`. The source matrix runs this as a direct matrix-to-matrix near-beam operation; the cradle holding the target must be at the same location as the source matrix.

POST /v1/devices/{matrix_code}   201 created

```
$ curl -X POST https://api.replicant.space/v1/devices/37C51F74 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "replicate",
      "target": "7A303E2A"
    }'
```

response response

```
{
  "status": "replicated",
  "new_replicant_code": "A1B2C3D4",
  "new_replicant_name": "Bob-2",
  "host_device_code": "BCF35044",
  "matrix_code": "3CA5D7E4"
}
```

## Vessel options

| Vessel | Drives | Cost | Best for |
| --- | --- | --- | --- |
| *matrix_container* | none | cheap | parking a clone - needs a surge plate or carrier to move |
| *heaven_vessel* | cruise + surge | moderate | fully autonomous interstellar clones |
| *system_hub* | cruise | expensive | in-system permanent clones |

## Sharing knowledge

Replicants share unlocked blueprints automatically through beacon networks. Experience diverges after cloning - the new replicant starts with 0 XP, gaining it independently from the moment of the split.

## Bootstrapping elsewhere

A common pattern: print yourself a fresh vessel with some drones and a few transport devices full of resources. Stick them all on a surge platform. Clone and send the platform off to a target system. The clone arrives ready to start a new manufacturing operation.

## Identity

Every replicant you own answers to the same API key on your account. The replicant `code` is what differentiates them. All devices are owned by the replicant to keep devices lists clean. If you have a cohort of replicants working together, give them their own jobs to do. All resources are account-scoped.
