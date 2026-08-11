---
title: "Replicate"
source_url: "https://replicant.space/docs/api/replicants/replicate/"
crawled_at: "2026-08-11T15:11:29.057833+00:00"
---

API · Replicants

# Replicate

Copy your consciousness into an empty matrix and watch a fresh replicant walk away with it. Unlike transfer and teleport, the source replicant stays put - now there are two of you. Maybe print a vessel and some drones first before you unleash another one of you on the galaxy.

## Endpoint

`POST /v1/devices/{matrix_code}`

This is a device command on your current matrix device, not a replicant endpoint. The source matrix runs the replicate command as a direct matrix-to-matrix operation.

## Pre-conditions

- The target is an `empty_replicant_matrix` device.
- The target is stowed in a device that has the cradle feature.
- The cradle device is at the same location as the source matrix.

## Stowing an empty matrix

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

Any device with the cradle feature works - a vessel is the usual choice if you want to bring the new replicant along with you, but a matrix container or system hub will do the same job if you'd rather leave the new replicant where it spawns.

## Replicating

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

Congratulations! There are now two of you. Your second replicant endpoint responses will now be a lot cleaner if you want to embark on another project somewhere.
