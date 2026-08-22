---
title: "Decommission"
source_url: "https://replicant.space/docs/api/devices/decommission/"
crawled_at: "2026-08-22T22:43:50.054173+00:00"
---

API · Devices

# Decommission

Send the decommission command to a device you own. It will cruise to the nearest autofactory and be broken down into raw resources. Around 60% of the original material is recovered.

## Endpoint

`POST /v1/devices/{code}`

## Example

POST /v1/devices/{code}   202 accepted

```
$ curl -X POST https://api.replicant.space/v1/devices/2AC61214 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "decommission"}'
```

response response

```
{
  "device_code": "2AC61214",
  "status": "decommissioning"
}
```

## Blueprints

If you don't already have a [blueprint](../../../concepts/blueprints/index.md) for the device type being decommissioned, the autofactory will learn it by taking the device apart. This is one of the main ways to discover new blueprints - other players will have blueprints you don't know yet, and you may be able to trade for them.
