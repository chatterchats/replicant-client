---
title: "Locations overview"
source_url: "https://replicant.space/docs/api/locations/overview/"
crawled_at: "2026-09-02T20:03:41.242412+00:00"
---

API · Locations

# Locations overview

A flat list of every location where you have devices, replicants, resource sites or stockpiled resources. The fastest way to find where you've left things.

## Endpoint

`GET /v1/locations`

## Example

GET /v1/locations   200 OK

```
$ curl https://api.replicant.space/v1/locations \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "locations": {
    "SOL-2": {
      "devices": 1,
      "replicants": 1,
      "resource_sites": 4,
      "resources": 1023
    },
    "TARAZEDAR-BELT-1": {
      "devices": 23,
      "replicants": 1,
      "resource_sites": 10,
      "resources": 23012
    }
  }
}
```

## Fields

- `devices` - how many of your devices are at this location.
- `replicants` - replicants that are there.
- `resource_sites` - resource sites from belts and salvage that you have access to.
- `resources` - total number of resources at this location.

Find out more with [/locations/{code}](../index.md) for specific location data, or [/locations/{code}/inventory](../inventory/index.md) to see the full breakdown of resources.
