---
title: "Travel"
source_url: "https://replicant.space/docs/api/replicants/travel/"
crawled_at: "2026-08-25T15:34:31.739324+00:00"
---

API · Replicants

# Travel

Send a replicant somewhere. The nav system figures out the route for you. It can be lonely in the black.

## Endpoint

`POST /v1/replicants/{code}/travel`

## Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `destination` | string | A location code. Star, planet, moon, belt, Lagrange point - anything in [the location grammar](../../../concepts/locations/index.md). |

## Example

POST /v1/replicants/{code}/travel   200 OK · 84ms

```
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/travel \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"destination": "SOL"}'
```

response response

```
{
  "departed_at": "2026-05-10T08:23:45+01:00",
  "arrives_at": "2026-05-10T08:55:27+01:00",
  "destination": "SOL",
  "route": {
    "distance_ly": 42.2915,
    "from": "PORRAMA-KUIPER",
    "to": "SOL-5-L4",
    "time_seconds": 1902,
    "type": "surge"
  },
  "status": "travel_initiated"
}
```

## Dry run

Pass `dry_run=true` to preview the route without departing. The response returns the full route the nav system was planning to take, to help with any pre-flight checks.

POST /v1/replicants/{code}/travel   200 OK

```
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/travel \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"destination": "SOL", "dry_run": true}'
```

response response

```
{
  "destination_type": "star",
  "final_destination": "SOL",
  "final_destination_name": null,
  "route": [
    {
      "distance_ly": 63.5775,
      "from": "ICUSTOS-1-L4",
      "from_name": null,
      "leg": 1,
      "time_seconds": 28,
      "to": "SOL-5-L4",
      "to_name": null,
      "type": "surge"
    }
  ],
  "status": "preview",
  "total_distance_ly": 63.5775,
  "total_time_seconds": 28
}
```
