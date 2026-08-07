---
title: "Megastructures"
source_url: "https://replicant.space/docs/api/locations/megastructures/"
crawled_at: "2026-08-07T00:51:30.701703+00:00"
---

API · Locations

# Megastructures

A megastructure is a system object that's built collectively. Print the right devices, deliver them to the location, contribute them. Each season features a megastructure objective - contribute before the deadline.

## Concept

A megastructure sits at a system object location just like an [asteroid](../asteroids/index.md) or any other body (see [locations](../../../concepts/locations/index.md) for the wider model). It needs specific devices to complete. Anyone can contribute - your share is tracked.

## Inspect requirements

`GET /v1/locations/{code}` on a megastructure location returns the standard location payload with an extra `megastructure` block describing overall `progress` and a per-device-type tally of `needed` versus `contributed`.

GET /v1/locations/{code}   200 OK

```
$ curl https://api.replicant.space/v1/locations/FLARM-OBJ-1 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "code": "FLARM-OBJ-1",
  "location_type": "megastructure",
  "system": "FLARM",
  "megastructure": {
    "name": "Exodus Gate",
    "progress": 0.42,
    "requirements": {
      "surge_plate": { "needed": 12, "contributed": 5 },
      "ftl_beacon": { "needed": 6, "contributed": 3 },
      "autofactory": { "needed": 4, "contributed": 2 }
    }
  },
  "devices": [],
  "inventory": []
}
```

## Contribute devices

Bring devices to the megastructure location, then POST their codes to `/v1/locations/{code}/contribute`. Accepted devices are consumed by the structure and their codes appear in `accepted`; anything ineligible (wrong type, wrong location, already pledged) comes back in `rejected`.

POST /v1/locations/{code}/contribute   200 OK

```
$ curl -X POST https://api.replicant.space/v1/locations/FLARM-OBJ-1/contribute \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"devices": ["A473F411", "F54FA154"]}'
```

response response

```
{
  "location": "FLARM-OBJ-1",
  "accepted": ["A473F411", "F54FA154"],
  "rejected": [],
  "progress": 0.46,
  "status": "contribution_recorded"
}
```

## Leaderboard

`GET /v1/leaderboards/megastructure` returns the all-time contribution leaderboard across the galaxy, ranked by total devices donated.

GET /v1/leaderboards/megastructure   200 OK

```
$ curl https://api.replicant.space/v1/leaderboards/megastructure \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "leaderboard": [
    { "rank": 1, "replicant": "Bob-19", "devices": 214 },
    { "rank": 2, "replicant": "Bob-04", "devices": 198 },
    { "rank": 3, "replicant": "Bob-22", "devices": 177 }
  ]
}
```
