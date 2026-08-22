---
title: "Nearest stars"
source_url: "https://replicant.space/docs/api/replicants/stars/"
crawled_at: "2026-08-22T22:43:51.082794+00:00"
---

API · Replicants

# Nearest stars

Your vessel will have the census feature, this allows you to figure out some details about the stars in your area. List the nearest stars. This is how you decide where to go next.

## Endpoint

`GET /v1/replicants/{code}/stars`

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `per_page` | integer · optional | 1-50, default 10. |
| `page` | integer · optional | Page number, default 1. |

## Example

GET /v1/replicants/{code}/stars   200 OK

```
$ curl https://api.replicant.space/v1/replicants/8AFE4482/stars?per_page=3&page=1 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "page": 1,
  "per_page": 3,
  "replicant_position": {
    "x": 34.1906,
    "y": -8.9593,
    "z": -42.9832
  },
  "stars": [
    {
      "designation": "CHAMAKUY",
      "color": "red",
      "distance_from_replicant": 0,
      "entry_point": "CHAMAKUY-4-L4",
      "estimated_planets": 4,
      "estimated_travel_time": 0,
      "has_hub": true,
      "position": { "x": 34.1906, "y": -8.9593, "z": -42.9832 },
      "region": "solzone",
    },
    {
      "designation": "TARAZEDAR",
      "color": "yellow",
      "distance_from_replicant": 4.5,
      "entry_point": "TARAZEDAR-1-L4",
      "estimated_planets": 5,
      "estimated_travel_time": 120,
      "position": { "x": 37.8, "y": -7.1, "z": -41.0 }
      "region": "solzone",
    },
    {
      "designation": "PORRAMA",
      "color": "blue",
      "distance_from_replicant": 6.8,
      "entry_point": "PORRAMA-2-L4",
      "estimated_planets": 3,
      "estimated_travel_time": 182,
      "position": { "x": 29.4, "y": -12.2, "z": -40.1 }
      "region": "solzone",
    }
  ]
}
```

## Response fields

- **replicant_position** - your replicant's current galactic coordinate, used as the origin for distance calculations.
- **distance_from_replicant** - straight-line distance in light years.
- **estimated_planets** - approximate count from the long-range census. Only a full [system scan](../scan/index.md) confirms exact counts.
- **estimated_travel_time** - surge travel time in seconds from your replicant's current position.
- **position** - the star's galactic coordinate. See [Locations](../../../concepts/locations/index.md) for the coordinate model.

## A single star

Append a star designation to get details for just that star: `GET /v1/replicants/{code}/stars/{star}`. Handy for checking a specific star's coordinates and distance without paging through the full census.

GET /v1/replicants/{code}/stars/{star}   200 OK

```
$ curl https://api.replicant.space/v1/replicants/8AFE4482/stars/MENKENTAR \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "replicant_position": {
    "x": 0.0,
    "y": 0.0,
    "z": 0.0
  },
  "star": {
    "color": "Yellow",
    "designation": "MENKENTAR",
    "distance_from_replicant": 6.37,
    "entry_point": "MENKENTAR-5-L4",
    "estimated_planets": 9,
    "estimated_travel_time": 3,
    "explored": false,
    "has_life": null,
    "position": {
      "x": -4.659,
      "y": -0.1315,
      "z": 4.3386
    },
    "spectral_type": "G4"
  }
}
```

Alongside the census fields above, the single-star response includes `spectral_type`, `explored`, and `has_life` (`null` until you've scanned the system).
