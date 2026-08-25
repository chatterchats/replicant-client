---
title: "Location details"
source_url: "https://replicant.space/docs/api/locations/"
crawled_at: "2026-08-25T15:34:31.230094+00:00"
---

API · Locations

# Location details

Get whatever the API knows about a place. The response shape depends on what kind of location you ask for - stars, belts, planets and moons all return different fields.

## Endpoint

`GET /v1/locations/{code}`

## Stars

If `{code}` is a star (for example `TARAZEDAR`), the response is identical to a fresh system scan - the star's planets, belts, lagrange points and any system hub. See [replicants/scan](../replicants/scan/index.md) for the full response shape.

## Belts

Belt responses describe the asteroid field itself - overall density and the scarcity of each resource type. Density level ranges from sparse to dense. Scarcity levels range from scarce to rich.

GET /v1/locations/{code}   200 OK

```
$ curl https://api.replicant.space/v1/locations/TARAZEDAR-BELT-1 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "location_type": "belt",
  "location": "TARAZEDAR-BELT-1",
  "belt": {
    "density": "sparse",
    "designation": "TARAZEDAR-BELT-1",
    "inner_radius_au": 0.6,
    "outer_radius_au": 0.9,
    "resources": {
      "carbon": "rich",
      "conductive": "scarce",
      "rares": "low",
      "silicates": "low",
      "structural": "moderate",
      "volatiles": "high"
    }
  },
  "devices": [],
  "inventory": [],
  "resource_sites": []
}
```

## Planets and moons

Planet and moon responses only include resource site details once you've sent a [survey drone](../../drones/survey/index.md) to the location.

GET /v1/locations/{code}   200 OK

```
$ curl https://api.replicant.space/v1/locations/TARAZEDAR-2-3 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "code": "TARAZEDAR-2-3",
  "location_type": "moon",
  "system": "TARAZEDAR",
  "parent": "TARAZEDAR-2",
  "surveyed": true,
  "atmosphere": true,
  "resource_sites": [
    { "resource": "silicates", "yield": 0.62 },
    { "resource": "rare_earth", "yield": 0.08 }
  ],
  "your_devices": 3,
  "your_resources": 412
}
```

## Your devices and resources

Any non-star location response also includes `devices`, `inventory` totals and `resource_sites` for that location.
