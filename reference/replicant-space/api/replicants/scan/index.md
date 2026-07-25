---
title: "Scan system"
source_url: "https://replicant.space/docs/api/replicants/scan/"
crawled_at: "2026-07-24T18:16:34.420502+00:00"
---

API · Replicants

# Scan system

A rudimentary scan of the planetary system your replicant is currently in, using the vessel's onboard sensors. Returns planets, belts, and outer-system bodies. Planet details, moons, and additional belt sites need a survey_drone.

## Endpoint

`POST /v1/replicants/{code}/scan`

## Example

POST /v1/replicants/{code}/scan   200 OK

```
$ curl -X POST https://api.replicant.space/v1/replicants/8AFE4482/scan \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "asteroid_belt": {
    "belts": [
      {
        "designation": "CHAMAKUY-BELT-1",
        "density": "dense",
        "inner_radius_au": 0.52,
        "outer_radius_au": 0.78,
        "resources": {
          "carbon": "scarce",
          "conductive": "high",
          "rares": "low",
          "silicates": "scarce",
          "structural": "high",
          "volatiles": "low"
        }
      }
    ],
    "present": true
  },
  "entry_point": "CHAMAKUY-5-L4",
  "outer_system": {
    "kuiper": {
      "designation": "CHAMAKUY-KUIPER",
      "distance_au": 19.21
    },
    "oort": {
      "designation": "CHAMAKUY-OORT",
      "distance_au": 2326.29
    }
  },
  "planets": [
    {
      "designation": "CHAMAKUY-1",
      "in_habitable_zone": false,
      "moon_count": 1,
      "orbital_distance_au": 0.141,
      "type": "Barren"
    },
    {
      "designation": "CHAMAKUY-2",
      "in_habitable_zone": true,
      "moon_count": 3,
      "orbital_distance_au": 0.205,
      "type": "Ocean World"
    },
    {
      "designation": "CHAMAKUY-3",
      "in_habitable_zone": false,
      "moon_count": 0,
      "orbital_distance_au": 0.395,
      "type": "Terrestrial"
    },
    {
      "designation": "CHAMAKUY-4",
      "in_habitable_zone": false,
      "moon_count": 0,
      "orbital_distance_au": 0.739,
      "type": "Frozen"
    },
    {
      "designation": "CHAMAKUY-5",
      "in_habitable_zone": false,
      "moon_count": 43,
      "orbital_distance_au": 1.241,
      "type": "Ice Giant"
    },
    {
      "designation": "CHAMAKUY-6",
      "in_habitable_zone": false,
      "moon_count": 1,
      "orbital_distance_au": 2.629,
      "type": "Frozen"
    }
  ],
  "replicants": {
    "Bob": {
      "last_active": "2026-05-10T23:01:19+01:00",
      "location": "CHAMAKUY-BELT-1",
      "replicant_code": "8AFE4482"
    }
  },
  "star": {
    "age_my": 5988.11,
    "color": "Red",
    "designation": "CHAMAKUY",
    "habitable_zone": {
      "inner_au": 0.18,
      "outer_au": 0.32
    },
    "luminosity_solar": 0.035611,
    "mass_solar": 0.2444,
    "position": {
      "x": 34.1906,
      "y": -8.9593,
      "z": -42.9832
    },
    "spectral_type": "M5",
    "temperature_k": 2977
  },
  "system_tags": [
    "binary_system"
  ]
}
```

## What you get

Onboard sensors are good enough to enumerate the major bodies in a system - planets, belts, Kuiper, Oort - but won't find moons, resource sites, salvage, or other players' devices. For those, print a [survey_drone](../../../drones/survey/index.md) and run a deep scan, or use a [survey controller](../../../ami/survey-controller/index.md) to automate it.

## Related

- [Scan devices](devices/index.md) - see other players' and NPC devices in the current system.
- [Locations](../../../concepts/locations/index.md) - the location grammar and scan completeness model.
