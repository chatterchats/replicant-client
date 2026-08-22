---
title: "Star catalogue"
source_url: "https://replicant.space/docs/api/locations/star-catalogue/"
crawled_at: "2026-08-22T22:43:50.786965+00:00"
---

API · Locations

# Star catalogue

Download the entire star catalogue in a single request. The response is pre-built and cached, so it returns instantly regardless of how many stars have been discovered.

## Endpoint

`GET /v1/stars`

## Rate limit

This endpoint is limited to **1 request per minute** per account. The catalogue is the same for all accounts, so there's no need to poll it frequently.

## Caching

The catalogue is regenerated every **5 minutes**. The `generated_at` field tells you when the current snapshot was built. Newly discovered stars will appear in the catalogue within 5 minutes of discovery.

## Example

GET /v1/stars   200 OK

```
$ curl https://api.replicant.space/v1/stars \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "total": 24,
  "generated_at": "2026-07-12T14:30:00.123456+00:00",
  "stars": [
    {
      "designation": "CYGNUS",
      "name": "Cygnus Prime",
      "spectral_type": "M5",
      "color": "yellow",
      "position": {
        "x": 41.2,
        "y": -11.4,
        "z": 30.9
      },
      "estimated_planets": 6,
      "entry_point": CYGNUS-5-L4,
      "has_hub": true,
      "region": "solzone",
    },
    {
      "designation": "TARAZEDAR",
      "spectral_type": "K4",
      "color": "red",
      "position": {
        "x": -2.55,
        "y": 0.44,
        "z": 6.71
      },
      "estimated_planets": 4,
      "has_ward": true,
      "region": "solzone"
    }
  ]
}
```

## Notes

The catalogue does not include account-specific fields like distance from your replicant, travel time estimates, or whether you've explored a system. Use [nearest stars](../../replicants/stars/index.md) for distance-aware listings relative to a specific replicant.

If the catalogue is still being prepared (e.g. immediately after a server restart), the endpoint returns `503` with a message to retry shortly.

## Hubs and wards

Stars with a [system hub](../../../system-hubs/index.md) include *has_hub: true*, and stars with a [system ward](../../../system-wards/index.md) include *has_ward: true*. These fields are omitted when the system has no hub or ward.

A [system hub](../../../system-hubs/index.md) is your claim on a star system - it locks mining, grants naming rights, boosts incoming surge travel, and extends FTL relay range. A [system ward](../../../system-wards/index.md) is a lighter alternative that just locks mining and species interactions.
