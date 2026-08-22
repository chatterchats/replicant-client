---
title: "Inventory"
source_url: "https://replicant.space/docs/api/locations/inventory/"
crawled_at: "2026-08-11T15:11:28.794085+00:00"
---

API · Locations

# Inventory

Query your resource stockpiles across your locations. Filter by star or specific location, page through results with a cursor.

## Endpoint

`GET /v1/inventory`

Returns your stockpiled resources, grouped by location. Results are sorted alphabetically by location code.

The legacy endpoint for location inventory is still supported, but deprecated in favour of this one, and may be removed in the future: `GET /v1/locations/{code}/inventory`.

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `location` | string · optional | Filter by location. A star code like `TARAZEDAR` returns all stockpiles in that system. A specific location like `TARAZEDAR-BELT-1` returns that location only. |
| `cursor` | string · optional | Location code to page from. Use `next_cursor` from the previous response. |
| `limit` | integer · optional | Number of locations to return, default 20 (max 50). |

## Pagination

Results are ordered alphabetically by location code. When more results are available, the response includes a `next_cursor` value. Pass it as the `cursor` query parameter on the next request to fetch the next page. When `next_cursor` is `null`, there are no more results.

## By system

Pass a star code to get a per-location breakdown of every stockpile in that system.

GET /v1/inventory   200 OK

```
$ curl https://api.replicant.space/v1/inventory?location=TARAZEDAR&limit=2 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "locations": [
    {
      "location": "TARAZEDAR-2-3",
      "items": {
        "rares": 38,
        "silicates": 412
      }
    },
    {
      "location": "TARAZEDAR-BELT-1",
      "items": {
        "carbon": 348,
        "structural": 9234
      }
    }
  ],
  "next_cursor": "TARAZEDAR-BELT-1"
}
```

## Single location

Pass a specific location code to get the inventory at that one location.

GET /v1/inventory   200 OK

```
$ curl https://api.replicant.space/v1/inventory?location=TARAZEDAR-BELT-1 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "locations": [
    {
      "location": "TARAZEDAR-BELT-1",
      "items": {
        "carbon": 348,
        "conductive": 412,
        "silicates": 1820,
        "structural": 9234
      }
    }
  ],
  "next_cursor": null
}
```
