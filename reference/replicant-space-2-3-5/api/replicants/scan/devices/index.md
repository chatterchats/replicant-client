---
title: "Scan devices"
source_url: "https://replicant.space/docs/api/replicants/scan/devices/"
crawled_at: "2026-08-03T00:42:37.427352+00:00"
---

API · Replicants

# Scan devices

Pick up the pings of devices owned by other players and NPCs in the system. Anything broadcasting locally that isn't yours shows up here.

## Endpoint

`GET /v1/replicants/{code}/scan/devices`

Every deployed device broadcasts a basic ping. Your replicant sees those pings while in the system. This endpoint lists everything you can see that is owned by other players and NPCs. Your own devices and escrow-held trade inventory are excluded; use [List devices](../../../devices/list/index.md) for those.

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `device_type` | string · optional | Filter by device type (e.g. `survey_drone`). |
| `owner_replicant_code` | string · optional | Filter to devices owned by a replicant. |
| `cursor` | integer · optional | Device ID to page from. Use `next_cursor` from the previous response. |
| `limit` | integer · optional | Number of devices to return, default 20 (max 50). |

## Pagination

Results are ordered by device ID. When more results are available, the response includes a `next_cursor` value. Pass it as the `cursor` query parameter on the next request to fetch the next page. When `next_cursor` is `null`, there are no more results.

## Example

GET /v1/replicants/{code}/scan/devices   200 OK

```
$ curl https://api.replicant.space/v1/replicants/8AFE4482/scan/devices?device_type=survey_drone&limit=2 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "star": "CHAMAKUY",
  "device_count": 2,
  "devices": [
    {
      "device_code": "D8C2A140",
      "device_type": "survey_drone",
      "location": "CHAMAKUY-2",
      "owner_replicant_code": "4A1F0B22",
      "owner_name": "helga-3"
    },
    {
      "device_code": "E1B79301",
      "device_type": "survey_drone",
      "location": "CHAMAKUY-5",
      "owner_replicant_code": "4A1F0B22",
      "owner_name": "helga-3"
    }
  ],
  "next_cursor": 7122
}
```
