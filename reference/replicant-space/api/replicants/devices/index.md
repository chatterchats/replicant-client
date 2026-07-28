---
title: "List devices"
source_url: "https://replicant.space/docs/api/replicants/devices/"
crawled_at: "2026-07-28T00:53:11.393062+00:00"
---

API · Replicants

# List devices

Returns all the devices the replicant owns, as long as they are in range. Travel to another system and forget to establish FTL comms, and you will lose track of your old devices.

## Endpoint

`GET /v1/replicants/{code}/devices`

## Path parameters

| Name | Type | Description |
| --- | --- | --- |
| `code` | string | The replicant whose devices you want to list. |

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `location` | string · optional | Filter results to a specific location code. |
| `device_type` | string · optional | Filter by device type (e.g. `mining_drone`). |
| `cursor` | integer · optional | Return devices with an internal ID greater than this value. Use `next_cursor` from the previous response. |
| `limit` | integer · optional | Number of devices to return, default 20 (max 50). |

## Pagination

Results are ordered by device ID ascending. The `cursor` query parameter means "return devices with ID greater than this value". When another page exists, the response includes a `next_cursor` value set to the ID of the last returned device. Pass it as the `cursor` query parameter on the next request to fetch the next page. When `next_cursor` is `null`, there are no more results.

## Example

GET /v1/replicants/{code}/devices   200 OK

```
$ curl https://api.replicant.space/v1/replicants/57F0F6C8/devices?device_type=mining_drone&limit=2 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "devices": [
    {
      "device_code": "2AC61214",
      "device_type": "mining_drone",
      "location": "SOL-BELT-1",
      "status": "mining",
      "operational_capacity": 0.92
    },
    {
      "device_code": "7FE981A0",
      "device_type": "mining_drone",
      "location": "SOL-BELT-1",
      "status": "mining (structural)",
      "operational_capacity": 0.99
    }
  ],
  "next_cursor": 4821
}
```
