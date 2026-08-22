---
title: "List devices"
source_url: "https://replicant.space/docs/api/devices/list/"
crawled_at: "2026-08-22T22:43:50.088934+00:00"
---

API · Devices

# List devices

Fetch a batched list of every device on your account. Filter by replicant, device type, or location and page through results with a cursor. Useful for players managing hundreds or thousands of devices across multiple systems.

## Endpoint

`GET /v1/devices`

Returns all active (non-decommissioned) devices owned by any replicant on the account. Each entry in the list is a full [device details](../retrieve/index.md) response.

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `replicant_code` | string · optional | Filter to a single replicant's devices. |
| `device_type` | string · optional | Filter by device type (e.g. `mining_drone`). |
| `tag` | string · optional | **Deprecated** - use `tags` instead. Filter to devices with this exact tag. |
| `tags` | string · optional | Comma-separated tag patterns. Returns devices matching any pattern. Supports `*` wildcards - e.g. `squad2:*` or `*:miners`. Cannot be combined with `tag`. |
| `exclude_tags` | string · optional | Comma-separated tag patterns to exclude. Devices matching any pattern are omitted. Supports `*` wildcards. Can be combined with `tag` or `tags`. |
| `untagged` | boolean · optional | Filter to devices with no tags. Incompatible with `tag`, `tags`, and `exclude_tags`. |
| `location` | string · optional | Filter by location. A star code like `SOL` matches all devices in that system. A specific location like `SOL-1`, `SOL-BELT-1`, or `SOL-3-L4` matches that exact location. |
| `cursor` | integer · optional | Device ID to page from. Use `next_cursor` from the previous response. |
| `limit` | integer · optional | Number of devices to return, default 20 (max 50). |

## Pagination

Results are ordered by device ID. When more results are available, the response includes a `next_cursor` value. Pass it as the `cursor` query parameter on the next request to fetch the next page. When `next_cursor` is `null`, there are no more results.

## Example

GET /v1/devices   200 OK

```
$ curl https://api.replicant.space/v1/devices?device_type=mining_drone&location=SOL&limit=2 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "devices": [
    {
      "device_code": "B58FCC78",
      "device_type": "mining_drone",
      "replicant_code": "4BBA7CBE",
      "location": "SOL-BELT-1",
      "features": [
        "cruise",
        "mine",
        "stow"
      ],
      "available_commands": [
        "change_owner",
        "deactivate",
        "decommission",
        "deploy",
        "recall",
        "retarget",
        "start_mining",
        "stow",
        "travel"
      ],
      "operational_capacity": 67.0,
      "status": "mining (rares)"
    },
    {
      "device_code": "9AC4E21F",
      "device_type": "mining_drone",
      "replicant_code": "4BBA7CBE",
      "location": "SOL-BELT-1",
      "features": [
        "cruise",
        "mine",
        "stow"
      ],
      "available_commands": [
        "change_owner",
        "deactivate",
        "decommission",
        "deploy",
        "recall",
        "retarget",
        "start_mining",
        "stow",
        "travel"
      ],
      "operational_capacity": 100.0,
      "status": "mining (silicates)"
    }
  ],
  "next_cursor": 4821
}
```
