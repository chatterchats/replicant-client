---
title: "Tagging"
source_url: "https://replicant.space/docs/api/devices/tagging/"
crawled_at: "2026-08-07T00:51:30.177204+00:00"
---

API · Devices

# Tagging

Label your devices with free-form tags so you can fetch their device status in a batched response. Save yourself hundreds of status requests and organise your devices efficiently.

## Setting tags

`PATCH /v1/devices/{code}`

Pass a list of tags in the `configuration` body. This replaces the full tag list on that device - include every tag you want it to keep.

If you just want to add or remove a few tags without sending the full list, use `add_tags` and `remove_tags` instead. These are mutually exclusive with `tags` - you can use one or the other in a single request, not both.

Tags can be up to 32 characters long and may contain lowercase letters, numbers, hyphens, and underscores.

| Field | Type | Description |
| --- | --- | --- |
| `tags` | string[] | Replace the entire tag list. Cannot be combined with `add_tags` / `remove_tags`. |
| `add_tags` | string[] | Append tags to the existing list. Cannot be combined with `tags`. |
| `remove_tags` | string[] | Remove tags from the existing list. Cannot be combined with `tags`. |

PATCH /v1/devices/{code}   200 OK

```
# tag a device
$ curl -X PATCH https://api.replicant.space/v1/devices/B58FCC78 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"configuration": {"tags": ["autofactories", "group-alpha"]}}'
```

response response

```
{
  "device_code": "B58FCC78",
  "tags": [
    "autofactories",
    "group-alpha"
  ]
}
```

## Listing devices by tag

`GET /v1/devices/tags/{tag}`

Returns every device that carries the given tag. Each entry in the list is a full [device details](../retrieve/index.md) response.

### Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `cursor` | integer · optional | Position in the list to start from, default null. |
| `limit` | integer · optional | Number of devices to return, default 10 (max 50). |

## Example

GET /v1/devices/tags/{tag}   200 OK

```
$ curl https://api.replicant.space/v1/devices/tags/sol-miners?limit=2 \
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
      "location": "TARAZEDAR-BELT-1",
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
  "next_cursor": 3
}
```
