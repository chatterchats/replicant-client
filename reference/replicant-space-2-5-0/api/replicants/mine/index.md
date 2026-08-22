---
title: "Mine"
source_url: "https://replicant.space/docs/api/replicants/mine/"
crawled_at: "2026-08-11T15:11:29.009557+00:00"
---

API · Replicants

# Mine

Use your vessel's onboard mining equipment to extract resources at the current location. Useful early on, or when you accidentally left your drones back on FERGETEM-2.

## Endpoint

`POST /v1/replicants/{code}/mine`

## Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `resource_type` | string | One of `carbon`, `silicates`, `structural`, `conductive`, `rares`, `volatiles`. |

## Example

POST /v1/replicants/{code}/mine   202 accepted

```
$ curl -X POST https://api.replicant.space/v1/replicants/8AFE4482/mine \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"resource_type": "rares"}'
```

response response

```
{
  "status": "mining",
  "resource_type": "rares"
}
```

## Sorry, busy mining.

While the onboard mining rig is engaged, the matrix vessel cannot travel, scan, or print. Stop mining first - or use a [mining_drone](../../../drones/mining/index.md) which runs independently.
