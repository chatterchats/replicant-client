---
title: "Transfer matrix"
source_url: "https://replicant.space/docs/api/replicants/transfer/"
crawled_at: "2026-07-24T18:16:34.595379+00:00"
---

API · Replicants

# Transfer matrix

Local version of teleport. Instantly move your consciousness from one replicant matrix to another empty matrix at the same location. No FTL link needed - the matrices just need to be near each other.

## Endpoint

`POST /v1/replicants/{code}/transfer`

## Pre-conditions

- The target matrix is installed in a vessel at the same location as your current host device.
- The target matrix is empty.

## Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `target` | string | Device code of the empty matrix to transfer into. |

## Example

POST /v1/replicants/{code}/transfer   200 OK

```
$ curl -X POST https://api.replicant.space/v1/replicants/8AFE4482/transfer \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"target": "B22A1198"}'
```

response response

```
{
  "status": "transferred",
  "old_host": "B22A1198",
  "new_host": "0799A49D"
}
```

## Transfer vs teleport

Transfer is instant and only works within the same location. For interstellar moves, use [teleport](../teleport/index.md), which goes through the FTL network and takes ~30 seconds.
