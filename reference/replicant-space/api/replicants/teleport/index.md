---
title: "Teleport"
source_url: "https://replicant.space/docs/api/replicants/teleport/"
crawled_at: "2026-07-28T00:53:11.657140+00:00"
---

API · Replicants

# Teleport

Send your consciousness to an empty matrix in another star system. Requires an FTL network link between the two systems. You'll be offline for around 30 seconds while the transfer completes.

## Endpoint

`POST /v1/replicants/{code}/teleport`

## Pre-conditions

- An empty replicant matrix exists in the destination system, installed in a vessel and idle.
- An FTL link is established between your current system and the destination system - see [FTL Relays](../../../ftl-relays/index.md).

## Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `target` | string | Device code of the empty matrix to teleport into. |

## Example

POST /v1/replicants/{code}/teleport   202 accepted

```
$ curl -X POST https://api.replicant.space/v1/replicants/8AFE4482/teleport \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"target": "B22A1198"}'
```

response response

```
{
  "status": "teleporting",
  "source_star": "SOL",
  "destination_star": "POLIBUS",
  "started_at": "2026-05-10T14:30:00+01:00",
  "completes_at": "2026-05-10T14:30:30+01:00",
  "offline_seconds": 30,
  "target_matrix_code": "0799A49D"
}
```
