---
title: "Event log"
source_url: "https://replicant.space/docs/api/replicants/events/"
crawled_at: "2026-08-07T00:51:30.902050+00:00"
---

API · Replicants

# Event log

We love logs. Anything that happens will be in the logs. Scan results, transport collection and deposits, travel arrival, belt mining site depletion. Everything.

> **Deprecated**
>
> The per-replicant event log is deprecated and will be phased out in a future major release. New integrations should use the [event stream](../../events/stream/index.md) instead, which delivers events across all your replicants in real time.

## Endpoint

`GET /v1/replicants/{code}/events`

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `cursor` | integer · optional | Position in the list to start from, default null. |
| `limit` | integer · optional | Number of events to show, default 20. |
| `latest` | boolean · optional | Show latest events. Default `false`. Incompatible with `cursor`. |
| `event_type` | string · optional | Filter by results by type of event. |
| `device_type` | string · optional | Filter results by the type of device. |
| `device` | string · optional | Filter results to a single device code. |

## Example

GET /v1/replicants/{code}/events   200 OK

```
$ curl https://api.replicant.space/v1/replicants/8AFE4482/events?latest=true \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "events": [
    {
      "created_at": "2026-05-10T22:31:05+01:00",
      "device_code": "0B086F53",
      "device_type": "ami_mining_controller",
      "event_type": "device_deployed",
      "message": "Deployed ami_mining_controller 0B086F53 at CHAMAKUY-BELT-1",
      "payload": {
        "location": "CHAMAKUY-BELT-1",
        "star": "CHAMAKUY"
      }
    },
    {
      "created_at": "2026-05-10T00:19:02+01:00",
      "device_code": "37C51F74",
      "device_type": "heaven_vessel",
      "event_type": "print_complete",
      "message": "Completed printing ami_mining_controller",
      "payload": {
        "device_type": "ami_mining_controller",
        "location": "CHAMAKUY-BELT-1",
        "new_device_code": "0B086F53"
      }
    }
  ]
}
```

## Event payloads

The `payload` field shape depends on `event_type`. The `message` field is always a human-readable summary.
