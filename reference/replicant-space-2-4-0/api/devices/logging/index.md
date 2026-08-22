---
title: "Logging"
source_url: "https://replicant.space/docs/api/devices/logging/"
crawled_at: "2026-08-07T00:51:30.117844+00:00"
---

API · Devices

# Logging

Pull the event log for a single device. Same event format as the replicant-level event log, scoped to one device.

## Endpoint

`GET /v1/devices/{code}/logs`

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `cursor` | integer · optional | Position in the list to start from, default null. |
| `limit` | integer · optional | Number of events to return, default 20. |
| `latest` | boolean · optional | Show latest events. Default `false`. Incompatible with `cursor`. |

## Example

GET /v1/devices/{code}/logs   200 OK

```
$ curl https://api.replicant.space/v1/devices/8F748260/logs?latest=true&limit=5 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "events": [
    {
      "created_at": "2026-06-06T22:00:05+01:00",
      "device_code": "8F748260",
      "device_type": "heaven_vessel",
      "event_type": "device_cruise_arrived",
      "id": 125836,
      "message": "Arrived at object MUROPE-OBJ-1",
      "payload": {
        "from_location": "MUROPE-5-L4",
        "location": "MUROPE-OBJ-1",
        "recalling": false
      }
    },
    {
      "created_at": "2026-06-06T22:00:03+01:00",
      "device_code": "8F748260",
      "device_type": "heaven_vessel",
      "event_type": "device_cruise_departed",
      "id": 125835,
      "message": "Cruising to object MUROPE-OBJ-1",
      "payload": {
        "attached_devices": [],
        "destination": "MUROPE-OBJ-1",
        "distance_au": 0.328,
        "origin": "MUROPE-5-L4",
        "recalling": false,
        "travel_time_seconds": 1.1
      }
    }
  ]
}
```

## AMI controller logs

When querying an [AMI controller](../../../ami/index.md), the log includes advanced directive evaluation entries. These show when the controller makes decisions and when things change. Useful for debugging a directive that isn't behaving the way you expect.
