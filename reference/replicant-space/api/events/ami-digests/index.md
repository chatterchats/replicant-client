---
title: "AMI digests"
source_url: "https://replicant.space/docs/api/events/ami-digests/"
crawled_at: "2026-07-28T00:53:10.891040+00:00"
---

API · Events

# AMI digests

When devices are managed by an AMI controller, their individual events are buffered and compiled into periodic digest events. Instead of hundreds of mining.started and mining.stopped events, you get a single summary to keep you informed.

## How digests work

Devices adopted by an AMI controller stop emitting events directly to your account stream. Instead, their events are buffered for their controller to process. On each AMI evaluation, the controller drains the buffer and emits a single digest event containing:

- A **report** customised for the directive type (mining quanities, scan progress, transport volumes).
- An **activity** summary: total event count, per-event-type counts, and the time window covered.
- A **devices** list showing each device's current status, how many events it generated, and its last event type.

## Digest frequency

By default, a digest is emitted every evaluation tick (roughly every 10 seconds when there is activity).
For players with lots of AMI controllers, this can be slowed down via `PATCH /v1/accounts/me` by setting `events.ami_digest_interval` to a multiplier between 1 and 30.
A value of `5` means a digest is emitted at most every 5 ticks. Events continue to buffer during the gap, so no data is lost; the next digest covers a longer window.

## Inactive directives

Digest events are not sent when the controller's directive is inactive - for example, when the controller has been stowed. When you redeploy a previously active controller, you may notice some older events appearing in the next digest. These are buffered events from before the stow and can mostly be ignored.

While a directive is inactive, adopted devices that generate events will send them directly to your event stream as normal individual events rather than buffering them. This can happen when you manually scan with an adopted survey device while its controller is stowed, or when collecting resources with a transport drone after its delivery directive finishes. Any action taken by an adopted device whose controller's directive is not currently active will produce events on the stream directly.

## Digest event types

| Event | Controller | Report contents |
| --- | --- | --- |
| *ami.mining.digest* | Mining controller | Resource levels: actual vs. desired vs. capacity, exhaustion and status per resource type. |
| *ami.survey.digest* | Survey controller | Survey progress for the system scans and belt searching updates. |
| *ami.transport.digest* | Transport controller | Cargo collection and delivery status with shortfalls and total capacity. |

## Shared payload structure

All digest events share the same outer structure. The *report* object varies by controller type.

| Field | Type | Description |
| --- | --- | --- |
| *directive* | string | Name of the active directive (e.g. `deplete_smallest`, `survey_system`). |
| *report* | object | Directive-specific report data. See examples below. |
| *activity.event_count* | integer | Total buffered events since the last digest. |
| *activity.counts* | object | Breakdown by event name (e.g. `mining.started: 8`). |
| *activity.window* | [string, string] | ISO 8601 timestamps of the first and last buffered event. |
| *devices[]* | array | One entry per managed device. |
| *devices[].device_code* | string | Device code. |
| *devices[].status* | string | Current device status label. |
| *devices[].events* | integer | Events this device generated in this digest window. |
| *devices[].last_event* | string | Most recent event name from this device. |

## Mining digest example

response ami.mining.digest

```
{
  "directive": "deplete_smallest",
  "activity": {
    "counts": {
      "mining.resource_depleted": 1,
      "mining.started": 1
    },
    "event_count": 2,
    "window": [
      "2026-07-16T12:48:49+01:00",
      "2026-07-16T12:48:59+01:00"
    ]
  },
  "devices": [
    {
      "device_code": "4EC618A9",
      "events": 2,
      "last_event": "mining.started",
      "status": "mining (conductive)"
    },
    {
      "device_code": "54EE8574",
      "status": "mining (carbon)"
    },
    {
      "device_code": "5E37E65A",
      "status": "mining (conductive)"
    }
  ],
  "report": {
    "location": "NUNKA-BELT-1",
    "resources": {
      "carbon": {
        "actual": 1,
        "capacity": 28,
        "desired": 1
      },
      "conductive": {
        "actual": 2,
        "capacity": 11,
        "desired": 2
      },
      "rares": {
        "actual": 0,
        "capacity": 0,
        "desired": 0,
        "exhausted": true
      },
      "silicates": {
        "actual": 0,
        "capacity": 11,
        "desired": 0
      },
      "structural": {
        "actual": 0,
        "capacity": 195,
        "desired": 0
      },
      "volatiles": {
        "actual": 0,
        "capacity": 0,
        "desired": 0,
        "exhausted": true
      }
    }
  }
}
```

In the resource report, `actual` and `desired` refer to the number of mining drones assigned to that resource type. `capacity` is the total amount of that resource available across all sites at the location. When a resource is fully depleted, `exhausted` is set to `true`.

## Survey digest example

response ami.survey.digest

```
{
  "directive": "survey_system",
  "activity": {
    "counts": {
      "salvage.discovered": 1,
      "scan.completed": 3,
      "travel.departed": 3
    },
    "event_count": 7,
    "window": [
      "2026-07-16T12:58:01+01:00",
      "2026-07-16T12:58:19+01:00"
    ]
  },
  "devices": [
    {
      "device_code": "00AC58D1",
      "events": 2,
      "last_event": "travel.departed",
      "status": "travelling"
    },
    {
      "device_code": "0F20C3DC",
      "events": 2,
      "last_event": "travel.departed",
      "status": "travelling"
    },
    {
      "device_code": "73983EFE",
      "events": 3,
      "last_event": "travel.departed",
      "status": "travelling"
    }
  ],
  "report": {
    "assigned_this_tick": 3,
    "busy": 3,
    "idle": 0,
    "progress": {
      "remaining": 8,
      "scanned": 28,
      "total": 36
    },
    "scans": [
      {
        "device_code": "00AC58D1",
        "scan_target": "NUNKA-3",
        "scan_type": "planet",
        "report": { ... }
      },
      {
        "device_code": "0F20C3DC",
        "scan_target": "NUNKA-4",
        "scan_type": "planet",
        "report": { ... }
      },
      {
        "device_code": "73983EFE",
        "scan_target": "NUNKA-5",
        "scan_type": "planet",
        "report": { ... }
      }
    ]
  }
}
```

The `scans` array contains one entry for each `scan.completed` event that occurred during the digest window. Each entry includes the `device_code` that performed the scan, the `scan_target` name, the `scan_type`, and the full scan `report` object. The report contents vary by scan type and can be large — see the [scans documentation](https://replicant.space/docs/api/scans/) for the full report structure. When no scans completed during the window, the array is empty.

## Transport digest examples

### Ferry directive

response ami.transport.digest

```
{
  "directive": "ferry",
  "activity": {
    "counts": {
      "travel.departed": 1
    },
    "event_count": 1,
    "window": [
      "2026-07-16T09:09:41+01:00",
      "2026-07-16T09:09:41+01:00"
    ]
  },
  "devices": [
    {
      "device_code": "43B427DC",
      "events": 1,
      "last_event": "travel.departed",
      "status": "cruising"
    },
    {
      "device_code": "88E8E2BA",
      "status": "surging"
    },
    {
      "device_code": "FD4580C2",
      "status": "cruising"
    }
  ],
  "report": {
    "cargo_capacity": 60,
    "cargo_carried": 20,
    "collect": "ULKURUD-1",
    "deliver": "NUNKA-BELT-1",
    "fleet": {
      "delivering": 3,
      "loading": 0,
      "waiting": 0
    }
  }
}
```

### Delivery directive

response ami.transport.digest

```
{
  "directive": "delivery",
  "activity": {
    "counts": {
      "transport.collected": 3,
      "travel.arrived": 3,
      "travel.departed": 3
    },
    "event_count": 9,
    "window": [
      "2026-07-16T13:06:22+01:00",
      "2026-07-16T13:06:38+01:00"
    ]
  },
  "devices": [
    {
      "device_code": "43B427DC",
      "events": 3,
      "last_event": "travel.departed",
      "status": "travelling"
    },
    {
      "device_code": "88E8E2BA",
      "events": 3,
      "last_event": "travel.departed",
      "status": "travelling"
    },
    {
      "device_code": "FD4580C2",
      "events": 3,
      "last_event": "travel.departed",
      "status": "travelling"
    }
  ],
  "report": {
    "cargo_capacity": 60,
    "cargo_carried": 50,
    "collect": "NUNKA-BELT-1",
    "deliver": "NUNKA-4",
    "fleet": {
      "delivering": 3,
      "loading": 0,
      "waiting": 0
    },
    "requirement": {
      "carbon": 150,
      "silicates": 200
    },
    "shortfalls": {
      "carbon": 23,
      "silicates": 27
    }
  }
}
```

## Filtering

AMI digest events are never suppressed by mute patterns. They always appear in both the [event logs](../logs/index.md) and [event stream](../stream/index.md), regardless of your filter configuration.
