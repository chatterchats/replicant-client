---
title: "Event logs"
source_url: "https://replicant.space/docs/api/events/logs/"
crawled_at: "2026-08-22T22:43:50.553932+00:00"
---

API · Events

# Event logs

Fetch a page of recent events for your account. The default view returns the most recent events with the latest at the bottom, ready to append to a feed. Use the cursor to page forward through history.

## Endpoint

`GET /v1/events`

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `cursor` | string · optional | ID to read after. Use `next_cursor` from the previous response to page forward. |
| `limit` | integer · optional | Number of events to return, default 100 (max 100). |
| `filtered` | boolean · optional | Apply your account's muted event patterns. Default `false`. |
| `device_code` | string · optional | Filter to events from a specific device (e.g. `2AC61214`). |
| `category` | string · optional | Filter by category (e.g. `mining`, `travel`, `ami`). |
| `event` | string · optional | Filter by event name (e.g. `mining.started`). |
| `after` | string · optional | Return events at or after this time. ISO 8601 format; naive timestamps are treated as your account timezone. |
| `before` | string · optional | Return events before this time. Same format rules as `after`. |

## Example

GET /v1/events   200 OK

```
$ curl "https://api.replicant.space/v1/events?category=mining&limit=3" \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "events": [
    {
      "id": "1784292742869-0",
      "version": 1,
      "category": "mining",
      "event": "mining.started",
      "replicant_code": "BC6F680D",
      "device_code": "32763CDF",
      "device_type": "mining_drone",
      "star": "ABOTEIN",
      "location": "ABOTEIN-BELT-1",
      "payload": {
        "availability": "high",
        "belt": "ABOTEIN-BELT-1",
        "cycle_time_seconds": 52,
        "density": "moderate",
        "designation": "ABOTEIN-BELT-1-SITE-0",
        "location": "ABOTEIN-BELT-1",
        "location_type": "belt",
        "resource_type": "structural",
        "site": "ABOTEIN-BELT-1-SITE-0"
      },
      "created_at": "2026-07-17T13:52:22+01:00"
    },
    {
      "id": "1784292762940-0",
      "version": 1,
      "category": "mining",
      "event": "mining.stopped",
      "replicant_code": "BC6F680D",
      "device_code": "32763CDF",
      "device_type": "mining_drone",
      "star": "ABOTEIN",
      "location": "ABOTEIN-BELT-1",
      "payload": {
        "belt": "ABOTEIN-BELT-1",
        "location": "ABOTEIN-BELT-1",
        "quantity_mined": 0,
        "resource_type": "structural"
      },
      "created_at": "2026-07-17T13:52:42+01:00"
    }
  ],
  "next_cursor": "1784292762940-0"
}
```

## Pagination

Without a cursor, the endpoint returns the most recent `limit` events in chronological order - latest at the bottom.
When the response includes a `next_cursor`, pass it as the `cursor` query parameter on your next request to fetch the next page.
When `next_cursor` is `null`, there are no more events to read.

## Event envelope

Every event shares a common envelope. The `payload` field varies by event type - see the [event catalogue](../catalogue/index.md) for the full list.

| Field | Type | Description |
| --- | --- | --- |
| `id` | string | Event ID. Use as a cursor value. |
| `version` | integer | Envelope version (currently `1`). |
| `category` | string | Event category (e.g. `mining`, `travel`, `device`). |
| `event` | string | Dotted event name (e.g. `mining.started`). |
| `replicant_code` | string | Replicant that owns the device or triggered the action. |
| `device_code` | string? | Device code, if the event relates to a specific device. |
| `device_type` | string? | Device type, when `device_code` is set. |
| `star` | string? | Star system designation. |
| `location` | string? | Specific location code (planet, belt, moon, etc.). |
| `payload` | object | Event-specific data. Shape varies by `event` name. |
| `created_at` | string | ISO 8601 timestamp, localised to your account timezone. |

## Muted events

You can configure mute patterns via [account settings](../../accounts/index.md) to suppress noisy event types.
Patterns use `*` as a wildcard for one dot-separated segment (e.g. `mining.*` mutes `mining.started` and `mining.stopped`).
Set `filtered=true` on this endpoint to apply your mute list.
AMI digest events are never muted.

The SSE stream at `/v1/events/stream` always applies your mute list. Filters are compiled when the connection opens, so if you change your muted patterns you will need to reconnect the stream for the new filters to take effect.
