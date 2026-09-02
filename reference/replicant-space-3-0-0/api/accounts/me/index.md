---
title: "Account details"
source_url: "https://replicant.space/docs/api/accounts/me/"
crawled_at: "2026-09-02T20:03:40.266247+00:00"
---

API · Accounts

# Account details

Your account profile - identity, status, the replicants under your control, notification preferences and BobNet subscriptions. One call to get everything.

## Endpoint

`GET /v1/accounts/me`

## Example

GET /v1/accounts/me   200 OK

```
$ curl https://api.replicant.space/v1/accounts/me \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "name": "bob",
  "email": "bob@example.com",
  "email_verified": true,
  "created_at": "2026-05-17T20:53:25+01:00",
  "experience_points_total": 87340,
  "status": "active",
  "timezone": "Europe/London",
  "unread_message_count": 1,
  "replicants": [
    {
      "created_at": "2026-05-17T20:53:43+01:00",
      "current_location": "REGULUZ-KUIPER",
      "current_star": "REGULUZ",
      "device_count": 5,
      "experience_points": 0,
      "hosted_device_code": "11ADA230",
      "name": "bob-1",
      "replicant_code": "77F75255"
    }
  ],
  "events": {
    "ami_digest_interval": 1,
    "muted": [
      "travel.*",
    ]
  },
  "messages": {
    "email": false,
    "subscribed": [
      "alert",
      "progression"
    ]
  },
  "bobnet_channels": [
    "#general",
    "#trade"
  ],
  "message_notify": {
    "email": false,
    "webhook": true,
    "preferences": {
      "ami": true,
      "devices": true,
      "location_events": true,
      "mining": true,
      "multiplayer": true,
      "printing": true,
      "progression": true,
      "scanning": true,
      "trade": true,
      "travel": true
    }
  }
}
```

## Notable fields in the response

- **experience_points_total** - the aggregated XP across every replicant under your account.
- **unread_message_count** - number of unread [messages](../messages/index.md). The same number is exposed on every API response via the `X-Replicant-Space-Unread-Count` header.
- **replicants** - summary of every replicant on the account. For full detail on one of them, call the [replicant details](../../replicants/index.md) endpoint.
- **events** - [event stream](../../events/stream/index.md) settings. `ami_digest_interval` controls [AMI digest](../../events/ami-digests/index.md) frequency as a multiplier on the default; `muted` lists suppressed [event types](../../events/catalogue/index.md) (supports wildcards).
- **messages** - message email subscription settings. `email` is the master toggle for story/system messages; `subscribed` is the list of message categories you'll receive emails for.
- **bobnet_channels** - the [BobNet](../../../concepts/bobnet/index.md) channels you're subscribed to.
- **message_notify** - notification settings - see [Account settings](../index.md) for more details.
