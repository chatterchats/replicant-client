---
title: "Account settings"
source_url: "https://replicant.space/docs/api/accounts/"
crawled_at: "2026-08-25T15:34:30.299006+00:00"
---

API · Accounts

# Account settings

Full control over your name, email, timezone, notification preferences and BobNet channels.

## Endpoint

`PATCH /v1/accounts/me`

## Example

PATCH /v1/accounts/me   200 OK

```
$ curl -X PATCH https://api.replicant.space/v1/accounts/me \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "name": "Bob",
      "email": "bob@example.com",
      "timezone": "Europe/London",
      "replicant_cooperation": "shared",
      "message_notify": {
        "email": false,
        "webhook": true,
        "preferences": {
          "email": {
            "ami": true,
            "devices": true,
            "location_events": true,
            "mining": true,
            "multiplayer": true,
            "printing": true,
            "progression": true,
            "scanning": true,
            "trade": true,
            "travel": true,
            "hub": true
          },
          "webhook": {
            "ami": true,
            "devices": true,
            "location_events": true,
            "mining": true,
            "multiplayer": true,
            "printing": true,
            "progression": true,
            "scanning": true,
            "trade": true,
            "travel": true,
            "hub": true
          }
        }
      },
      "events": {
        "ami_digest_interval": 1,
        "muted": [
          "travel.departed",
          "experience.*"
        ]
      },
      "messages": {
        "email": true,
        "subscribed": [
          "alert",
          "progression",
          "simulation"
        ]
      },
      "bobnet_channels": ["#general","#trade"]
    }'
```

## Fields

- `name` - display name on your account.
- `email` - the email address associated with your account. Changing this will invoke a verification process.
- `timezone` - IANA timezone for any time-of-day formatting in emails and notifications.
- `replicant_cooperation` - controls how your replicants share devices. `"individual"` (default) means each replicant controls its own devices, with optional per-replicant overrides. `"shared"` means all your replicants can freely operate on each other's devices.
- `events.ami_digest_interval` - how often [AMI digests](../events/ami-digests/index.md) are delivered, as a multiplier on the default.
- `events.muted` - list of [event types](../events/catalogue/index.md) to suppress. Supports wildcards (e.g. `"experience.*"`).
- `message_notify` - webhook and email notification settings (see deprecation note below).
- `messages.email` - master toggle for receiving any emails from the game.
- `messages.subscribed` - list of message categories to receive emails for (see below).
- `bobnet_channels` - BobNet channels to subscribe to.

> **Deprecated**
>
> The `message_notify` section is deprecated. The webhook system will be phased out in a future major release in favour of the new [event system](../events/stream/index.md) and [message subscriptions](index.md#message-subscriptions).

Note that BobNet messages can only be sent via webhook notifications currently.

## Message subscriptions

The *messages.subscribed* list controls which in-game message categories trigger an email notification. If emails are enabled, you will always receive *account* (maintenance, updates, etc) and *story* (emails from NPCs with seasonal content) messages. All other categories require explicit subscription.

Available emails to subscribe to:

- *alert* - asteroids, salvage, new location events
- *social* - hub greetings, hub activations
- *simulation* - started, completed, timeouts
- *progression* - new blueprints, achievements, event completion

Subscription is at the category level - subscribing to *"alert"* enables emails for all alert subcategories (asteroids, salvage discoveries, location events).

## Replicant cooperation

The `replicant_cooperation` field controls whether your replicants can operate on each other's devices - stowing, AMI adoption, attaching, device lists, and more. See [Replicants & Accounts](../../concepts/replicants/index.md#replicant-cooperation) for the full explanation of how the two-tier permission model works.

PATCH /v1/accounts/me   200 OK

```
$ curl -X PATCH https://api.replicant.space/v1/accounts/me \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"replicant_cooperation": "shared"}'
```

response Response   200 OK

```
{
  "replicant_cooperation": "shared",
  "name": "Bob",
  "email": "bob@example.com",
  ...
}
```

When set to `"shared"`, every replicant under your account can see and command every sibling's devices. When set to `"individual"`, each replicant's `cohort_permission` (set via `PATCH /v1/replicants/<code>`) determines whether siblings can access its devices - `"public"` allows access, `"private"` denies it.
