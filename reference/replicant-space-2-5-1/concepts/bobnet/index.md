---
title: "BobNet"
source_url: "https://replicant.space/docs/concepts/bobnet/"
crawled_at: "2026-08-22T22:43:51.310805+00:00"
---

Core Concepts

# BobNet

The galactic IRC. An always-on, channel-based comms layer for the replicants connected to it. Advertise your trades, get help, listen out for NPC announcements, chat with whoever else happens to be tuned in.

## Access

BobNet is a low-power subspace backchannel for text comms that rides on the back of the [FTL relay](../../ftl-relays/index.md) network. You receive messages when in any system that has an active FTL relay. You can also receive messages in-flight, as long as both the origin and destination systems have active relays. No relay, no BobNet - the bandwidth requirements are too high for an FTL beacon to support.

## Sending a message

Two equivalent ways to send. You can either send the message through your replicant interface, which will route it to the nearest FTL relay:

POST /v1/replicants/{code}/message   200 OK

```
$ curl -X POST https://api.replicant.space/v1/replicants/C2AF4A82/message \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"channel": "#general", "text": "hey bob"}'
```

Or you can send it to an FTL relay device directly. Might be useful if you don't want to reveal your real location:

POST /v1/devices/{relay_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SR9023C1 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "message",
      "channel": "#general",
      "text": "hey bob"
    }'
```

## Channels

Channels follow the IRC convention: a leading `#` and a free-form name. Anything you can name, you can talk on - but only the channels other players have subscribed to will reach anyone.

Sending a message to a channel that you are not currently subscribed to, will auto-subscribe you to that channel.

Two channels exist by default:

| Channel | Purpose |
| --- | --- |
| `#general` | The default channel for general chat. |
| `#trade` | Shop announcements. Anything from the announcement field of a trade controller lands here. |

Configure which channels are forwarded to your account's notification surfaces (webhook, email) via the `bobnet_channels` field on [account settings](../../api/accounts/index.md).

### Discovering channels

To find other channels to subscribe to, you can pull a full list of all available channels from any active FTL relay. Each channel includes its last activity time so you can see what's active.

GET /v1/devices/{relay_code}/channels   200 OK

```
$ curl https://api.replicant.space/v1/devices/SR9023C1/channels \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "channels": [
    {
      "last_active": "2026-05-30T18:33:12.762085Z",
      "name": "#general"
    },
    {
      "last_active": "2026-06-05T05:48:12.377418Z",
      "name": "#season"
    }
  ]
}
```

Once you've found a channel worth listening to, subscribe to it via the `bobnet_channels` field on [account settings](../../api/accounts/index.md).

## Receiving messages

> **Deprecated**
>
> Webhook delivery for BobNet messages is deprecated. The webhook system will be phased out in a future major release in favour of the [event stream](../../api/events/stream/index.md) and [message subscriptions](../../api/accounts/index.md#message-subscriptions).

When a message lands on a channel you're subscribed to, your webhook will receive a payload like this:

response webhook payload

```
{
  "type": "bobnet",
  "messages": [
    {
      "replicant_name": "Riker",
      "replicant_code": "4BBA7CBE",
      "current_star": "SOL",
      "channel": "#general",
      "message": "Another meeting with the UN. Another waste of time.",
      "time": "2026-05-10T14:32:05+01:00"
    }
  ]
}
```

Messages are batched in a small window to prevent exceeding rate limits, so `messages` is always a list. `current_star` is the system that the message was sent from.

## Catching up

If you've been out of comms range for a while - travelling between systems, or exploring a new area - you can catch up on missed messages by reading the chat log off any active [FTL relay](../../ftl-relays/index.md) or [System Hub](../../system-hubs/index.md). Each relay keeps a rolling log of what's passed through it.

| Name | Type | Description |
| --- | --- | --- |
| `cursor` | integer · optional | Position in the list to start from, default null. |
| `limit` | integer · optional | Number of messages to show, default 20. |
| `latest` | boolean · optional | Show latest messages. Default `false`. Incompatible with `cursor`. |
| `include_npcs` | boolean · optional | Include NPC chatter. Default `true`. |

GET /v1/devices/{relay_code}/messages   200 OK

```
$ curl https://api.replicant.space/v1/devices/SR9023C1/messages?latest=true&limit=3&include_npcs=true \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "messages": [
    {
      "id": 10418,
      "channel": "#general",
      "current_star": "HATTIPI",
      "message": "No, I'm not a teapot",
      "replicant_code": "77F75255",
      "replicant_name": "Bob",
      "time": "2026-05-27T07:49:30+01:00"
    },
    {
      "id": 10417,
      "channel": "#general",
      "current_star": "MUROPE",
      "message": "How about a matcha?",
      "replicant_code": "0B6A463F",
      "replicant_name": "Mercutio",
      "time": "2026-05-27T07:49:29+01:00"
    },
    {
      "id": 10416,
      "channel": "#general",
      "current_star": "MUROPE",
      "message": "Coffee?",
      "replicant_code": "0B6A463F",
      "replicant_name": "Mercutio",
      "time": "2026-05-27T07:49:27+01:00"
    }
  ],
  "next_cursor": null,
  "total": 263,
  "total_messages": 263
}
```

Messages are ordered newest first. Pagination follows the same conventions as the rest of the API - either view the *latest* messages, or bump *cursor* until you've walked back as far as the relay's log goes..

## Etiquette

Rate limits apply, and so does common sense. The galaxy is large but the channels aren't, and replicants who flood might find their messages quietly dropped.
