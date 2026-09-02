---
title: "Webhook"
source_url: "https://replicant.space/docs/api/accounts/webhook/"
crawled_at: "2026-09-02T20:03:40.384251+00:00"
---

API · Accounts

# Webhook

Register your own URL to receive signed event payloads from the game. Payloads are signed, so you can prove it came from us.

> **Deprecated**
>
> The webhook system is deprecated and will be phased out in a future major release. New integrations should use the [event stream](../../events/stream/index.md) and [message subscriptions](../index.md#message-subscriptions) instead. Existing webhooks will continue to work until then.

## Endpoint

`POST /v1/accounts/webhook`

## Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `url` | string | Your URL that will receive event payloads. Non-HTTPS URLs are rejected. |

## Registering

POST /v1/accounts/webhook   200 OK

```
$ curl -X POST https://api.replicant.space/v1/accounts/webhook \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"url": "https://your-server.example.com/replicant-hook"}'
```

On success, you'll receive a one-time `webhook_secret`. **This is the only time it's shown.** Lose it and you'll have to re-register the webhook for a new secret.

response response

```
{
  "status": "webhook_registered",
  "verified_at": "2026-05-10T10:14:50+01:00",
  "webhook_secret": "whsec_4f8c2b7a1e9d5f3c6b8a0d2e4f7c9b1a"
}
```

## Viewing your webhook

`GET /v1/accounts/webhook` returns your currently registered webhook URL and the time it was verified.

GET /v1/accounts/webhook   200 OK

```
$ curl https://api.replicant.space/v1/accounts/webhook \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "url": "https://webhooks.yourwebsite.example.com/intercept/",
  "verified_at": "2026-05-27T06:24:19.917217"
}
```

## Verification handshake

Before the webhook is activated, the API will POST a verification challenge to your URL. Your server has to reply `200 OK` with the same `challenge` in the response. If you respond correctly, the webook will be registered and you'll start receiving signed payloads.

webhook incoming · webhook_verification   incoming

```
{
  "type": "webhook_verification",
  "challenge": "chl_8f1a3c5e7d9b2f4a"
}
```

response your response

```
{
  "challenge": "chl_8f1a3c5e7d9b2f4a"
}
```

Because your code won't know the secret at this point, you can't verify the signature on the first request. Skip signature verification for any payload with `type: "webhook_verification"`.

## Signature verification

Every webhook request you receive from the game contains an HMAC-SHA256 signature in the `X-Replicant-Space-Signature` header. To verify, recompute the HMAC over the *raw* request bytes using your `webhook_secret` as the key, then compare against the header value.

| Field | Value |
| --- | --- |
| Algorithm | `HMAC-SHA256`, hex, lowercase, 64 chars |
| Key | The `webhook_secret` from the registration response |
| Message | The raw request body bytes, exactly as received |

Be careful here - if the incoming JSON is being parsed/processed by something beforehand, your validation code might fail. The check needs to be on the raw payload bytes you receive, since whitespace may not be preserved after any JSON processing.

## Webhook payload

Payloads follow this shape. There is a large range of events - see further below for a reference.

webhook incoming · device_arrived   incoming

```
# every event delivery is signed
X-Replicant-Space-Signature: a5f7e3b9c1d8f2a4e6b0c3d5f7a9b1c2e4d6f8a0b2c4d6e8f0a2c4e6d8b0f2a4

{
  "type": "event",
  "event_type": "device_cruise_arrived",
  "device_code": "F54FA154",
  "device_type": "heaven_vessel",
  "replicant_code": "57F0F6C8",
  "payload": {
    "location": "SOL-5-L5",
    "from_location": "PORRAMA-KUIPER"
  },
  "timestamp": "2026-05-10T08:55:27+01:00"
}
```

## Sample code

Here's a quick example with Python/Flask. This handles the verification handshake and verifies the signature on subsequent deliveries. Ensur your code runs behind a public HTTPS endpoint.

response listener.py

```
import hmac, hashlib, os
from flask import Flask, request, jsonify, abort

SECRET = os.environ["REPLICANT_WEBHOOK_SECRET"].encode()
app = Flask(__name__)


def verify(body, signature):
    expected = hmac.new(SECRET, body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


@app.post("/replicant-hook")
def hook():
    body = request.get_data()
    data = request.get_json()
    sig = request.headers.get("X-Replicant-Space-Signature", "")

    # registration handshake. Echo the challenge, skip verification
    if data.get("type") == "webhook_verification":
        return jsonify(challenge=data["challenge"])

    if not verify(body, sig):
        abort(401)

    # do stuff ...
    return "", 200
```

## Webhook type reference

There are four top-level *type* values:

- *webhook_verification* - your webhook verification challenge
- *message* - new account message, achievements, notifications, game hints, story, etc
- *bobnet* - the in-game realtime chat system, if you're in a system with an active FTL relay
- *event* - notification that something happened in the game that you've subscribed to

The *event* type is the one where you can listen and react to anything that happens to your devices. There are a large number of these, here's the full list.

### Travel

travel_departed, travel_arrived, travel_cancelled, cruise_departed, cruise_arrived,
surge_hop_arrived, surge_cruise_departed, device_travel_departed, device_travel_arrived,
device_travel_cancelled, device_cruise_departed, device_cruise_arrived, device_surge_hop_departed,
device_surge_hop_arrived, teleport_started, teleport_completed, teleport_failed

### Trade

trade_created, trade_deleted, trade_completed_buyer, trade_completed_seller

### Printing

print_started, print_complete, autofactory_print_complete

### Mining

mining_started, mining_stopped, mining_retargeted, mining_relocated, prospect_complete, salvage_discovered,
salvage_depleted, salvage_resource_depleted, site_depleted, site_resource_depleted,
site_tracking_lost

### Scanning

scan_started, scan_complete, system_scan_complete, system_scan_bonus_awarded

### Location

location_event_discovered, location_event_completed

### AMI

ami_directive_set, ami_directive_cleared, ami_directive_completed, ami_directive_resumed,
ami_device_adopted, ami_device_released, ami_assemble, ami_launch, ami_withdraw,
ami_gather_evenly_evaluated, ami_deplete_smallest_evaluated, auto_adopt_failed,
oncomplete_failed, resume_failed, hint

### Devices

device_deployed, device_decommissioned, device_recalled, device_recall_stowed, device_owner_changed

### Achievements

experience_gained

### Multiplayer

replicant_entered_system, replicant_left_system

### Uncategorised

diversion_activated, diversion_deactivated, diversion_impacted, diversion_diverted,
diversion_partial, diversion_plate_failed, diversion_wear, megastructure_contributed,
system_object_detected, system_devices_halted, hub_activated, hub_destroyed,
hub_entry_point_set, hub_inactive_warning, relay_deployed, relay_activated,
search_started, search_completed, search_failed, transport_collected, transport_delivered,
body_renamed, matrix_transferred, message_received, replicant_awakening
