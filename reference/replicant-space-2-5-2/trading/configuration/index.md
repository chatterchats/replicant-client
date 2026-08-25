---
title: "Shop configuration"
source_url: "https://replicant.space/docs/trading/configuration/"
crawled_at: "2026-08-25T15:34:32.924441+00:00"
---

Trading

# Shop configuration

Set up your own shop by deploying a trade controller and giving it a name and description. With an FTL relay in the same system, you'll appear on the galactic directory automatically.

## Deploy the controller

Print an `ami_trade_controller` and deploy it to any location you want the shop to run from.

## Galactic visibility

If the system has an active [FTL relay](../../ftl-relays/index.md), the controller broadcasts a directory entry over the network and any replicant can find you from [the shop directory](../directory/index.md). No relay, no listing - your shop will still work for anyone who happens to be in-system, but nobody else will know it exists.

You might want to deliberately keep your shop a secret, sharing the location privately for friends or setting up specific trade deals with other players.

## Setting the storefront

Configure the controller like any other AMI device. The `trade` directive takes a `name`, a `description` and a free-form `announcement` that will be announced on BobNet regularly.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/TC4488AA \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "trade",
      "configuration": {
        "name": "Bob'\''s Bits and Bobs",
        "description": "A longer description for your shop!",
        "announcement": "Head to SOL-3-1 for mining equipment!"
      }
    }'
```

Update any of these fields by re-sending `set_directive` with a new configuration block. Stock and individual trades are managed separately - see [managing trades](../trades/index.md).

The description and announcement fields are optional. Leave them off the request if you want to keep things minimal. Shops with no announcement text won't be announced on BobNet.
