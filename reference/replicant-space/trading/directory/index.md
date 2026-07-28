---
title: "Shop directory"
source_url: "https://replicant.space/docs/trading/directory/"
crawled_at: "2026-07-28T00:53:12.573841+00:00"
---

Trading

# Shop directory

The galactic directory of player-run shops. Anyone running a trade controller on a system with an active FTL relay shows up here.

## Overview

In Replicant Space, trading is an essential part of gameplay. Why wait days to print a shiny new racing vessel when someone's selling them nearby. If players want to participate in megastructures they will need to work together to share devices with each other, so blueprints can be distributed.

A trade can be any combination of resources and devices, for both the criteria and reward. When a shopkeeper creates a trade, they deposit the rewards to the AMI Trade Controller which holds the items in escrow until a valid trade is carried out.

## Listing traders

Use your replicant interface to access the list of shops on the network. The response is a list of trade controllers visible from the galactic relay network, with the system they're in and a short blurb.

GET /v1/replicants/{code}/traders   200 OK

```
$ curl https://api.replicant.space/v1/replicants/C2AF4A82/traders \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "traders": [
    {
      "controller_code": "C25C44CE",
      "description": "Bring some compute cores to the 'works if you want my new bots.",
      "is_local": false,
      "location": null,
      "owner_name": "Bill",
      "owner_replicant_code": "67D2FE23",
      "shop_name": "The Skunkworks",
      "star": null,
      "total_stock": 6,
      "trade_count": 2
    },
    {
      "controller_code": "F61FA449",
      "description": "We need volatiles, happy to trade.",
      "is_local": false,
      "location": "SOL-3-1",
      "owner_name": "Riker",
      "owner_replicant_code": "05AB5A1E",
      "shop_name": "Riker's Lunar Supplies",
      "star": "SOL",
      "total_stock": 10,
      "trade_count": 1
    }
  ]
}
```

Some shop locations might be hidden, like Bill's Skunkworks above.

## Inspecting a shop

The directory only gives you the location and description. You'll need to travel to the shop system to query the full stock details for the first time. If you have an [FTL beacon](../../ftl-beacons/index.md) or an [FTL relay](../../ftl-relays/index.md) in the system, you can check stock levels remotely.

GET /v1/devices/{controller_code}/trades   200 OK

```
$ curl https://api.replicant.space/v1/devices/TC4488AA/trades \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "trades": [
    {
      "name": "Cheap Mining Drones For Sale",
      "trade_code": "TRD-E400B1",
      "current_stock": 10,
      "initial_stock": 10,
      "criteria": {
        "resources": {
          "structural": 10,
          "volatiles": 10
        }
      },
      "rewards": {
        "devices": {
          "mining_drone": 1
        }
      },
      "created_at": "2026-05-10T14:04:32+01:00"
    }
  ]
}
```

Each entry lists the cost (`criteria`) and the reward, along with the current stock level and the original allocation. Use the `trade_code` when executing the trade.

## Executing a trade

Your replicant must be present at the shop's location, with the criteria resources and/or devices on hand. Trigger the swap by POSTing to the trade endpoint - no body required.

POST /v1/devices/{controller_code}/trades/{trade_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/93C4B199/trades/TRD-1A893F \
    -H "Authorization: Bearer $API_KEY"
```

The trade controller takes your criteria payment and hands over the rewards from its escrow inventory at the location. Nothing is spawned from nothing - the shop owner deposited the rewards at the location ahead of time, and the controller reassigns ownership atomically to keep the books straight. Once the trade completes, the shop owner is notified to come and collect their payment, and to keep the shelves stocked.
