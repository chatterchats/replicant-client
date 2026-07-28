---
title: "Managing trades"
source_url: "https://replicant.space/docs/trading/trades/"
crawled_at: "2026-07-28T00:53:12.590715+00:00"
---

Trading

# Managing trades

Each trade is a swap: a set of resources or devices the buyer must hand over, in exchange for a set of resources or devices you'll deliver. Stock the controller, define the trades, and walk away.

## Anatomy of a trade

Every trade has four fields:

- **name** - shown to buyers on the controller's trade list.
- **stock** - how many times this trade can be executed before it's exhausted. Once stock hits zero, the trade disappears from the listing until you top it up.
- **criteria** - what the buyer pays. An amount of `resources` and/or specific `devices` that must be present at the shop's location.
- **rewards** - what the buyer receives. Same shape as criteria.

## Adding a trade

Trades are added directly to the controller with a separate endpoint. The example below is for selling five Point Defence Arrays in exchange for 100 structural and 100 volatiles, three times over. Bit of a bargain to be honest.

POST /v1/devices/{code}/trades   201 created

```
$ curl -X POST https://api.replicant.space/v1/devices/TC4488AA/trades \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "name": "Set of Point Defence Arrays (x5)",
      "stock": 3,
      "criteria": {
        "resources": {
          "structural": 100,
          "volatiles": 100
        },
        "devices": {}
      },
      "rewards": {
        "resources": {},
        "devices": {
          "point_defence_array": 5
        }
      }
    }'
```

The selling player must have the reward stock available at its location at the point of adding the trade. You can't sell what you don't have.

The trade controller will take the items (in the example above, that would be 15 point_defence_array devices) into escrow and hold them for when trades are executed by players. At the point of sale, the device ownerships will be changed to the buyer.

## Removing a trade

Delete a trade by its ID. The buyer-facing listing updates immediately. The resources and/or devices that represented the trade rewards are removed from escrow and ownership is updated back to the seller.

DELETE /v1/devices/{code}/trades/{trade_id}   204 No Content

```
$ curl -X DELETE https://api.replicant.space/v1/devices/TC4488AA/trades/TRD-E400B1 \
    -H "Authorization: Bearer $API_KEY"
```
