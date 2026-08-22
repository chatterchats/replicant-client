---
title: "Civilisations"
source_url: "https://replicant.space/docs/concepts/civilisations/"
crawled_at: "2026-08-22T22:43:51.355595+00:00"
---

Core Concepts

# Civilisations

Scan a planet or moon in the right slice of a system and you might find life. From microbes drifting in a warm ocean to spacefaring neighbours with their own agenda, the galaxy has company.

## Finding life

Scanning a planet or moon with your [survey drone](../../drones/survey/index.md) reveals lots of information. If there's life to be found, this is the way to do it. Survey drones are programmed to look specifically for signs of life, and will send you a message if something shows up. Life is most commonly found in the habitable zone of a system, though the galaxy isn't that tidy and exceptions are common. Each species you encounter has a life stage - prebiotic, microbial, complex, intelligent or spacefaring.

Once a species reaches the intelligent stage, someone will spot your drone flying around. They quickly establish that you are not a threat and may request assistance. This creates a Location Event at the planet or moon being scanned.

## Events and reputation

Completing an event improves your reputation with the species involved. As trust grows, the tasks will get more interesting - and more rewarding. There are four tiers to the event progression. Tier 1 is mostly resource-based requests with resource-based rewards, but they might be a handy way to fill the gaps in your mining situation. Tier 2 starts to include devices in the requirements (eg: supply some atmospheric processors). Tier 3 is much harder, but can result in bg XP rewards and achievements. Tier 4 represents the pinnacle of cooperation and will unlock very advanced blueprints. Some events can be resolved in multiple different ways - a species that needs help with a famine, for example, might be satisfied with hydroponic bays, nutrient synthesisers, or an orbital farm.

Reputation isn't just about you. Helping a species raises their life stage over time, which leads to richer events for every replicant that meets them.

## Blueprints as reward

Triggering a new event will unlock the blueprints required to resolve it. The people will help you understand how to build the things they need. This is the primary way to grow your blueprint list.

If you plan to contribute to any of the ongoing [megastructures](../../api/locations/megastructures/index.md), growing this list is critical.

Knowledge doesn't have to stay with you either. Set up a [shop](../../trading/directory/index.md) to sell your newly discovered devices to other replicants - anyone who [decommissions](../../api/devices/decommission/index.md) a device at one of their own autofactories learns the blueprint to be able to make their own.

## Keeping in touch

After completing your first event at a planet or moon, deploy an [FTL beacon](../../ftl-beacons/index.md) there. The beacon delivers in-game messages whenever that species asks for more help - usually once a day. Without one, you'll only hear from them when you return and scan the location again.

## Listing your known events

Use the /accounts/events endpoint to see all your current ongoing events.

### Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `status` | string · optional | Filter by event status: `active` or `completed`. |
| `cursor` | integer · optional | Position in the list to start from. Use `next_cursor` from the previous response. |
| `limit` | integer · optional | Number of events to return, default 20. |

GET /v1/accounts/events   200 OK

```
$ curl https://api.replicant.space/v1/accounts/events?status=active&limit=5 \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "events": [
    {
      "location": "QIGONOG-3",
      "designation": "QIGONOG-3-EVT-001",
      "broadcast_message": "...our communication networks are degrading...",
      "title": "Electronics Shortage"
      "description": "A civilisation's conductive material reserves are exhausted...",
      "category": "resource_trade",
      "status": "active",
      "criteria": {
        "devices": [],
        "resources": {
          "conductive": 250,
          "silicates": 100
        }
      },
      "rewards": {
        "civilisation_points": 1,
        "completion_achievement": "electronics_shortage_completed",
        "resources": {
          "rares": 350
        },
        "xp": 500
      },
      "discovered_at": "2026-05-23T21:17:25+01:00",
      "tier": 1
    }
  ]
}
```

## Completing an event

[Transport](../../drones/transport/index.md) all the requirements to the location, then POST to the event with an empty body. The server checks the location's inventory against the event's criteria and resolves it if everything's there.

POST /v1/locations/QIGONOG-3/events/QIGONOG-3-EVT-001   200 OK

```
$ curl -X POST https://api.replicant.space/v1/locations/QIGONOG-3/events/QIGONOG-3-EVT-001 \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "designation": "QIGONOG-3-EVT-001",
  "title": "Distress Signal",
  "status": "event_resolved",
  "rewards": {
    "xp": 500,
    "resources": {
      "rares": 350
    },
    "civilisation_points": 1
  }
}
```

## The species we've encountered

A list of all the species that players have encountered so far.

GET /v1/species   200 OK

```
$ curl https://api.replicant.space/v1/species \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "species": [
    {
      "description": "Nomadic arthropods migrating across vast deserts.",
      "government": "Traveller Council",
      "greeting": "Short-range directional signal. Source is in motion.",
      "homeworld_type": "desert_world",
      "name": "Branthar",
      "species_key": "branthar",
      "tech_affinity": "navigation",
      "trait": "nomadic"
    },
    ...
  ]
}
```
