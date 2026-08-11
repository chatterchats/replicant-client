---
title: "System Hubs"
source_url: "https://replicant.space/docs/system-hubs/"
crawled_at: "2026-08-11T15:11:30.176442+00:00"
---

Infrastructure

# System Hubs

The official claim on a star system. Place a hub somewhere stable and all mining and events are now locked to your account. You can rename the system and planets. Get a speed boost on incoming travel.

## What a hub gives you

- Mining lock - other players can't mine your asteroid belts or salvage.
- Naming rights - you can rename the star, the planets or the moons.
- Entry point - change the location for incoming system surge travel.
- Welcome messages - send a message to visiting replicants when they arrive.
- Surge assistance - braking lasers and navigational assistance reduces the deceleration phase for your incoming vessel.
- Built-in FTL relay - double the range of a traditional FTL relay device.
- Matrix housing - the hub can host a replicant matrix if you want a permanent presence there.

## What a hub costs

- Gravitational stability - the L4 or L5 Lagrange points of a planet is ideal.
- Upkeep requirements - regular maintenance with carbon and structural resources.
- Printing time - additional hubs take longer each time to print.

## How to activate

Move the hub to an L4 or L5 Lagrange point of any planet in the system, then issue the `activate` command.

POST /v1/devices/{hub_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SH8C2A1F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "travel",
      "destination": "PHARM-3-L5"
    }'
```

POST /v1/devices/{hub_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SH8C2A1F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "activate"}'
```

You now have 7 days before the protection shield wears off and the hub starts to degrade. See the Maintenance section below for more details.

## Mining lock

Having a System Hub in operation will lock any mining activities to other players. This also prevents other players from carrying out location events with any species you've encountered in that system. You effectively have exclusive rights to anything going on there. It's your playground!

## Naming things

Having a system hub means you can personalise the names of the star, planets and moons in the system. Names are unique in the shared galaxy catalogue amongst all players.

## Entry points

The default entry point in a system for incoming surge travel is automatically designated on the initial system scan. This is usually the closest planet to the belt, if there is one. If there's no belt, it's usually the outermost planet.

Once you've deployed a system hub somewhere, you can specify that location as your new entry point. Perhaps you'd rather be on the other side of the belt for efficiency, or you're too protective of your innermost system and want to impose a slow cruise on any visiting traffic.

## Custom welcome messages

An active hub can greet visitors. Issue the `set_welcome_message` command with a `message` of up to 500 characters, and any replicant from another account that lands in your system will receive it. The hub must be active for the message to be set or delivered.

POST /v1/devices/{hub_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SH8C2A1F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_welcome_message",
      "message": "Welcome to PHARM. Trading outposts on the third planet are fully stocked!."
    }'
```

response response

```
{
  "status": "set",
  "welcome_message": "Welcome to PHARM. Trading outposts on the third planet are fully stocked!"
}
```

A hub won't send the same message to the same replicant more than once per hour, to reduce spamming regular visitors.

## Surge assistance

One of the more practical uses of a System Hub is the navigational assistance it provides to any of your surge-capable devices travelling to the system. The use of braking lasers and FTL navigational support means you can defer your deceleration phase until much later, reducing travel time by up to 25%.

## FTL Connections

Typically, an FTL relay device will allow remote control of the devices in that system, from up to 7.5 light years away. The System Hub connects an FTL mesh network across the entire galaxy, allowing remote control from up to 15 light years away.

## Maintenance

Hubs are huge and constantly peppered with space debris. Without dedicated maintenance, integrity drops over weeks. If a hub's integrity hits zero, it fails - the system unlocks for anyone and incoming Surge traffic loses the assist. Look after your hubs folks.

When the hub is first printed, the upkeep requirements are calculated. These can vary depending on the system the hub is operating within. This will be a combination of structural and carbon resources that will be needed to keep the hub operational.

Newly printed system hubs will run with a protective shield that lasts for a week. After that initial 7 days, the hub will start to degrade by 10% per day. Therefore, a hub that is not maintained once in the 17 days since printing will degrade to 0%.

Once you know the maintenance resource costs, just ensure to keep a stockpile at the location and have a maintenance drone patrolling in the system. The drone will keep the hub active.

## Relocation

System hubs have the `modular` feature. If you decide to move a hub to a different system, issue the `compact` command to collapse it into a transportable state, attach it to a surge carrier, and `unfurl` it at the new location. The hub is offline for the duration of the compacting/unfurling process. Your system will lose its mining lock, surge assistance and FTL relay until the hub is unfurled and reactivated.

## Printing time

All of the system hubs that operate under your account are connected via an FTL mesh network. Each one you print takes longer to assemble, due to the increased complexities of fine tuning the connection parameters of the internal mesh networking hardware. Your blueprint list will show the time requirements for your next one.
