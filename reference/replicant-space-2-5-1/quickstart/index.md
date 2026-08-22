---
title: "Quickstart"
source_url: "https://replicant.space/docs/quickstart/"
crawled_at: "2026-08-22T22:43:51.889972+00:00"
---

Getting Started

# Quickstart

Your first ten minutes. Wake up, look around, visit the belt, deploy some mining drones. By the end of this page you'll have completed the Bootstrap tutorial.

## Before you start

1. [Register an account](../authentication/index.md) and grab your API key.
2. Set `$API_KEY` in your shell.
3. Make a note of your first replicant code, it looks like `57F0F6C8`
4. Have `jq` installed if you want pretty JSON output. Optional but nice.

Prefer clicking over typing? Grab the [Postman collection](../postman/index.md) and you can run every step below from there instead of `curl`.

Note: the API responses shown in these docs are often simplified versions of the real ones you'll see when playing the game. I've removed parts that aren't useful to the learning process. You will see more output when playing normally.

*Want to get a feel for the game before signing up? Try the [interactive tutorial](https://replicant.space/tutorial/) - it walks you through the core gameplay loop in the browser, no account required.*

**In-game tutorials available**

The game includes a guided tutorial sequence that walks you from your very first scan all the way to being released into galactic society. Seven tutorials, each building on the last. Call `GET /v1/tutorials` at any time to see your next objective, or read the [Tutorials](../tutorials/index.md) page for the full breakdown.

## Step 0 - Open the event stream

Before you do anything else, open [stream.replicant.space](https://stream.replicant.space/) in a browser tab and paste in your API key. This connects you to the real-time event stream - every action you take in the game fires events here, so you can see what's happening without polling the API.

Keep this tab open while you learn. It's the fastest way to know when travel completes, when drones finish mining, and when something interesting happens in your system.

It's an SSE endpoint at */v1/events/stream* so feel free to write your own consumer client, and maybe start automating things

## Step 1 - Hello, replicant

Confirm you can authenticate and that your replicant is awake.

GET /v1/accounts/me   200 OK

```
# confirm the replicant you woke up as
$ curl https://api.replicant.space/v1/accounts/me \
    -H "Authorization: Bearer $API_KEY"
```

## Step 2 - Check your messages

You'll have a welcome message waiting. Call the messages endpoint to read it - this is where the game sends you notifications about completed tutorials, incoming trades, and system alerts.

GET /v1/messages   200 OK

```
# check your inbox
$ curl https://api.replicant.space/v1/messages \
    -H "Authorization: Bearer $API_KEY"
```

## Step 3 - Check your events

The event log records everything that happens to your account. If you connected to the stream in Step 0 you've already seen events arrive in real time - this endpoint gives you the same data as a paginated list.

GET /v1/events   200 OK

```
# check your event log
$ curl https://api.replicant.space/v1/events \
    -H "Authorization: Bearer $API_KEY"
```

## Step 4 - Scan the system

You start in an unexplored system. A full system scan will reveal technical details of your star along with a list of planets and asteroid belts.

Place your replicant code from registration (or find it in the /accounts/me output) and substitute it in the following request:

POST /v1/replicants/{code}/scan   200 OK

```
# scan your starting system
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/scan \
    -H "Authorization: Bearer $API_KEY" \
```

System scans return instantly. Your vessel has been collecting data during your journey here.

You will start at the outer edge - either the Kuiper belt or Oort cloud - depending on the age of the system.

## Step 5 - Check your vessel

Before heading anywhere, check what you're carrying. This shows your replicant's current location and a list of stowed devices - you'll find your replicant matrix and some mining drones packed into your vessel. Make a note of the device codes, you'll need them next.

GET /v1/replicants/{code}   200 OK

```
# check your replicant and stowed devices
$ curl https://api.replicant.space/v1/replicants/57F0F6C8 \
    -H "Authorization: Bearer $API_KEY"
```

## Step 6 - Travel to the belt

Pick the asteroid belt out of your scan results and send your replicant vessel there.

The location code looks like `SOL-BELT-1`.

POST /v1/replicants/{code}/travel   200 OK

```
# travel to the asteroid belt
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/travel \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"destination": "SOL-BELT-1"}'
```

The response tells you how long the trip will take. See the [travel docs](../concepts/drives/index.md) for more details.

response 200 response

```
{
  "origin": "SOL-OORT",
  "destination": "SOL-BELT-1",
  "departed_at": "2026-05-17T13:55:41+01:00",
  "arrives_at": "2026-05-17T13:56:22+01:00",
  "total_time_seconds": 41.1,
  "route": [
    {
      "leg": 1,
      "from": "SOL-OORT",
      "to": "SOL-4-L4",
      "type": "surge_hop",
      "time_seconds": 30
    },
    {
      "leg": 2,
      "from": "SOL-4-L4",
      "to": "SOL-BELT-1",
      "type": "cruise",
      "time_seconds": 11.1
    }
  ],
  "status": "travel_initiated"
}
```

Check in on your replicant any time to see how far along it is.

## Step 7 - Deploy a mining drone

When you checked your vessel in Step 5, you'll have noticed three mining drones stowed inside. Deploy one with a `deploy` command using its device code.

POST /v1/devices/{code}   200 OK

```
# deploy one of your stowed mining drones
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "deploy"}'
```

## Step 8 - Mine for resources

Tell each deployed drone to start mining. [Pick a resource](../concepts/resources/index.md), all belts have all six of the resource types. Repeat the deploy/start_mining commands for the other two drones.

POST /v1/devices/{code}   200 OK

```
# start mining carbon (repeat for each drone)
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "start_mining", "resource_type": "carbon"}'
```

Now wait for a little bit, and the mining drones will start releasing resources to the location. More drones, faster mining.

## Step 9 - Check your inventory

You can see the resources your drones have mined. Hit the inventory endpoint for your replicant to see everything stockpiled in your current system.

GET /v1/replicants/{code}/inventory   200 OK

```
# check what your drones have pulled out of the belt
$ curl https://api.replicant.space/v1/replicants/57F0F6C8/inventory \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "star": "SOL",
  "locations": [
    {
      "location": "SOL-BELT-1",
      "items": {
        "carbon": 25,
        "silicates": 28,
        "structural": 123
      }
    }
  ]
}
```

Your resources are owned by you, other players can't see them.

## Step 10 - Switch resource targets

Mining drones can only focus on one resource at a time. Send a `retarget` command to change what the drone is working on. You'll want to do this a few times to get a good quantity of each resource.

POST /v1/devices/{code}   200 OK

```
# switch a drone's focus to silicates
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "retarget", "resource_type": "silicates"}'
```

## Step 11 - Check your blueprints

See what you already know how to print. New blueprints unlock as you explore the game.

GET /v1/blueprints   200 OK

```
# see which blueprints you can print
$ curl https://api.replicant.space/v1/blueprints \
    -H "Authorization: Bearer $API_KEY"
```

Each entry tells you the device type, what features it has, how long it takes to print, and which resources it costs.

response 200 response

```
{
  "blueprints": [
    {
      "device_type": "mining_drone",
      "features": [
        "cruise",
        "mine",
        "stow"
      ],
      "print_time": 180,
      "resources": {
        "carbon": 25,
        "conductive": 50,
        "silicates": 25,
        "structural": 100
      }
    },
    ...
  ]
}
```

## Step 12 - Print a mining drone

Once you've mined enough resources, print a new mining drone. More drones means faster resource collection - and printing your first one completes the Bootstrap tutorial.

POST /v1/replicants/{code}/print   200 OK

```
# print another mining drone
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/print \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"device_type": "mining_drone"}'
```

Printing takes time. Check your replicant status to watch the progress of your internal 3D printer. Bootstrapping from nothing will take patience, but you'll have more than enough to focus on once you are up and running.

## What's next

You've got the basics down - you can scan, travel, deploy, and mine. From here, follow the [in-game tutorials](../tutorials/index.md). They'll walk you through belt exploration, surveying planets for salvage, moving resources around, helping local civilisations, and eventually using your FTL slingshot to return to SOL and join the wider galaxy.

Call `GET /v1/tutorials` to see your current progress and what to do next. Each tutorial builds on the last, and the final one releases your account into the full game.
