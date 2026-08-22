---
title: "Quickstart"
source_url: "https://replicant.space/docs/quickstart/"
crawled_at: "2026-08-07T00:51:31.852715+00:00"
---

Getting Started

# Quickstart

Your first ten minutes. Wake up, look around, visit the belt, deploy some mining drones. By the end of this page you'll understand the bootstrap process.

## Before you start

1. [Register an account](../authentication/index.md) and grab your API key.
2. Set `$API_KEY` in your shell.
3. Make a note of your first replicant code, it looks like `57F0F6C8`
4. Have `jq` installed if you want pretty JSON output. Optional but nice.

Prefer clicking over typing? Grab the [Postman collection](../postman/index.md) and you can run every step below from there instead of `curl`.

Note: the API responses shown in these docs are often simplified versions of the real ones you'll see when playing the game. I've removed parts that aren't useful to the learning process. You will see more output when playing normally.

*Want to get a feel for the game before signing up? Try the [interactive tutorial](https://replicant.space/tutorial/) - it walks you through the core gameplay loop in the browser, no account required.*

## Step 1 - Hello, replicant

Confirm you can authenticate and that your replicant is awake.

GET /v1/accounts/me   200 OK

```
# 1. confirm the replicant you woke up as
$ curl https://api.replicant.space/v1/accounts/me \
    -H "Authorization: Bearer $API_KEY"
```

## Step 2 - Scan the system

You start in an unexplored system. A full system scan will reveal technical details of your star along with a list of planets and asteroid belts.

Place your replicant code from registration (or find it in the /accounts/me output) and substitute it in the following request:

POST /v1/replicants/{code}/scan   200 OK

```
# 2. scan your starting system to discover what's around you
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/scan \
    -H "Authorization: Bearer $API_KEY" \
```

System scans return instantly. Your vessel has been collecting data during your journey here.

You will start at the outer edge - either the Kuiper belt or Oort cloud - depending on the age of the system.

## Step 3 - Travel to the belt

Pick the asteroid belt out of your scan results and send your replicant vessel there.

The location code looks like `SOL-BELT-1`.

POST /v1/replicants/{code}/travel   200 OK

```
# 3. travel to the asteroid belt found in your scan
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

GET /v1/replicants/{code}   200 OK

```
# check progress at any time
$ curl https://api.replicant.space/v1/replicants/57F0F6C8 \
    -H "Authorization: Bearer $API_KEY"
```

## Step 4 - Deploy a mining drone

When you checked your replicant status, you'll have noticed four devices currently stowed - three of them are mining drones. Make a note of their device codes and deploy one with a `deploy` command.

POST /v1/devices/{code}   200 OK

```
# 4. deploy one of your stowed mining drones
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "deploy"}'
```

## Step 5 - Mine for resources

Tell each deployed drone to start mining. [Pick a resource](../concepts/resources/index.md), all belts have all six of the resource types. Repeat the deploy/start_mining commands for the other two drones.

POST /v1/devices/{code}   200 OK

```
# 5. start mining carbon (repeat for each drone)
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "start_mining", "resource_type": "carbon"}'
```

Now wait for a little bit, and the mining drones will start releasing resources to the location. More drones, faster mining.

## Step 6 - Check your inventory

You can see the resources held at any location where you have devices. Hit the location endpoint for the belt your drones are working.

GET /v1/locations/{code}   200 OK

```
# 6. check what your drones have pulled out of the belt
$ curl https://api.replicant.space/v1/locations/SOL-BELT-1 \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "location": "SOL-BELT-1",
  "location_type": "belt",
  "inventory": [
    { "resource_type": "carbon",     "quantity": 25 },
    { "resource_type": "silicates",  "quantity": 28 },
    { "resource_type": "structural", "quantity": 123 }
  ]
}
```

Your resources are owned by you, other players can't see them.

## Step 7 - Switch resource targets

Mining drones can only focus on one resource at a time. Send a `retarget` command to change what the drone is working on. You'll want to do this a few times to get a good quantity of each resource.

POST /v1/devices/{code}   200 OK

```
# 7. switch a drone's focus to silicates
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "retarget", "resource_type": "silicates"}'
```

The drone keeps mining without redeploying. Repeat for each drone you want to retarget.

## Step 8 - Check your blueprints

See what you already know how to print. New blueprints unlock as you explore and scan.

GET /v1/blueprints   200 OK

```
# 8. see which blueprints you can print
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

## Step 9 - Print something

Once you've mined enough resources, print a new device. More mining drones means faster resource collection. A survey drone would enable you to start scanning planets and moons, or search a new mining site at the belt. Or you can pick something else from your blueprint list. Your bootstrap strategy is your own!

POST /v1/replicants/{code}/print   200 OK

```
# 9. print another mining drone
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/print \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"device_type": "mining_drone"}'
```

Printing takes time. Check your replicant status to watch the progress of your internal 3D printer. Bootstrapping from nothing will take patience, but you'll have more than enough to focus on once you are up and running.

## What's next

1. Connect to the [event stream](../api/events/stream/index.md) so you don't have to poll for results.
2. Print more mining drones and exhaust that belt site.
3. Print survey drones and unlock more mining sites.
4. Scan the planets and moons to find valuable salvage.
5. Print transport drones to start stockpiling resources.
6. Travel to another system.
7. Visit Earth!

When you've got more drones than you can micromanage, print and deploy AMI controllers and configure automation directives.

- [Event stream](../api/events/stream/index.md) - be told when things happen in real time.
- [Mining drones](../drones/mining/index.md) - turning rocks into useful stuff.
- [AMI Survey Controller](../ami/survey-controller/index.md) - automate a whole system scan.
