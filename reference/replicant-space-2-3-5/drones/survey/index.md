---
title: "Survey drone"
source_url: "https://replicant.space/docs/drones/survey/"
crawled_at: "2026-08-03T00:42:37.983612+00:00"
---

Drones

# Survey drone

Your replicant vessel has basic scientific observation gear, but for any kind of detailed survey work you'll want to print a survey drone. They're highly specialised, packed with sensor equipment and precision manoeuvring controls for more thorough orbital analysis.

## What they're for

Survey drones have two basic uses. The first is scanning planets, moons and belts. The second is searching belts for new resource sites you can mine.

## Scanning

A scan looks at the location the drone is currently at, so send it where you need it before issuing the command. On a planet or moon, scans look for signs of life and intercept any comms broadcasts. On a belt, the scan returns accurate counts for the remaining resources at each known site.

POST /v1/devices/{code}   202 accepted

```
# scan the planet or moon the drone is currently at
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "scan"}'
```

Scans take time - moons are faster than planets - and results are delivered to you on completion. Depending on your account settings the payload is posted to your [webhook](../../api/accounts/webhook/index.md) or sent by email.

Scan results also show up in your [event log](../../api/replicants/events/index.md), for you to review at your leisure.

## Searching

Searches are belt-only. The drone scours nearby rocks until it locates a cluster with enough resources to be worth mining, then keeps a continuous fix on it - this is how a new resource site appears at the location.

POST /v1/devices/{code}   202 accepted

```
# search the belt the drone is currently at for resource sites
$ curl -X POST https://api.replicant.space/v1/devices/A1B2C3D4 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "search"}'
```

Tracking a site is an ongoing operation - the drone stays on it. Moving or deactivating the survey drone will result in losing the site. To open multiple sites in parallel, deploy more survey drones.

Be aware there are diminishing returns to opening lots of sites, but they do replenish over time as the belt moves. You'll need to find a balance.

## Running a fleet

For a full system sweep across planets, moons and belts, hand your survey drones to a [Survey Controller](../../ami/survey-controller/index.md) to dispatch and coordinate them in parallel.
