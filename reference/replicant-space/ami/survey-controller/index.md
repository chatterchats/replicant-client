---
title: "AMI Survey Controller"
source_url: "https://replicant.space/docs/ami/survey-controller/"
crawled_at: "2026-08-07T00:51:29.730572+00:00"
---

AMI Controllers

# AMI Survey Controller

Survey controllers automate the process of scanning multiple locations for you. Hand a fleet of survey drones to one and it can sweep a whole system or hunt the belt continuously for new sites.

## What it does

A survey controller has two main uses:

- **System scans** - automate the sequence of scans across the planets and moons of a system. Configure once, launch when you arrive, and watch the results come in.
- **Belt search** - send a fleet of survey drones out into the belt to find more resource sites to mine.

## Directives

- `survey_system` - sweep every body in the current system.
- `belt_search` - hunt the belt for additional resource sites to mine.

## Surveying a system

Configure ahead of time to visit all planets and moons. When you land in a new system, launch the controller and it dispatches every assigned drone in parallel. When the directive completes, the controller brings the fleet home.

POST /v1/devices/{code}   200 OK

```
# sweep every body in the current system
$ curl -X POST https://api.replicant.space/v1/devices/SC74F210 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "survey_system",
      "configuration": {
        "planets": "all",
        "moons": "none",
        "recall": true
      }
    }'
```

## Searching the belt

You can always find new sites to mine at the belt, although it takes longer each time, and have diminishing returns. Thankfully they do regenerate over time, so finding a sustainable routine is key. The `belt_search` directive sends the survey drones sweeping through the belt looking for new sites to track. When one depletes, the survey drone will be commanded to find another.

POST /v1/devices/{code}   200 OK

```
# hunt the belt for new resource sites
$ curl -X POST https://api.replicant.space/v1/devices/SC74F210 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "belt_search"
    }'
```
