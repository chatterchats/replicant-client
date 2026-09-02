---
title: "FTL Beacons"
source_url: "https://replicant.space/docs/ftl-beacons/"
crawled_at: "2026-09-02T20:03:42.746605+00:00"
---

Infrastructure

# FTL Beacons

A cheap floating bundle of FTL comms equipment. Drop them everywhere - they pay for themselves the first time you find out something happened in a system you'd forgotten about.

## How they differ from relays

Beacons can't support the low-latency requirements of realtime comms, so they're no substitute for an [FTL relay](../ftl-relays/index.md) if you need to send commands to your devices remotely. In exchange they're cheap to print and have no placement requirement - drop one anywhere in a system and it'll start broadcasting.

## What they're good for

Three main uses:

- **Remote device monitoring** - a beacon anywhere in a system lets you keep track of your devices in that system from elsewhere. You won't be able to command them without a relay, but you'll know what they're up to.
- **Future event hooks** - if you've completed an event at a planet or moon, deploying a beacon at that location means future requests from the locals reach you. You'll receive a message in-game (and an email, if you've configured one).
- **Traffic logging** - beacons track the comings and goings of every device in the system, including other players. Useful for logging traffic, or for tracking down a particularly elusive NPC.

## Deploying

No activation step, no gravitational requirements. Print the device and drop it wherever you want it. When you're out exploring, it's worth dropping one in every system you visit.

## System audit

Fetch the traffic log from a beacon with `GET /v1/devices/{code}/audit`. Use `latest=true` to get the most recent entries first, and `limit` to control the page size.

| Name | Type | Description |
| --- | --- | --- |
| `cursor` | integer · optional | Position in the list to start from, default null. |
| `limit` | integer · optional | Number of entries to show, default 20. |
| `latest` | boolean · optional | Show latest entries. Default `false`. Incompatible with `cursor`. |
| `device_type` | string · optional | Filter results by type of device. |
| `replicant_code` | string · optional | Filter results by specific replicant code. |

GET /v1/devices/{beacon_code}/audit   200 OK

```
$ curl "https://api.replicant.space/v1/devices/FB7710AC/audit?latest=true&limit=10" \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "audit": [
    {
      "id": 78,
      "device_code": "814A64A0",
      "device_type": "heaven_vessel",
      "replicant_code": "4BBA7CBE",
      "travel_type": "departure",
      "location": "ALNITAKON-7-L4",
      "logged_at": "2026-05-10T11:43:16+01:00",
      "vector": "-0.37,0.14,-0.92"
    },
    {
      "id": 77,
      "device_code": "814A64A0",
      "device_type": "heaven_vessel",
      "replicant_code": "4BBA7CBE",
      "travel_type": "arrival",
      "location": "ALNITAKON-7-L4",
      "logged_at": "2026-05-10T11:43:16+01:00",
      "vector": null
    }
  ]
}
```

The above example shows two audit entries. The older (bottom one) shows a heaven vessel arriving at `ALNITAKON-7-L4` from an in-system cruise. The top one shows a direction *vector* which indicates off-system travel - departing for the stars.

When it comes to interstellar travel, beacons don't know the remote location that serves as the origin or destination. The beacon will however track the direction vector. A curious player could use this and the star catalogue to figure out where that player went.

  Direction vector visualisation An isometric 3D plot with the example vector -0.37, 0.14, -0.92 drawn from the origin, with a dashed projection onto the xy plane.          +x +y -z -x -y +z          ALNITAKON-7-L4 vector: -0.37, 0.14, -0.92

The recorded departure vector from `ALNITAKON-7-L4`. The dashed line shows the direction of travel; most of the movement is along -z.



This specific movement was:

| Leg | System | Coordinates (ly) |
| --- | --- | --- |
| From | `ALNITAKON` | 55.84, -27.85, 25.06 |
| To | `ALPHACCA` | 55.22, -27.62, 23.51 |

The calculations are left as an exercise for the reader.
