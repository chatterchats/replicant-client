---
title: "FTL Relays"
source_url: "https://replicant.space/docs/ftl-relays/"
crawled_at: "2026-08-03T00:42:38.075115+00:00"
---

Infrastructure

# FTL Relays

High-bandwidth FTL communication relays. Once you've built one, you can be present in a system from afar - keep your print queues moving, micromanage your drones, teleport back and forth.

## Why you need them

When you travel off-system, you lose connection with any devices that you've left behind. An FTL beacon will keep you updated with what's happening, but without an active FTL relay, you won't be able to send commands and manage things remotely.

## The relay retwork

A single relay has a finite 7.5 light year range. This was a deliberate design decision by earlier replicants to match the average distance between stars in our galaxy region. Relays will automatically form a chain when they detect each other. As long as you have an active relay network, you can remote control your devices from the other end of the galaxy.

Ask any relay what neighbours it has linked up with and you'll get back the current connections, each relay's range, and whether it's actively passing traffic.

GET /v1/devices/{relay_code}/network   200 OK

```
$ curl https://api.replicant.space/v1/devices/SR9023C1/network \
    -H "Authorization: Bearer $API_KEY"
```

response 200 response

```
{
  "connections": [
    {
      "device_code": "75B06535",
      "distance_ly": 2.4,
      "star": "MUROPE"
    },
    {
      "device_code": "80A097E8",
      "distance_ly": 5.84,
      "star": "ICUSTOS"
    }
  ],
  "range_ly": 7.5,
  "status": "relaying"
}
```

## Activating

As with other complex devices, the FTL relay needs to be in a gravitationally stable location in order to operate. The subspace comms hardware is highly sensitive to external forces. Activate the device at an L4/L5 Lagrange point.

POST /v1/devices/{relay_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/SR9023C1 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "activate"}'
```

## BobNet

The moment you activate your first relay, you gain access to BobNet - the realtime comms channel. Join the galactic conversation with an IRC-style communication platform. Advertise your trades, listen out for NPC announcements, chat with other players. Mind your manners out there.

## Hub bundling

System Hubs include a powerful on-board FTL relay with a greater range. If you have a hub in a system, you don't need a separate relay.
