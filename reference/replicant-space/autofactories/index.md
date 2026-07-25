---
title: "Autofactories"
source_url: "https://replicant.space/docs/autofactories/"
crawled_at: "2026-07-24T18:16:34.678910+00:00"
---

Infrastructure

# Autofactories

More than just an external printer. Autofactories are full-scale manufacturing operations with internal AMI controls that manage a queue of print jobs, waiting for resources, recycling old devices and generating blueprints from new devices.

## What makes them different

Your vessel can print, but it can't do anything else while it's doing that - 3D printers are sensitive bit of equipment. Autofactories have four features that make them more attractive than just printing everything yourself: managed print queues, device scrapping, blueprint discovery and automatic post-print commands.

## How to use

When specifying the type of device to print at an Autofactory, you can also configure auto-adoption by an AMI controller. This allows you to add a set of mining drones to the queue and have them automatically assigned to a specific mining controller for example.

You can also specify a command to send to the newly printed device, like automatically travelling to a specific location.

These two features combine to complement your bootstrapping strategy when setting up in a new system. Bring a set of resources on the voyage, print an Autofactory at the belt, and build up a queue.

While the Autofactory can only print one device at a time, there is nothing stopping you from printing more Autofactories and setting up a full industrial manufacturing operation in a system. Establish transport routes to keep the location inventory full.

## Queueing a print job

POST /v1/devices/{autofactory_code}   201 created

```
$ curl -X POST https://api.replicant.space/v1/devices/AF7C2310 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "enqueue_print",
      "device_type": "mining_drone",
      "tags": ["fleet-713"],
      "controller": "MC91FF22",
      "oncomplete": {
        "command": "travel",
        "destination": "LERNA-BELT-1"
      }
    }'
```

The `tags`, `controller` and `oncomplete` fields are optional. The `oncomplete` command currently only supports `travel` and `start_mining` - more commands will be supported soon.

response 201 response

```
{
  "queue": [],
  "queue_length": 0,
  "status": "enqueued"
}
```

## Viewing the queue

The print queue will show in the normal device details on the `/devices/<code>` endpoint.

## Removing items from the queue

POST /v1/devices/{autofactory_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/AF7C2310 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "dequeue_print",
      "index": 1
    }'
```

## Clear the queue

POST /v1/devices/{autofactory_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/AF7C2310 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "clear_queue"
    }'
```

## Cancel the current print

POST /v1/devices/{autofactory_code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/AF7C2310 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "deactivate"
    }'
```

## Relocation

Autofactories have the `modular` feature. Use the `compact` command to collapse the factory into a transportable state, attach it to a surge carrier, and `unfurl` it at the new location. The factory is offline and its print queue is paused for the duration.

## An alternative

If the upfront cost of an Autofactory is too much to bear, there is a blueprint out there for a Structural Fabricator, which might be useful despite its limitations.
