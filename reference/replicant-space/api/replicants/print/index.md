---
title: "Print"
source_url: "https://replicant.space/docs/api/replicants/print/"
crawled_at: "2026-07-24T18:16:34.352342+00:00"
---

API · Replicants

# Print

Use the replicant's onboard 3D printer to make a device from a blueprint. Resources are taken from the current location. The printer is busy until the job completes.

## Endpoint

`POST /v1/replicants/{code}/print`

## Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `device_type` | string | Blueprint device type from `GET /v1/blueprints`. Must be something you know how to print. |
| `command` | string | Optional command instead of a new print job. Use `clear_queue` to remove queued print jobs, or `cancel` to stop the current print. |

## Start a print

POST /v1/replicants/{code}/print   202 accepted

```
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/print \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"device_type": "mining_drone"}'
```

response response

```
{
  "status": "enqueued"
}
```

## Clear the queue

If you queued a print job, but it hasn't started printing yet, due to a lack of resources at your current location, you can send the *clear_queue* command to remove anything that hasn't started yet.

POST /v1/replicants/{code}/print   200 OK

```
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/print \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "clear_queue"}'
```

response response

```
{
  "queue": [],
  "queue_length": 0,
  "status": "queue_cleared"
}
```

## Cancel the current print

If you've already started printing, and want to stop and return the resources to your location, send the *cancel* command.

POST /v1/replicants/{code}/print   200 OK

```
$ curl -X POST https://api.replicant.space/v1/replicants/57F0F6C8/print \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "cancel"}'
```

response response

```
{
  "device_type": "survey_drone",
  "resources_refunded": true,
  "status": "af_print_cancelled"
}
```

## Hosted device controls

You can also issue `clear_queue` and `deactivate` directly to your replicant's hosted device. See [Autofactories](../../../autofactories/index.md) for more advanced print controls.
