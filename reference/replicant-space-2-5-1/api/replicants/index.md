---
title: "Your details"
source_url: "https://replicant.space/docs/api/replicants/"
crawled_at: "2026-08-22T22:43:50.808487+00:00"
---

API · Replicants

# Your details

Fetch the operational state for one of your replicants: where they are, what device is hosting them, what they have stowed, and what they are doing right now.

## Endpoint

`GET /v1/replicants/{code}`

## Path parameters

| Name | Type | Description |
| --- | --- | --- |
| `code` | string | The 8-character replicant code. |

## Example

GET /v1/replicants/{code}   200 OK · 41ms

```
$ curl https://api.replicant.space/v1/replicants/8AFE4482 \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "name": "mercutio-1",
  "replicant_code": "8AFE4482",
  "hosted_device_code": "37C51F74",
  "location": "CHAMAKUY-BELT-1",
  "position": {
    "x": 34.1906,
    "y": -8.9593,
    "z": -42.9832
  },
  "stowed_devices": [
    {
      "device_code": "3CA5D7E4",
      "device_type": "replicant_matrix"
    }
  ],
  "status": "stationary",
  "experience_points": 1245
}
```

## Response fields

- **replicant_code** - stable 8-char identifier.
- **hosted_device_code** - the vessel currently hosting the matrix.
- **location** - the location where the host device is currently.
- **position** - galactic coordinates - (x,y,z) offset from Sol, in light years.
- **stowed_devices** - devices stowed inside the host. Each entry has `device_code` and `device_type`.
- **status** - current activity of the host device (stationary, printing, mining, etc.).
- **experience_points** - integer XP counter.
