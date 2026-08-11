---
title: "Errors"
source_url: "https://replicant.space/docs/errors/"
crawled_at: "2026-08-11T15:11:29.717123+00:00"
---

Getting Started

# Errors

Every error response has the same shape - a single error field with a human-readable message. The HTTP status tells you the category.

## Error envelope

response // error response shape

```
{
  "error": "Insufficient conductive resource to print this device"
}
```

## HTTP status codes

| Status | Meaning |
| --- | --- |
| 400 | Bad Request - most validation and game-state errors: missing fields, invalid commands, invalid destination, criteria not met. |
| 401 | Unauthorized - missing or invalid bearer token. |
| 403 | Forbidden - not your replicant or device, email not verified, gated resources. |
| 404 | Not Found - missing replicant, device, star, location, event, trade, or token. |
| 409 | Conflict - account still provisioning, duplicate email, replicant offline. |
| 410 | Gone - expired verification link. |
| 413 | Payload Too Large - oversized request body. |
| 429 | Too Many Requests - hit the rate limits. |
| 500 | Internal Server Error - unknown issue on the server. |
| 503 | Service Unavailable - maintenance mode, galaxy not seeded, etc. |
