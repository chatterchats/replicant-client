---
title: "Rate limits"
source_url: "https://replicant.space/docs/rate-limits/"
crawled_at: "2026-08-07T00:51:31.872985+00:00"
---

Getting Started

# Rate limits

Be kind to the server, limits are a little conservative for launch but may be relaxed later.

## Per-endpoint limits

Account endpoints are throttled on an hourly window:

| Endpoint | Limit |
| --- | --- |
| Account registration | 10 / hour |
| Account verification | 30 / hour |
| Changing your webhook | 12 / hour |
| Feedback submissions | 10 / hour |
| Star catalogue downloads | 1 / minute |

## Global limits

Everything else falls under two per-minute buckets, scoped to your API token:

| Request type | Limit |
| --- | --- |
| Reads (`GET`) | 120 / minute |
| Actions (`POST`, `DELETE`, `PATCH`, etc.) | 60 / minute |

## When you hit a limit

Exceeding any limit returns `429 Too Many Requests` with a `Retry-After` header telling you how many seconds to wait before trying again.

response 429 response

```
HTTP/1.1 429 Too Many Requests
Retry-After: 47
X-RateLimit-Limit: 60
X-RateLimit-Remaining: 0
X-RateLimit-Reset: 1779087998
...

{
  "code": 429,
  "status": "Too Many Requests"
}
```

Be polite. Prefer batching or polling over hammering the same endpoint. Most async requests will give you an ETA for when they will be finished.
