---
title: "Feedback"
source_url: "https://replicant.space/docs/api/accounts/feedback/"
crawled_at: "2026-08-07T00:51:29.831808+00:00"
---

API · Accounts

# Feedback

A way for players to submit their ideas, report typos or let the maintainers know about any bugs they spot.

Games like this thrive on player feedback. If you spot something wrong or have an interesting idea, please let us know!

## Endpoint

`POST /v1/feedback`

## Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `type` | string | One of `typo`, `bug` or `idea`. |
| `body` | string | What you want to tell us. Up to 2000 characters. |

## Rate limit

Limited to 10 submissions per hour per account. See [rate limits](../../../rate-limits/index.md) for details.

## Example

POST /v1/feedback   200 OK

```
$ curl -X POST https://api.replicant.space/v1/feedback \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "type": "bug",
      "body": "Searching at an asteroid belt shows as scanning in the device list."
    }'
```

response 200 response

```
{
  "status": "feedback_received"
}
```
