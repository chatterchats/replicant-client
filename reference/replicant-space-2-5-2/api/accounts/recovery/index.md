---
title: "Recovery"
source_url: "https://replicant.space/docs/api/accounts/recovery/"
crawled_at: "2026-08-25T15:34:30.421754+00:00"
---

API · Accounts

# Recovery

Lost your API token? Trigger the recovery flow and a fresh verification link is emailed to you. Verifying issues a new token and invalidates the previous one.

## Endpoint

`POST /v1/accounts/recover`

## Example

POST /v1/accounts/recover   200 OK

```
$ curl -X POST https://api.replicant.space/v1/accounts/recover \
    -H "Content-Type: application/json" \
    -d '{"email": "bob@replicant.space"}'
```

response recover response

```
{
  "message": "If that email exists, a verification link has been sent"
}
```

The response is deliberately vague about whether the email exists as a general security feature.
