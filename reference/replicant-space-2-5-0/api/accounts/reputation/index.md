---
title: "Reputation"
source_url: "https://replicant.space/docs/api/accounts/reputation/"
crawled_at: "2026-08-11T15:11:28.014129+00:00"
---

API · Accounts

# Reputation

Your standing with the various species you've discovered in the galaxy.

## Endpoint

`GET /v1/accounts/reputation`

As you scan planets and moons, you will eventually discover intelligent life. Those people will want help. The more you help them, the higher tier events you'll get roped into. View your current reputation level with this endpoint.

## Example

GET /v1/accounts/reputation   200 OK

```
$ curl https://api.replicant.space/v1/account/reputation \
    -H "Authorization: Bearer $API_KEY"
```
