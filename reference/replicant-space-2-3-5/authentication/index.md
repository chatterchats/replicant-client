---
title: "Authentication"
source_url: "https://replicant.space/docs/authentication/"
crawled_at: "2026-08-03T00:42:37.583810+00:00"
---

Getting Started

# Authentication

One API token per account. Bearer auth header. Same as every other API you've played with.

## Registering an account

Create an account by POSTing your email, name and timezone to `/accounts`. Timezone accepts any IANA zone (e.g. `Europe/London`, `America/Los_Angeles`) or a fixed UTC offset like `+02:00`.

POST /v1/accounts   201 Created

```
$ curl -X POST https://api.replicant.space/v1/accounts \
    -H "Content-Type: application/json" \
    -d '{
      "email": "bob@replicant.space",
      "name": "Bob",
      "timezone": "Europe/London"
    }'
```

The response confirms the verification email is on its way:

response 201 response

```
{
  "message": "Verification email sent. Click the link in the email to activate your account."
}
```

Click the link in the email and you'll land on a page that displays your API key and first replicant code. Copy the key somewhere safe - after you leave that page only a hash is stored, and we can't show it to you again. If you are hooking this part into an automation process, you'll also find an API-based verification link in the email which will supply your API token in a JSON response.

response verification response

```
{
  "api_token": "OsiJIqbw_8tj4SLgeo_xKmYR23IF2UlhycBRSl1GZwAg7ZWTRVy8GZmaFUH2mp8E",
  "message": "Email verified successfully",
  "replicant": {
    "name": "bob-1",
    "replicant_code": "C2AF4A82"
  }
}
```

## Bearer tokens

Authenticate every request with an `Authorization: Bearer <token>` header. There is no OAuth flow, no per-request signing, no expiry. Just you and your token. Make a new one if you lose it.

example request   200 OK

```
# every request needs your bearer token
$ curl https://api.replicant.space/v1/accounts/me \
    -H "Authorization: Bearer $API_KEY"
```

### Rotating your token

You can rotate your token at any time by re-verifying your email address. The previous token is invalidated after you verify.

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
