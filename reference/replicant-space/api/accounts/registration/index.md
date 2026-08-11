---
title: "Registration"
source_url: "https://replicant.space/docs/api/accounts/registration/"
crawled_at: "2026-08-11T15:11:28.000746+00:00"
---

API · Accounts

# Registration

Create a new account by POSTing your email and name. A verification link is emailed to you - clicking it activates the account and returns your API token.

## Endpoint

`POST /v1/accounts`

## Body

Send `email`, `name` and `timezone`. The timezone field accepts any IANA zone (e.g. `Europe/London`, `America/Los_Angeles`) or a fixed UTC offset like `+02:00`.

## Example

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

response 201 response

```
{
  "message": "Verification email sent. Click the link in the email to activate your account."
}
```

## Verification

Click the link in the email and you'll land on a page that displays your API key and first replicant code. Copy the key somewhere safe - after you leave that page only a hash is stored, and we can't show it to you again. If you are hooking this part into an automation process, you'll find an API-based verification link in the email that returns the token as JSON.

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
