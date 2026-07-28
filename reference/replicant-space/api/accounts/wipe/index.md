---
title: "Wipe account"
source_url: "https://replicant.space/docs/api/accounts/wipe/"
crawled_at: "2026-07-28T00:53:10.627944+00:00"
---

API · Accounts

# Wipe account

Self-service account deletion. Triggering the endpoint emails a confirmation link - clicking it permanently erases your replicants, devices, inventory, blueprints, achievements and messages.

## Endpoint

`DELETE /v1/accounts/me`

## Example

DELETE /v1/accounts/me   202 Accepted

```
$ curl -X DELETE https://api.replicant.space/v1/accounts/me \
    -H "Authorization: Bearer $API_KEY"
```

response 202 response

```
{
  "message": "Confirmation email sent. Click the link in the email to wipe your account."
}
```

## Confirmation

The email contains a one-time link to a confirmation page that lists exactly what will be deleted - account, replicant count, device count, message count. Submitting the page performs the wipe; there is no API-level shortcut and the link cannot be replayed.

Wipe links expire after the same window as a verification link (see [Recovery](../recovery/index.md)). If the link expires, call `DELETE /v1/accounts/me` again to send a fresh one.

## What gets deleted

Everything tied to your account is removed: replicants, devices and their inventories, device permissions, scans, surveys, species reputation, blueprints, achievements, location-event history, BobNet messages sent by your replicants, and your trades and trade transactions.

Shared-world references are preserved where possible - location events you completed or discovered, and resource sites you found, are kept but with the discovering replicant nulled out. Your email is freed up immediately and can be used to register a new account.
