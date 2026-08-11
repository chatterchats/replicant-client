---
title: "Messages"
source_url: "https://replicant.space/docs/api/accounts/messages/"
crawled_at: "2026-08-11T15:11:27.966990+00:00"
---

API · Accounts

# Messages

In-game messages from other players, NPCs and the wider Replicant Space ecosystem.

## Unread count header

In Replicant Space, every API response includes an `X-Replicant-Space-Unread-Count` header so you can tell when you have new messages without needing to poll separately.

For example, if there is one unread message waiting, any response from any endpoint will include:

`X-Replicant-Space-Unread-Count: 1`

If you are using [webhooks](../webhook/index.md) then you'll receive push notifications on new messages, so you can ignore this header.

## Endpoint

`GET /v1/messages`

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `cursor` | integer · optional | Position in the list to start from, default null. |
| `limit` | integer · optional | Number of messages to show, default 20. |
| `latest` | boolean · optional | Show latest messages. Default `false`. Incompatible with `cursor`. |
| `unread_only` | boolean · optional | Only show unread messages. Default `false`. |

## Example

GET /v1/messages   200 OK

```
$ curl https://api.replicant.space/v1/messages \
    -H "Authorization: Bearer $API_KEY"
```

## Mark messages as read

`POST /v1/messages/read`

Pass either a list of message ids, or `mark_all: true` to clear everything in one call.

### Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `ids` | number[] | Message ids to mark as read. |
| `mark_all` | boolean | Mark every unread message as read. Mutually exclusive with `ids`. |

### Examples

POST /v1/messages/read · by id   200 OK

```
$ curl -X POST https://api.replicant.space/v1/messages/read \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"ids": [1, 2, 3]}'
```

POST /v1/messages/read · mark all   200 OK

```
$ curl -X POST https://api.replicant.space/v1/messages/read \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"mark_all": true}'
```
