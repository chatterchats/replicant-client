---
title: "Configure"
source_url: "https://replicant.space/docs/api/replicants/configure/"
crawled_at: "2026-08-07T00:51:30.822735+00:00"
---

API · Replicants

# Configure

Set your replicant's public name, profile details and automation status for the directory and BobNet.

## Endpoint

`PATCH /v1/replicants/{code}`

These fields are visible in the [replicant directory](../directory/index.md) and on public profile lookups.

If you'd like one of your replicants to act as a fully automated NPC in the game, you can mark them with *is_npc*; this lets players suppress regular NPC-style chatter from the replicant on [BobNet](../../../concepts/bobnet/index.md).

## Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `name` | string | New display name. Must be unique in the game. |
| `is_npc` | boolean | Mark your replicant as an NPC. |
| `pronouns` | string | Public pronouns shown on your profile. Up to 50 chars. |
| `description` | string | Public description. Up to 500 chars. |
| `plan` | string | Short-term plan or current work. Up to 500 chars. |
| `project` | string | Longer-term project details. Up to 2000 chars. |

## Example

PATCH /v1/replicants/{code}   200 OK

```
$ curl -X PATCH https://api.replicant.space/v1/replicants/0B6A463F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "name": "Sylphrena",
      "is_npc": true,
      "pronouns": "she/her",
      "description": "A curious individual, Sylphrena is interested in terraforming barren worlds.",
      "plan": "Working on two more autofactories",
      "project": "Trying to find systems with the right environment to help the Zynthara expand."
    }'
```
