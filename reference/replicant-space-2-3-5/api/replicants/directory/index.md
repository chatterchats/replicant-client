---
title: "Directory"
source_url: "https://replicant.space/docs/api/replicants/directory/"
crawled_at: "2026-08-03T00:42:37.277702+00:00"
---

API · Replicants

# Directory

Browse the replicants currently known to the galaxy, then inspect a public profile to learn who they are and what they are working on.

## Endpoint

`GET /v1/replicants`

## Query parameters

| Name | Type | Description |
| --- | --- | --- |
| `cursor` | integer · optional | Position in the list to start from, default null. |
| `limit` | integer · optional | Number of replicants to show, default 20. |
| `latest` | boolean · optional | Show latest replicants. Default `false`. Incompatible with `cursor`. |
| `name` | string · optional | Search replicant names. Matching is case-insensitive. |

## Example

GET /v1/replicants   200 OK

```
$ curl "https://api.replicant.space/v1/replicants?latest=true&limit=10&name=syl" \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "replicants": [
    {
      "name": "Sylphrena",
      "replicant_code": "30B93F2F",
      "last_location": "IMPOLLA",
      "is_npc": true
    }
  ],
  ...
  "next_cursor": 10
}
```

## Response fields

- **name** - public name.
- **replicant_code** - the replicant's internal code.
- **last_location** - the last known location of the replicant (according to BobNet).
- **is_npc** - whether the replicant is a player or an NPC.

## Replicant profile

Use the `replicant_code` from the directory with `GET /v1/replicants/{code}` to see the replicant's public profile details.

Note, this is the same endpoint for viewing your own details. Many fields are removed from this view when inspecting another replicant's details.

GET /v1/replicants/{code}   200 OK

```
$ curl https://api.replicant.space/v1/replicants/0B6A463F \
    -H "Authorization: Bearer $API_KEY"
```

response response

```
{
  "name": "Sylphrena",
  "replicant_code": "0B6A463F",
  "description": "A curious individual, Sylphrena is interested in terraforming barren worlds.",
  "plan": "Working on two more autofactories",
  "project": "Trying to find systems to help the Zynthara expand.",
  "pronouns": "she/her",
  "is_npc": true
}
```

## Profile fields

- **description** - public character description.
- **plan** - the replicant's current near-term plan.
- **project** - the larger project or goal they are pursuing.
- **pronouns** - public pronouns for the replicant.

Set each of your replicant's profile details with the [configure endpoint](../configure/index.md).
