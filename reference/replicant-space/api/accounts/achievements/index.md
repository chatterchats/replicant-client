---
title: "Achievements"
source_url: "https://replicant.space/docs/api/accounts/achievements/"
crawled_at: "2026-07-28T00:53:10.436190+00:00"
---

API · Accounts

# Achievements

The more things you do, the more achievements you grind. There are lots of different events to participate in.

## Public endpoints

These endpoints are **public** and require no authentication. If you're building a client interface, dashboard, or community tool that wants to display achievement data, you can call these directly.

### List all achievements

`GET /v1/achievements`

Returns every achievement with its title, description, category, XP reward, the number of players who have earned it, and the date it was most recently achieved. Sorted by most recently achieved first.

Hidden achievements (seasonal story content) are excluded from the list until the story progresses.

GET /v1/achievements   200 OK

```
$ curl https://api.replicant.space/v1/achievements
```

response response

```
{
  "achievements": [
    {
      "achievement_key": "first_scan",
      "title": "First Scan",
      "description": "Completed your first system scan.",
      "category": "exploration",
      "xp_reward": 100,
      "player_count": 42,
      "last_achieved_at": "2026-07-04T18:22:10"
    },
    {
      "achievement_key": "devices_in_two_systems",
      "title": "Two Systems",
      "description": "Have devices in two different star systems.",
      "category": "infrastructure",
      "xp_reward": 250,
      "player_count": 18,
      "last_achieved_at": "2026-07-03T09:11:45"
    }
  ]
}
```

#### Filter by category

Pass a `category` query parameter to filter the list to a single category.

GET /v1/achievements?category=exploration   200 OK

```
$ curl https://api.replicant.space/v1/achievements?category=exploration
```

#### Categories

- `community` - multiplayer and social achievements.
- `exploration` - scanning, discovering, and reaching new systems.
- `infrastructure` - building and expanding your devices across the galaxy.
- `location_events` - the variety of encounters at planets and moons.
- `progression` - milestones as you progress your account.
- `travel` - interstellar milestones.

### Achievement detail

`GET /v1/achievements/:achievement_key`

Returns a single achievement along with every player who has earned it, sorted by most recent first.

GET /v1/achievements/first_scan   200 OK

```
$ curl https://api.replicant.space/v1/achievements/first_scan
```

response response

```
{
  "achievement_key": "first_scan",
  "title": "First Scan",
  "description": "Completed your first system scan.",
  "category": "exploration",
  "xp_reward": 100,
  "player_count": 42,
  "players": [
    {
      "account_name": "alice",
      "achieved_at": "2026-07-04T18:22:10"
    },
    {
      "account_name": "bob",
      "achieved_at": "2026-06-30T14:05:33"
    }
  ]
}
```

## Your achievements

`GET /v1/accounts/achievements`

Returns the achievements earned by your account. Requires authentication.

GET /v1/accounts/achievements   200 OK

```
$ curl https://api.replicant.space/v1/accounts/achievements \
    -H "Authorization: Bearer $API_KEY"
```
