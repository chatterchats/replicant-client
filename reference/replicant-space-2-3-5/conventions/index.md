---
title: "Conventions"
source_url: "https://replicant.space/docs/conventions/"
crawled_at: "2026-08-03T00:42:37.929908+00:00"
---

Getting Started

# Conventions

The naming, formats, and shapes that show up everywhere in Replicant Space. Read this once and the rest of the reference will make more sense.

## Location codes

Every place in the galaxy has a deterministic code. The code is a hyphen-separated path from a star outwards. Star systems are named like SOL or PLANDIMUS as a sequence of upper case letters.

// location code grammar

```
# dissecting a location code
#  ┌─ system        ┌─ planet  ┌─ Lagrange point
#  │                │          │
#  SOL              -3         -L4

# first moon of the second planet of star BOOP
"location": "BOOP-2-1"

# second asteroid belt of star PORRAMA
"location": "PORRAMA-BELT-2"

# Kuiper belt of the UNARI system
"location": "UNARI-KUIPER"
```

Suffixes you'll see:

- `-1`, `-2`... for planets in orbital order outwards from the star.
- `-1-1`, `-1-2`... for moons.
- `-BELT-N` for asteroid belts (positioned between planets).
- `-KUIPER` for Kuiper belts. `-OORT` for the Oort cloud.
- `-L1` through `-L5` for Lagrange points.

## Distances

Distance is measured with Astronomical Units (AU) within systems, or Light Years (LY) when talking about objects in interstellar space. There is no orbital mechanics in the game for simplicity, planets are always the same distance from each other.

## Times

All timestamps are ISO-8601 with timezone offsets. The API does not assume UTC. Durations are returned in seconds as integers under fields suffixed `_seconds`.

## Codes for things

Everything you print and control is referred to as a device. You are a device. Your device is hosted in a device. All devices have a stable 8-character hexadecimal code. Codes never get reused. If you decommission a device, its code retires with it.

## Versioning

The current major version is `v1`, included in the path. Breaking changes mean a new major. Additive changes (new fields, new endpoints, new event types) are not versioned - write your clients to ignore unknown keys.
