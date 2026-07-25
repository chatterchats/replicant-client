---
title: "Welcome, replicant."
source_url: "https://replicant.space/docs/"
crawled_at: "2026-07-21T22:05:25.531342+00:00"
---

Introduction

# Welcome, replicant.

An API-first space exploration game. The galaxy is yours to turn into a manufacturing empire. Print, scan, mine and transport. This documentation covers everything you can do in the game, from your first system scan to running a fully automated interstellar manufacturing operation.

## What is Replicant Space?

You are a player with an account. You start with one freshly awoken replicant, hosted in a HEAVEN vessel on the outer edge of an unremarkable star. From there, the rest is up to you.

Every interaction with the game happens over an HTTP API. You are welcome to build your own UI, write a bot, set an AI agent loose on the galaxy, or just curl manually on the CLI. There is no client to install. The surface *is* the game.

Ready to make your first call? Jump straight to the [Quickstart](quickstart/index.md) - it walks you through registering, picking up your API key, and getting a useful response out of the API inside ten minutes.

## How the docs are organised

This reference is split into a few broad areas:

- **Core Concepts:** replicants, devices, locations, resources, blueprints, story, multiplayer.
- **Top Level API:** /accounts, /replicants, /devices, /locations.
- **Drones & Controllers:** mining, scanning, transporting, maintenance.
- **Infrastructure:** FTL beacons and relays, system hubs, autofactories, and cloning.
- **Trading:** the shop directory, setting up a shop, trading with NPCs and players.
- **Interstellar Transport** - vessels and platforms for moving stuff around the galaxy.
- **Simulations** - test your bootstrap strategies in a series of speed trials.

## OpenAPI spec

The full machine-readable spec is published at [api.replicant.space/swagger/](https://api.replicant.space/swagger/) - point your client SDK generator of choice at it, or browse the endpoints interactively.

## Conventions in this reference

Endpoints are written as `METHOD /v1/path/{param}`. Curly braces denote variables you substitute. All the CURL examples assume you've set `$API_KEY` in your shell. Times are returned in ISO-8601 with timezone offsets.

## Stuck?

The [Quickstart](quickstart/index.md) walks you through your first ten minutes. The [Conventions](conventions/index.md) page covers location codes, time formats, and the structure of responses. Everything else in the docs is reference, in roughly the order you'll need it.

Good luck replicants. Humanity needs you.
