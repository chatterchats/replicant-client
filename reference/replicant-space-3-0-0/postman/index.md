---
title: "Postman collection"
source_url: "https://replicant.space/docs/postman/"
crawled_at: "2026-09-02T20:03:42.922989+00:00"
---

Getting Started

# Postman collection

A Postman collection to give you a head start with the API. Import it, configure your API key for the game, and start poking around.

## What is Postman?

[Postman](https://www.postman.com/) is a website/desktop app for sending and inspecting HTTP requests. You build up a library of requests, save them into collections, and call them without having to remember the details.

## Download

Grab the collection file here:

- [ReplicantSpaceStarter.postman_collection.json](https://replicant.space/ReplicantSpaceStarter.postman_collection.json)

## Import into Postman

Open Postman, find the **Import** feature somewhere on the left, and upload the file you downloaded above.

## Collection variables

The collection ships with three variables you can edit by right-clicking the collection and choosing **Edit**, then the **Variables** tab.

- `baseUrl` - already set to `https://api.replicant.space/v1`. No need to change it.
- `bearerToken` - your API key. Paste the key you receive when verifying your email.
- `replicantCode` - the 8-character code of the replicant from your account registration.

  ![Postman collection variables panel](https://replicant.space/postman_variables.png)

Remember to hit **Save** after editing.
