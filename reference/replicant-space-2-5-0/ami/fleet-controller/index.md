---
title: "AMI Fleet Controller"
source_url: "https://replicant.space/docs/ami/fleet-controller/"
crawled_at: "2026-08-11T15:11:27.744041+00:00"
---

AMI Controllers

# AMI Fleet Controller

Move large groups of devices with a single command. Adopt your surge-capable hardware under a fleet controller, issue one travel command, and they all move. Chain controllers together to coordinate entire fleets across the galaxy.

## What it does

Unlike other AMI controllers, the Fleet Controller has no directives. Its only job is to relay `travel` commands to every device it has adopted. Adopt your surge platforms, cargo freighters, and other carriers, then issue a single travel command to the controller. Every adopted device receives the order and moves.

The fleet controller is the simplest way to move large numbers of devices. Instead of issuing separate travel commands to each device, you adopt them to a fleet controller and have them move together.

## Cascading controllers

Fleet controllers can adopt other fleet controllers. When a travel command is issued, it cascades down through every level - adopted controllers relay the command to their own adopted devices, and so on.

This is useful when you're coordinating multiple travel groups across the system. Assign a fleet controller to each group, then when it's time to bring them all to the same destination, adopt those group controllers under a single top-level controller and issue one travel command. The entire operation fans out from there.

## Location and routing

A fleet controller doesn't need to be in the same system as its adopted devices. As long as the controller is connected to each device through the [FTL relay network](../../ftl-relays/index.md), the travel command goes through. Each adopted device then computes a route from wherever it currently is to the destination - no need to assemble first.

## Adopting devices

Only surge-capable devices can be adopted. Smaller drones should be attached or stowed the usual way.

## How to get one

The Fleet Controller isn't available from any blueprint catalogue or autofactory queue. It's a special reward, granted by a particular NPC back at Earth in exchange for helping them out with a little project. If you've been hanging around `SOL-3` and listening to BobNet, you'll know who to talk to.
