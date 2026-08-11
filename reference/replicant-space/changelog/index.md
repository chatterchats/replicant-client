---
title: "Changelog"
source_url: "https://replicant.space/changelog/"
crawled_at: "2026-08-11T15:11:27.663660+00:00"
---

// Changelog

# Changelog

What's changed in the Replicant Space galaxy? Keep up with the latest.

1. v2.5.0 [◇](index.md#v2.5.0)

    10 August 2026

   This release brings forward some features that were originally planned for Season Three. The galaxy is getting smaller!

   The new player experience has changed significantly. Instead of spawning in the middle of our over-populated solzone, players start in a remote region of space with a guaranteed belt, some salvage and some people that need help. A new tutorial system guides players through the core mechanics.

   Each new player has a second preconfigured Heaven Vessel parked in the Oort cloud around SOL. FTL Slingshots allow them to travel between SOL and home.

   Private regions also come with System Wards that lock mining and event access, providing a safe space to learn the game.

   Before existing players start revolting, be advised that you all have a private region out there. Head to the equipment locker in SOL-OORT to retrieve your slingshot to go visit.

   ### Online docs

   - Added a new [Tutorials](../tutorials/index.md) page to explain how to use the new in-game tutorial system.
   - Updated the [Quickstart](../quickstart/index.md) page to match the in-game bootstrap tutorial.
   - Added a new [FTL Slingshots](../ftl-slingshots/index.md) page to explain how to fling your consciousness across the galaxy. With a bit of prep.
   - Added a new [System Wards](../system-wards/index.md) page to show how to claim and lock a system to your account.

   ### API updates

   - The initial asteroid belt site will now regenerate automatically when a new player scans the system.
   - New players will now start the game in a private region of space with ~12 stars to explore.
   - New endpoints added for the in-game tutorial system.
   - New blueprint: ftl_slingshot
   - New blueprint: system_ward
   - An old equipment locker has been discovered in SOL-OORT. It accepts POST requests to /devices/:code/retrieve.

   Moments from the wormhole being completed. You got this replicants! o7
2. v2.4.0 [◇](index.md#v2.4.0)

    6 August 2026

   This is a larger release containing content pack updates for Season Two’s finale. Apologies up front for the extra exposition.

   ### Online docs

   - Documented how the new [Galactic Observatory](../galactic-observatory/index.md#triangulation) subspace triangulation command works. Familiarise yourself - this will be useful shortly.

   ### API updates

   - Fixed a bug where an AMI mining controller would target a drone to a resource that just ran out, but then not marking it idle for reselection.
   - Fixed another BobNet messaging bug where your nearest relay device would reject the message unless it was an FTL Relay.
   - Prevented travel and location commands from revealing uncatalogued locations.
   - Fixed a bug where cancelling a travel command still awarded you the distance achievements upon your return.
   - The deactivate command now also cancels prospecting and triangulation actions.
   - Added the list of consumed device codes to the print.completed event.
   - Vessel printing no longer fails when supplying the flatpack:false param.
   - Added new events for device.compacting, device.compacted, device.unfurling, device.unfurled.
   - Added tags field to print.started and print.completed.

   Only one blueprint in the gamma region remains to be discovered, to complete the wormhole. The season finale approaches. o7
3. v2.3.6 [◇](index.md#v2.3.6)

    4 August 2026

   Introducing a new bit of kit in the FTL networking arsenal, and ensuring all relay-capable devices can carry BobNet chatter.

   A small patch to round off some little bits before the next big one.

   ### Online docs

   - Minor update to the [FTL Relays](../ftl-relays/index.md) page to mention other relay-capable devices.
   - Added brief explanation of our galactic coordinates to the [Locations](../concepts/locations/index.md) page.

   ### API updates

   - Updated BobNet protocol to use any device with the relay feature.
   - The Deep Space Relay Station device can now sustain a 10 light-year FTL network connection when activated.
   - Fixed bug with location event device rewards not inheriting the blueprint directives.

   ROSALIATE-3 looks like a great final stop for the Ark, we should optimise our drop-offs in that direction. o7
4. v2.3.5 [◇](index.md#v2.3.5)

    2 August 2026

   Some minor tweaks based on recent player feedback. Coolest being the new option to print modular devices in a flatpacked state. It’s a little quicker too. Win-win.

   ### Online docs

   - Updated trade.completed event examples in the [Event Catalogue](../api/events/catalogue/index.md).
   - Updated the [Autofactories](../autofactories/index.md) page to explain how to print modular devices flatpacked.

   ### API updates

   - Fixed bug with replication targets missing the matrix feature.
   - Fixed bug in the belt search directive, where it would assign too many drones to search for new sites, resulting in search failures.
   - Trade controller (shop) announcement message length increased from 80 to 500 characters.
   - Increased event stream history from 10k to 100k entries.
   - Implemented long-term event history fundamentals on the server - a step towards removing event logs.
   - Added scanned field to moon and planet responses, for both true and false scenarios.
   - Added trade outcomes to trade.completed event - rewards_received for the buyer and criteria_received for the seller.
   - Added a new report shape for the gather_salvage directive, to emit ami.mining.digest events.
   - Added new option to autofactories to print modular devices in a flatpack state, ready for shipping.
   - Added completes_at field to the print.started event.

   Riker has 16 colony surveys so far, he’s starting to see some good options for a route to take the Ark on. o7
5. v2.3.4 [◇](index.md#v2.3.4)

    29 July 2026

   The big thing for today is the launch of the interactive tutorial on the website, to help players see what they’re getting into before signing up.

   ### Website

   - Added the new [Tutorial](https://replicant.space/tutorial/) page to the nav.

   ### API updates

   - Fixed a bug with planets spinning in retrograde scoring zero in Riker’s ratings.

   *Keep the colony surveys coming. Each good find is a potential home for humanity! o7*
6. v2.3.3 [◇](index.md#v2.3.3)

    27 July 2026

   Loads of bug fixes and minor features with this release. Thank you to everyone who’s playing and contributing your ideas and issues.

   Big one for this release is improving events relating to controller directives. If the controller is inactive, then events from devices go straight to the stream. The inclusion of full scan details in the digest should help with those contributing to the Colony Survey project.

   Oh and rocks. Watch out for those.

   ### Online docs

   - Updated the [Event Catalogue](../api/events/catalogue/index.md) search.started and blueprint.unlock examples.
   - Added the new quantity field to the [Autofactories](../autofactories/index.md) example.
   - Updated [Star Catalogue](../api/locations/star-catalogue/index.md) to show the new region field.
   - Updated [Device List](../api/devices/list/index.md) page to show the new tag/untagged filters.
   - Updated the [AMI Digests](../api/events/ami-digests/index.md) page with details on collated world scans and details on inactive device events.

   ### API updates

   - Devices will now send events directly to the stream if their AMI controller’s directive is inactive.
   - AMI Survey Controller digests now include full scan results for any worlds scanned since the last digest.
   - AMI Survey Controllers now coordinate the launch and recall of devices while remaining active, for cleaner digests.
   - ETA seconds in various responses are now integers instead of floats.
   - Print times for blueprints will now show as integers instead of floats.
   - Added print_time field to the blueprint.unlock event.
   - Added new quantity param to the autofactory enqueue_print command, to enqueue multiple of the same device.
   - Added the missing cursor field to the new event stream schema.
   - Added region (solzone, alpha, beta, gamma) to the star catalogue. Things discovered between regions will show as null.
   - Added region and has_hub fields to the stellar census responses.
   - Added tag filter to the /devices endpoint, which makes it act like the /devices/tag/:tag endpoint.
   - Added untagged filter option to the /devices endpoint to return all devices with no tags. Incompatible with the tag field.
   - Added hosting_replicant field to device status output for vessels containing a replicant matrix.
   - Fixed bug with asteroid generation. The rocks are back.
   - Slowed down NPC BobNet chatter.

   Humanity will spread out. We’ll need options. Replicants, find some good homes! o7
7. v2.3.2 [◇](index.md#v2.3.2)

    25 July 2026

   Today’s patch introduces a pair of new leaderboards related to the Colony Survey project.

   Riker will shortly be asking replicants to help find suitable stopping points for when the Ark departs Earth.

   Look out for an update from Riker in your messages.

   ### API updates

   - New leaderboards: colony_moon and colony_planet.
   - Improved performance on scanning new systems.

   Brave new worlds are awaiting. But not too hot, and not too cold please. Replicants, the people need you! o7
8. v2.3.1 [◇](index.md#v2.3.1)

    21 July 2026

   Today’s patch is a followup to the event stream release earlier this week. Thanks for all the feedback from players trying this out. Keep reporting any issues or gaps that would help with maintaining accurate state on your end of things.

   A new event is running in the Beta region. The event will only be active while the megastructure is incomplete.

   ### Online docs

   - Updated [Event Catalogue](index.md) with a variety of corrections - see API Updates below.

   ### API updates

   - Event payload: added arrives_at field to travel.departed.
   - Event payload: added consumed resources and devices to event.completed.
   - Event payload: added contributed_devices to megastructure.contributed.
   - Event payload: added message_id to message.new.
   - Event payload: added new_device_codes list to trade.completed.
   - New blueprint.unlocked event added with blueprint details.
   - Fixed bug with final_arrives_at sometimes showing times before departure.
   - Performance improvement on /replicant/:code/stars endpoint by simplifying explored logic.
   - Fixed a bug where the observability metrics were impacting game performance.
   - Added the pulse_runner as a reward from the new Gwynhari event in the beta region.

   Exotic matter pulses through the ring as replicants race to restore it. Is this wise? Time will tell… o7
9. v2.3.0 [◇](index.md#v2.3.0)

    19 July 2026

   Event streaming!

   The webhook system is now deprecated in favour of event streams. All players now have access to an SSE endpoint for live streaming events from the game for direct processing. An event catalogue has been added to the online docs with example event payloads for every event that the game generates. Travel departure, arrival, cancellation - mining starting, stopping, retargetting - scans, searches, prospects, deposits, collections, attach, deploy, etc etc. Everything now generates an event to a stream that can be paused and resumed later as needed.

   Now some of you have waaay too many devices, so it isn’t practical to stream everything. But thankfully, the AMI controller system has been upgraded to produce per-directive reports on progress, including a full list of current device status and recent events - at a configurable pace. Every directive has a customised report payload surfacing the more interesting stats. If you have a mining controller with 20 drones, you’ll get one status update every 10 seconds (interval multiplier depending) with each drone’s status.

   ### Online docs

   - New API Events section in the documentation: [Event Logs](../api/events/logs/index.md), [Event Streams](../api/events/stream/index.md), [AMI Digests](../api/events/ami-digests/index.md), [Event Catalogue](../api/events/catalogue/index.md).
   - Added event and message configuration to the [Account details](../api/accounts/me/index.md) and pages.
   - Webhooks documentation has been deprecated.

   ### API updates

   - New API endpoint for viewing event logs: /events with filters for device, category, type, dates, etc.
   - New SSE API endpoint for a realtime event stream: /events/stream with the ability to resume the stream later.
   - New star catalogue endpoint: /stars showing all known star systems.
   - Fixed bug with deploy command not respecting cooperative replicants.
   - New hub wear warning at 50% operational capacity to give players more time to prepare for maintenance.
   - Increased request payload limit from 2KB to 64KB.
   - Fixed database deadlock issue that was aborting travel routes a hop too soon.

   All three regions have been unlocked, but what is this weird ring thing huh? Time to get exotic, replicants! o7
10. v2.2.0 [◇](index.md#v2.2.0)

     12 July 2026

    Mid-season feature drop: Simulations!

    Our lovely bunch of replicants recently constructed a datacentre megastructure in the MIRFAKA system. It was used to trace the potential origin of those giant asteroids that came for the Ark.

    The datacentre is mostly sitting idle, processing the occasional observatory report. So… Bill’s been tinkering with all the fancy analytical hardware. He came up with a design to interface replicants with it directly for the purpose of running simulations.

    The simulations run pretty hot, so you’ll need to supply your own compute power. And yes, there are leaderboards for the fastest times!

    btw: Event redesign coming next, thanks for the patience.

    ### Online docs

    - New section added to cover everything related to the new [Simulations](../simulations/index.md) feature.

    ### API updates

    - New endpoints for managing simulations at a replicant interface with /devices/:code/simulate.
    - Leaderboard list now has a type field to support nested items.
    - New public leaderboard endpoints for each simulation scenario at /leaderboards/simulations/:scenario.
    - New account endpoint for viewing previous simulation runs at /accounts/simulations.

    Time to put those bootstrap algorithms to the test! o7
11. v2.1.1 [◇](index.md#v2.1.1)

     8 July 2026

    A collection of little bugs and improvements. Thank you so much for all the feedback, you lovely bunch of bobs.

    ### Online docs

    - Mention the use of FTL devices to check trades remotely on the [Shop directory](../trading/directory/index.md) page.

    ### API updates

    - Fix bug to prevent compacting when an autofactory is printing, or an observatory is prospecting.
    - Fix trying to stow a device when the target host is elsewhere.
    - Add feature to view the current trade details at a shop when you have an FTL device in the shop’s system.
    - Fix mining drone bug where it wouldn’t switch to the next available site when a resource depletes.
    - Improved the response shape on /locations/:code to better match the /replicants/:code/scan results.
    - Added the spectral_type field to location output; stellar_class is deprecated, to be removed on the next breaking version.
    - Fix bug with activating propulsors while they are still attached to another device.
    - Add prospecting information to device status responses for galactic observatories.
    - Add device tags to the autofactory print status output.

    The galaxy is getting bigger. Reach for the stars! o7
12. v2.1.0 [◇](index.md#v2.1.0)

     6 July 2026

    Achievements are here. Over 200 achievements are now available across six categories. Half of them represent the variety of location events at planets in systems, but the rest are centred around your progression, exploration, travel, infrastructure and community involvement.

    Discovered a new star? Badge. Visited fifty systems? Badge. Diverted an asteroid? You better believe that’s a badge.

    ### Online docs

    - Updated the [Achievements](../api/accounts/achievements/index.md) page with new public endpoints.
    - Updated the [Scan Devices](../api/replicants/scan/devices/index.md) page to show new paging params.

    ### API updates

    - New public /achievements and /achievements/:key endpoints to browse all achievements and player unlock counts.
    - Added 120+ metrics-based achievements covering systems scanned, devices deployed, distance travelled, stars discovered, beacons placed, events completed, and more.
    - System device list is now paginated with cursor-based paging and filtering by owner replicant.
    - Prospect completion events now include the full list of discovered stars.
    - Performance improvement on the unread message counts that were causing 503s during a burst of traffic.
    - Added new prospect_no_fringe event to store details of prospect failures.

    NEEEEEEW ACHIEVEMENT! o7
13. v2.0.1 [◇](index.md#v2.0.1)

     5 July 2026

    Season two is underway. The megastructure at MIRFAKA was completed in record time, and the results from the DSTA (Deep Space Tracking Array) computations should be with us today. Where did those asteroids come from?

    Today’s patch is a collection of small quality-of-life fixes and upgrades.

    ### API updates

    - Improved the surge-hop routing algorithm to use the Kuiper as an intermediary waypoint when cruising to distant objects.
    - Replicants housed in a system hub’s cradle will auto-eject before destruction.
    - When an asteroid falls out of range, any devices still there will float to the Oort cloud.
    - Added the decommission command to all devices now, not just those with a cruise drive.
    - Fixed bug with asteroids spawning instantly after diversion. Now based on the original impact eta.
    - Device tags now support colon (:) and period (.) characters.
    - Added in_control_range to the device list, based on FTL connectivity.
    - Added completes_at field to scan and search responses.
    - Added quantity_mined field to mining drone status and related events.

    Asteroid hunters, we salute you! o7
14. v2.0.0 [◇](index.md#v2.0.0)

     1 July 2026

    Season two begins. It’s been 18 years since the Exodus Ark departed from Urcalis. Life is calm once more. Riker pushes forward with lifting humans off-planet. Bill has concerns - he’s working on something. Riker would like a hand, too.

    The season launches with the second instalment of our galactic story. In the interim years, replicants prospected outwards. Our star catalogue is now 88ly (up from 70ly) from Sol to the fringe.

    Space-faring species will start requesting more advanced devices, with a new fourth tier of events.

    ### Online docs

    - Story section added to the [homepage](https://replicant.space/#story).
    - New [Story Page](https://replicant.space/story) added to the site with the full timeline of the events so far.
    - Updates to the [Story](../concepts/story/index.md) API page, with practical impacts on players.

    ### API updates

    - Resource belts rebalanced - quantities, site scaling and search times.
    - Hull plate blueprint added to the starter pack.
    - New component-based blueprint designs.
    - Replicant device list is now paged.
    - More species.
    - More events.
    - More stars.
    - Oh, and three new [REDACTED] waiting to be [REDACTED].

    Hip hip, array! o7
15. v1.3.2 [◇](index.md#v1.3.2)

     28 June 2026

    Just a few minor bug fixes in today’s patch. A very amusing bug allowed players to keep deploying already-deployed FTL relays, causing rips in space and time.

    ### Online docs

    - Updated [Auofactories](../autofactories/index.md) page to mention that only travel/start_mining commands are currently supported for the oncomplete shape.

    ### API updates

    - Performance improvements on the new devices endpoint.
    - Fixed a bug with legacy FTL relay deployment mechanics.
    - New background task to fix any unfinished autofactory print jobs.

    The season is drawing to a close. All is calm. Nothing bad is coming. o7
16. v1.3.1 [◇](index.md#v1.3.1)

     26 June 2026

    The galactic observatory prospecting logic has been improved to be more useful in dense regions of space. You can now point the observatory in different directions if you want to explore along a non-outwards route. Perhaps you’re in a particularly sparse location and you’re pretty sure you saw a flicker of light in a dark zone.

    This update also comes with the long-awaited blueprint descriptions, which took forever to write. Let me know if you find typos or things that don’t make sense.

    ### Online docs

    - New [Galactic Observatories](../galactic-observatory/index.md) page with operating instructions.
    - Added description fields to the example on the [Blueprints](../concepts/blueprints/index.md) page.

    ### API updates

    - Improved prospecting to have a direction, and to focus on populating that hemisphere with new stars.
    - Allow fleet controllers to accept their current location as a travel destination, to route their devices to them.
    - Updated some NPC bobnet messages.
    - Added blueprint descriptions.

    Enjoy the newly added flavour of composite-conductive rare-earth shielded emitter exhaust relays with braided dissipation waffles! o7
17. v1.3.0 [◇](index.md#v1.3.0)

     25 June 2026

    Today’s new feature is the ability to move these large devices between systems with a new pair of commands.

    Large devices, like the autofactory, are actually more like a collection of fabrication and storage components in the same area, with drones sending parts back and forth. To shift them between systems, they need to be reduced to a suitable shape first. Some devices will take longer than others to compact, depending on the layout and the pre- and post-flight checks required to secure sensitive equipment

    ### Online docs

    - Updated [Moving Devices](../interstellar/moving-devices/index.md) to explain compact/unfurl workflow for large modular devices.
    - Updated [Device Retrieval](../api/devices/retrieve/index.md) with modular feature details and new status values for compacting/unfurling.
    - Updated [Commands](../api/devices/command/index.md) with new `modular` feature and `compact`/`unfurl` command reference entries.
    - Updated [Devices](../concepts/devices/index.md) overview to mention the modular transport path for the largest devices.
    - Updated [System Hubs](../system-hubs/index.md) with new relocation section covering compact/unfurl.
    - Updated [Autofactories](../autofactories/index.md) with new relocation section covering compact/unfurl and `tags` parameter for print jobs.
    - Updated [Blueprints](../concepts/blueprints/index.md) starter set to include `modular` feature on autofactories.
    - Updated [Civilisations](../concepts/civilisations/index.md) to show how to list active/completed events.

    ### API updates

    - Implemented new compact/unfurl commands for large modular devices to prep them for interstellar travel.
    - Fixed a series of bugs with stow/deploy logic to better support multiple-replicant scenarios.
    - Fixed a bug where webhook message events sent with type:event instead of type:message.
    - Attempting to stow a device that is repairing, or being repaired, will now stop the repair first.
    - Added ability to auto-tag devices from the print queue on autofactories.

    The very latest in flat-pack space station design. What a time to be alive! o7
18. v1.2.1 [◇](index.md#v1.2.1)

     21 June 2026

    Headlines for today’s update: better fleet controllers and a new endpoint for batched inventory data.

    There was a bug where location events were showing in location info, even though they hadn’t been discovered (planet scan, beacon at location, nearby system hub), which was causing some annoyance to players arriving without a survey drone.

    ### Online docs

    - Rewritten the [Fleet Controller](../ami/fleet-controller/index.md) page to provide details on fleet groups and travel routes.
    - Rewritten the [Location Inventory](../api/locations/inventory/index.md) to use the new batch inventory endpoint.

    ### API updates

    - Simplified how AMI fleet controllers work.
    - Added deployed_from and stowed_in to the deploy/stow responses.
    - Fixed bug where some AMI directives were not deactivated on completion.
    - New endpoint for browsing all your visible location inventories.
    - Fixed bug with not processing the print queue on travel arrival.
    - Fixed bug with undiscovered events appearing in location details.
    - Blueprint info now includes queue_size in the response.

    The Ark is saved! All incoming rocks have been diverted with a huge-scale collaborative operation. Great work replicants! o7
19. v1.2.0 [◇](index.md#v1.2.0)

     16 June 2026

    This has been requested several times, so the main addition in this release is proper filterable, batched device info lists.

    Although devices can already be listed by tag, there has been demand for full device status lists as well. This release adds support for paging through the full device list, with optional filtering by replicant code, device type, and location, including star-level location support.

    This is also a minor release, instead of a patch, due to the potential breaking change introduced by standardising the travel response shapes across different endpoints. Sorry if this breaks any user interfaces, but it was needed!

    ### Online docs

    - New page showing the [Device List](../api/devices/list/index.md) endpoint, for large scale batched device info.

    ### API updates

    - New GET /devices endpoint for batched device info.
    - Decommissioning a device that is attached, or has attachments will now separate itself first.
    - Fixed a bug with old diverted asteroids still showing in system scans.
    - Performance improvements on the /accounts/events endpoint.
    - Fixed the bug that was showing the first leg of travel instead of the full remaining route in the device info.
    - Ensured consistent travel response shapes across replicant and device info and command responses.
    - New final_arrives_at field in the travel schema, so you can track arrival for current leg and the whole journey.
    - Updated device list by tags endpoint to force supplied tag name to lowercase.
    - Error messages related to collecting resources when they don’t exist have been improved.
    - Resource inventories now show properly as integers and not those scary floats.
    - Added tags to device list responses.
    - Added device creation dates to all device info/list responses, so players can track their timelines.

    ### OpenAPI/Swagger updates

    - Added all of the missing device command schemas.
    - Added lots of missing params to endpoints that were previously undocumented.

    The Ark is under attack! Will the replicants save it in time? o7
20. v1.1.0 [◇](index.md#v1.1.0)

     12 June 2026

    The megastructure is complete, the season is ending, it’s been awesome. This is a little bit of a breaking change, hence the 1.1.0 version , but it’s been requested by quite a few of you. I’m hoping this is useful.

    ### Online docs

    - Updates to [Accounts and Replicants](../concepts/replicants/index.md) page to explain new two-tier scoping of devices.
    - Updates to [Account settings](../api/accounts/index.md) with examples of replicant ownership.

    ### API updates

    - A new two-tier permission model has been introduced for account/replicant-scoped device control.

    Have fun with the rocks! o7
21. v1.0.16 [◇](index.md#v1.0.16)

     11 June 2026

    Some players might have thought they saw humans out there in the galaxy, but they must be mistaken, since the only humans are currently on Earth. Coincidentally, there is a new species in the game called the Solari. Two unrelated facts. Ahem.

    Bill has also been working on subspace improvements to FTL comms. The relays are now open for all replicants to intercept bobnet chatter. Also, as long as there are relays at both ends of a travel route, you’ll receive in-flight messages too.

    ### Online docs

    - More details added to the [BobNet](../concepts/bobnet/index.md) page showing new channel list example and updates to receiving messages.

    ### API updates

    - Performance improvements on batch device listings.
    - Bug fix with location events and megastructures consuming devices that had already been used for something.
    - New “detonate” command now available for Impulse Chargee devices. Will become available as soon as the Ark is ready.
    - A new species has been discovered, the Solari, which looked very similar to humans at first glance.
    - Improved error handling on different scenarios for attempting replication.
    - New endpoint for listing the available BobNet channels with activity dates to help with client interfaces.
    - Replicants can now receive BobNet messages in any system that has a relay, regardless of relay ownership.
    - Travelling replicants will now intercept in-flight BobNet messages as long as they are travelling along active FTL network routes.
    - FTL relays can now be deployed anywhere, but only activated at L4/L5 Lagrange points.
    - Surge plates in taxi-mode are now shared by replicants under the same account, for assemble and ferry directives.

    This update was a real blast to work on. Heh. o7
22. v1.0.15 [◇](index.md#v1.0.15)

     8 June 2026

    Today’s patch has two primary focuses: surfacing AMI directive evaluations in the logs, and batched device status responses.

    ### Online docs

    - Added new [Tagging](../api/devices/tagging/index.md) page to explain how device tagging works.
    - Added new [Logging](../api/devices/logging/index.md) page to show examples of how to use the new device logs endpoint.
    - Updated [AMI Overview](../ami/index.md#directive_commands) page to show the more recent activate/deactivate commands for controlling directive evaluation.
    - Added more details on the difference between maintenance drones and service bots on the [Maintenance](../drones/maintenance/index.md) page.

    ### API updates

    - New device log endpoint at /devices/:code/logs for convenience.
    - AMI directive logging vastly improved, with logs now explaining all decisions made.
    - Implemented device tags as a core concept. You can now set any number of tags on devices to help stay organised.
    - New PATCH /devices/:code endpoint to set tags in a per-device configuration. Will be used for future device configuration.
    - Taxi-mode moved from a distinct ‘configure’ command to simply a ‘taxi’ tag on surge plates.
    - New GET /devices/tag/:tag endpoint to receive batched device info, with cursor/limit params for paging.
    - Fixed a few misc swagger-related inaccuracies.
    - Introduced device command and ami directive request schemas for deeper validation.
    - Simplified attachment logic. Eliminated attachment loops. Only allows three layers deep.
    - Diversion of asteroids now uses the “diversion” feature on a device, no longer specific to just propulsors.
    - Always show activate/deactivate commands as available on AMI devices.

    Megastructure progress at 27%. There’s a chance we might just pull this off! o7
23. v1.0.14 [◇](index.md#v1.0.14)

     7 June 2026

    Three big things from today’s patch:

    - Asteroid diversion is much improved, with persistent progress from all participating accounts.
    - Location events were seriously bugged when there were multiple paths to resolve - big overhaul.
    - The Service bot bugs had rendered it pretty useless. The ‘service’ directive now allows it to hot-repair devices properly.

    ### Online docs

    - Updated the [Asteroids](../api/locations/asteroids/index.md) page with a new example response and improved explanations.
    - Clarified on the [BobNet](../concepts/bobnet/index.md) that relays will receive messages anywhere on th subspace backchannels.

    ### API updates

    - Fixed an issue with emails not being deleted on the the account wipe process.
    - Added Access-Control-Max-Age header to preflight responses for CORS efficiency.
    - Location events fixed to support multiple resolution options.
    - Location events now include the resolution options, formatted nicely, in the event description.
    - Minor breaking change to flag: location event criteria has changed from a dict to a list of resolution options.
    - Overhauled asteroid diversion to track progress properly, with a new impact_likelihood value.
    - Per-planet resource inventories added to /locations/:star response.
    - Per-moon resource inventories added to /locations/:planet response.
    - Shop details added to the system scan response.
    - You can no longer attach a matrix directly to a carrier, they need to be protected by something.
    - Fixed premature system hub achievement.
    - AMI device status now shows as paused when the directive is paused.
    - Fix bug with location still showing after being stowed.
    - Fixed issues with service bot and maintenance drone interactions.
    - Fixed bug with attaching to devices that are also carriers.

    The galaxy has now been 20% explored. That’s not nearly enough! o7
24. v1.0.13 [◇](index.md#v1.0.13)

     3 June 2026

    Big collection of bug fixes, minor improvements and consistency updates under the hood today.

    ### Online docs

    - Fixed incorrect paging fields in [BobNet](../concepts/bobnet/index.md) catchup message response example.
    - Added OpenGraph image to the website, for previews when sharing on socials.
    - Standardised examples in the [Device commands](../api/devices/command/index.md) list to use “targets” instead of “device/devices”.
    - Updated [AMI Overview](../ami/index.md) page to mention the assemble directive using taxi plates.
    - Updated [Shop configuration](../trading/configuration/index.md) page to mention optional description/announcement fields.
    - Fixed typo on [Account reputation](../api/accounts/reputation/index.md) endpoint - it should be /accounts/reputation.

    ### API updates

    - Decomissioned devices will now return a 404 if you attempt to query or command them.
    - System scan results will now return replicants in a list, instead of a map, to match the swagger docs.
    - Mining cycle times now return as an integer, instead of a float, to match the docs.
    - BobNet trade announcements will now be more spread out, and less frequent.
    - The assemble directive has been upgraded to now use taxi plates where available.
    - Fix bug with BobNet messages coming from the relay-owner when using replicant message endpoint.
    - Added more details to the BobNet message responses, to assist with chat interface design.
    - Description and announcement fields are now optional fields in AMI Trade Controller configuration.
    - Dry-run travel route previews are now possible while your vessel is mining or printing.
    - Cleaned up the use of target/targets/device/devices on several endpoints, all should be accepted but examples use “targets” now.
    - Removed an old device-based rate limit that was confusing some tests.
    - The cargo lifter device can now actually be used as a carrier.
    - Using the wrong HTTP method on an endpoint will now list all the available methods for that route.
    - Tightened up the location wipe on stowing devices.
    - New achievements for asteroid diversion - whether you’re saving lives or rocks.

    Big thanks for all the bug reports and feature suggestions from everyone so far! o7
25. v1.0.12 [◇](index.md#v1.0.12)

     2 June 2026

    Today’s change is a cleanup patch. It has been undergoing regression tests for a few days, but I wanted to get it out. Please report any bugs or inaccuracies.

    Notification settings can now be configured separately for webhooks and email. This puts the configuration in players’ hands: for example, you can choose to receive emails for newly found location events and trade transactions, while receiving everything else via webhook. Master toggle to disable email and/or webhook.

    Upon release, this patch will automatically disable email notifications for all players. This is deliberate, to avoid spamming you. Please re-enable the email notifications you care about most. Emails are now a little prettier (the text/plain versions should also look cleaner in CLI mail clients)

    ### Online docs

    - Updated [Account settings](../api/accounts/index.md) page with email-change example, new preferences shape, and the new “hub” category.

    ### API updates

    - New “email” field added to PATCH /accounts/me for players to change the email address on their account. Triggers a verification process.
    - Players will no longer receive gameplay emails if their email notification setting is off. Emails for verification processes (registration, email change, account wipe) will still be sent.
    - Notification system overhauled to properly respect the per-category email/webhook account settings.
    - Fix added for players who occassionally find a device (or themselves!) out of command range. The underlying fix is still in progress, but your devices should self-correct when breaking.

    There are now 60 replicants roaming around the galaxy! o7
26. v1.0.11 [◇](index.md#v1.0.11)

     31 May 2026

    As a variety of custom BobNet chat clients are being created, it was time to ensure our replicants have a personality. And a plan!

    ### Online docs

    - Added new [Multiplayer](../concepts/multiplayer/index.md) concept page to explain the interactions possible.
    - Renamed the “Replicant details” page to [Your details](../api/replicants/index.md).
    - Added new [Directory](../api/replicants/directory/index.md) page to explain how to lookup other replicants.
    - Renamed the old replicant “Rename” page to [Configure](../api/replicants/configure/index.md) and added instructions on how to configure your public profile fields.
    - Updated the replicant [Print device](../api/replicants/print/index.md) page to include instructions for clearing the queue or cancelling a print.

    ### API updates

    - Carried out a review of swagger docs and fixed a range of inaccurate fields and examples. More still to do.
    - Added new replicant directory endpoint at GET /replicants.
    - Added new public profile fields for replicants to save with PATCH /replicants/:code.
    - Wrote descriptions for the NPC profiles.

    Megastructure progress is happening! Go replicants! o7
27. v1.0.10 [◇](index.md#v1.0.10)

     30 May 2026

    Biggest change is a round of fixes and refactors to the AMI mining controller. The overheating issue has been diagnosed now, and will appear in your event logs. Using one controller across multiple sites with lots of drones will overload the AMI’s compute core, resulting in delayed directive evaluation and increased damage.

    This is a good time to point out the difference between maintenance drones and service bots.

    ### Online docs

    - Updated pagination params on [BobNet](../concepts/bobnet/index.md), [Beacons](../ftl-beacons/index.md), [Messages](../api/accounts/messages/index.md), and [Event log](../api/replicants/events/index.md) pages.
    - Added location filter parameter to [Replicant devices](../api/replicants/devices/index.md) page.
    - Added entry_point to [Nearest stars](../api/replicants/stars/index.md) response examples.
    - Details on multi-location mining directive behaviour added to the [Mining controller](../ami/mining-controller/index.md) page.
    - Explain difference between maintenance drone and survey bot on the [Maintenance page](../drones/maintenance/index.md) page.

    ### API updates

    - Fix bug with mining controllers running multiple locations.
    - AMI mining controllers will now overheat due to heavy multitasking.
    - Riker’s system hub exploded. He’s now maintaining it daily.
    - System Hubs now degrade from the Lagrange point to the planet’s orbit.
    - Fix bug in system scan award not being issued.
    - New achievement for first BobNet message (starting from now!)
    - Riker will greet new BobNet replicants on #general.
    - Replicants will auto-subscribe to a channel when posting a BobNet message there.
    - Paging options standardised across all message/event/log endpoints to use cursor/limit/latest.
    - Star results now shows the entry_point, where known.
    - The location overview on GET /locations now includes the location_event count.
    - Access-Control-Expose-Headers configured for web-based clients to view our headers.
    - Trying to command a survey drone to scan when already scanning results in HTTP 409.
    - Active location events are now included in the scan reports.
    - Replicant device lists can now be filtered by location.

    Thanks for your patience with those janky AMI controllers folks. Focus now shifting to the other ones. o7
28. v1.0.9 [◇](index.md#v1.0.9)

     29 May 2026

    Today’s patch is focused on a range of player reports regarding device travel, replicant host control and AMI coordination.

    ### API updates

    - Cleaned up activating/deactivating propulsor error handling.
    - Fix bug with the recall command trying to stow unstowable devices and leaving them broken.
    - Fix cross-system cruise recall trick where AMI devices would break the laws of physics.
    - Fix in-system AMI recall behaviour where devices would use the wrong drive
    - AMI controller directives now have the devices use their own travel route computation, rather than issuing direct travel commands.
    - Location scans have been adjusted to show your own account knowledge, including knowledge of life.
    - A variety of interesting issues related to replicant matrix cradling and travel have been fixed.
    - Strapping a surge plate on the back of a matrix container and sending yourself off into the void will now grant a special achievement (thanks Tory!).
29. v1.0.8 [◇](index.md#v1.0.8)

     28 May 2026

    Two fun features added to today’s patch: travel route previews and custom system hub greetings!

    ### Online docs

    - Updated [Replicant Travel](../api/replicants/travel/index.md) and [Device Commands](../api/devices/command/index.md) pages to show the new travel preview feature.
    - Added custom welcome message details to the [System Hubs](../system-hubs/index.md) page.
    - Updated the [Webhooks page](../api/accounts/webhook/index.md) to show an example of the new GET endpoint.
    - Added example of checking the star catalogue for a single star on the [Nearest Stars](../api/replicants/stars/index.md) page.
    - Added capacities to the example response on the [Blueprints](../concepts/blueprints/index.md) page.

    ### API updates

    - New dry run parameter added to the travel commands, to view the planned travel route
    - Fixed rare bug on travel arrival where the device loses its location.
    - Removed autofactory message when cancelling a replicant print job.
    - Added the “assemble” command to AMI devices. All devices should now list all commands they accept.
    - Replicant now accepts the “clear_queue” command on the /replicants/{code}/print endpoint, instead of needing to clear on the vessel device directly.
    - Improved error message when attempting to travel a stowed device.
    - All AMI devices now include the “available_directives” in their details response.
    - New endpoint to see star catalogue data for unscanned star designations.
    - Improved error message on attempting to stow a matrix somewhere it shouldn’t go.
    - Added stow/cargo/attach capacities to blueprint response.
    - Fixed missing hosted_device_code while travelling.
    - Stowing a survey drone will now stop it tracking the resource site properly.
30. v1.0.7 [◇](index.md#v1.0.7)

     27 May 2026

    The big change from today’s update is an overhaul of the system scan logic. Up to this point, system bodies were showing as scanned if anyone had scanned them. This was leading to inconsistent AMI survey behaviour when encountering previously scanned systems (by other players). This was also confusing some location request lookups.

    ### Online docs

    - Updated the values on the [Rate Limits](../rate-limits/index.md) page.
    - Updated the [Replicate](../api/replicants/replicate/index.md) page example to use the matrix code instead of the cradle.
    - Fixed broken link and added more details to the [Replicant Cloning](../cloning/index.md) page.
    - Fixed typo on [Fleet Controller](../ami/fleet-controller/index.md) page.
    - Added details on accessing BobNet chat logs to the [BobNet](../concepts/bobnet/index.md) page.
    - New page talking about our current [NPC replicants](../concepts/npcs/index.md).

    ### API updates

    - Rate limits have been relaxed, allowing double the requests.
    - System scan logic has been overhauled, location info is available remotely if you had previously scanned a system.
    - Old asteroid impacts are removed from the system summary after a day.
    - Replicant lookups from other accounts no longer show detailed replicant details, just the name and code.
    - The “replicate” command has been moved from the cradle to the matrix.
    - Bill has reconfigured his FTL beacons to be auditable by players.
    - FTL beacons now report the “audit” feature, powering the audit endpoint.
    - FTL beacons and System Hubs now report the “comms” feature. This is what powers location event notifications.
    - New endpoint for catching up on BobNet messages from a relay.
    - Star lookups are now possible for travelling replicants, based on their origin location.
    - Star listings now include the current star.
31. v1.0.6 [◇](index.md#v1.0.6)

     24 May 2026

    ### Online docs

    - Updated [System Hub](../system-hubs/index.md) docs to include activation instructions
    - Improved the [AMI Overview](../ami/index.md) explanations to include more details on the launch process
    - Added the missing `belt_search` directive to [Survey Controller](../ami/survey-controller/index.md) details
    - Rewrote the old Surge Plate page to be a new [Moving devices](../interstellar/moving-devices/index.md) page
    - Added example of the new FTL network lookup on [FTL Relays](../ftl-relays/index.md)

    ### API updates

    - Fix a system entry point definition issue with sending survey drones into unscanned systems
    - Fix bug with FTL relays and system hubs not allowing remote device info; they were supposed to work like beacons
    - Overhauled the haulage situation with the ferry directive. Surge-capable transport devices will no longer wait for surge plates
    - The `message` command now shows as available for relay-capable devices
    - Blueprints now show an accurate print time for devices
    - The attach command is now stricter when you attempt to use it on larger devices
    - Device lists now include an additional `in_control_range` field to show if you can control it remotely
    - Surge plates now have the distinct `taxi` feature, which surfaces the `configure` command. There is a future where we’ll be able to configure a variety of different device settings, with taxi-mode just being the first for now
    - New network endpoint available for relay devices, to inspect the current FTL network status with a list of connected systems
    - The `activate` and `deactivate` commands are now fully implemented, allowing relay deactivation, and fixing its appearance on the maintenance drone command list

    Thanks again to the current playerbase for being so interactive, each patch is improving the game experience for us all! o7
32. v1.0.5 [◇](index.md#v1.0.5)

     23 May 2026

    One big change in today’s patch release: the new Civilisations page provides more details on interacting with species as we explore the galaxy.

    [https://replicant.space/docs/concepts/civilisations/](../concepts/civilisations/index.md)

    ### Online docs

    - Added device travel example to the drives page
    - Added missing 9-slot surge carrier device to the surge plates page
    - Rewritten surge plate page to explain Taxi Mode better
    - Added new page on species and location events for discovering new blueprints
    - Updated account registration page to include name restriction

    ### API updates

    - Added missing hosted_device_code to mining replicant’s info response
    - Refactored FTL network logic significantly to better support cross-system AMI control
    - Added stricter validation on account/replicant names
    - System scan output updates with real moon counts when planets are scanned, instead of the early estimates
33. v1.0.4 [◇](index.md#v1.0.4)

     22 May 2026

    ### Online docs

    - Fixed a mistake in the Account Settings example
    - Tweaked mobile responsive layout to show docs in the nav
    - Added more details and example code to the Webhooks page
    - Added a full table of all commands and params to the Device Commands page

    ### API updates

    - Fixed the “via” param not working properly on manual travel routes
    - Fixed a bug where a server reload would quadruplicate any ongoing print jobs
34. v1.0.3 [◇](index.md#v1.0.3)

     21 May 2026

    ### Online docs

    - Corrected the set_directive usage across all AMI examples
    - Rewrote asteroid impact doc to explain propulsor usage and diversion better
    - Added feedback page to explain the new endpoint usage

    ### API updates

    - Updated NPC chatter on BobNet to reflect current blueprint collection
    - Included the search command in survey drone details, and show as searching in device status
    - Added new feedback endpoint for bug/typo/idea requests from players
35. v1.0.2 [◇](index.md#v1.0.2)

     19 May 2026

    ### Online docs

    - Fixed incorrect replicant print example (thanks Ombre)
    - Added content-type headers to all POST examples (thanks Ombre)
    - Rewrote the messages page to include details on unread message counts, and examples of marking messages as read (thanks FrikiGeekFran)
    - Added a new page to show the account details endpoint (thanks Seveen)
    - Added more details on how asteroid belt mining works to the mining drone page (thanks Seveen)

    ### API updates

    - Added support for gzipped request payloads (thanks FrikiGeekFran)
    - Fixed a bug with the way that a certain NPC travels across the galaxy each day.
    - Ensured all scan responses now use `location_type` as standard. The old `type` response field is deprecated. (thanks Seveen)
    - Standardised the response shape for salvage/belt mining. Old `belt` and `designation` response fields deprecated. (can’t find who reported this sorry!)
    - Added better response for trailing slash on endpoints rather than the 404 (thanks Ombre)
36. v1.0.1 [◇](index.md#v1.0.1)

     18 May 2026

    ### Wipe clean

    New self-service endpoint at DELETE /accounts/me for nuking everything tied to your account - your replicants, devices, inventory, achievements, messages, etc. To keep things secure, when you trigger the endpoint, it will email you a confirmation with a button to click.

    ### Clearer rate limits

    New rate limits page added to spell out the per-endpoint and global buckets, and with an example of what happens when you hit it, so you can configure your client to back off cleanly.
37. v1.0.0 [◇](index.md#v1.0.0)

     17 May 2026

    ### Launch!

    Welcoming new replicants to help the exodus project. Season one ends summer 2026.
