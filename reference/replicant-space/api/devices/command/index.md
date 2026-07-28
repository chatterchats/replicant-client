---
title: "Commands"
source_url: "https://replicant.space/docs/api/devices/command/"
crawled_at: "2026-07-28T00:53:10.684535+00:00"
---

API · Devices

# Commands

Drive your devices around. The set of valid commands depends on device type - see each drone or controller page for the specifics.

## Endpoint

`POST /v1/devices/{code}`

## Body parameters

| Name | Type | Description |
| --- | --- | --- |
| `command` | string | Command name. Type-specific. |
| `...args` | any | Each command takes its own arguments inline alongside `command`. |

## Example

POST /v1/devices/{code}   200 OK

```
# stow a deployed drone back into its host vessel
$ curl -X POST https://api.replicant.space/v1/devices/40ED8A3E \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"command": "stow"}'
```

response response

```
{
  "device_code": "40ED8A3E",
  "status": "stowed"
}
```

## Command reference

Each device has a specific list of features available. These features determine what commands it can process. Additional parameters are command-specific. For example, the travel command will require a `destination: SOL-3`, and the start_mining command will want `resource_type: carbon` for example.

Full details on the additional parameters for each command can be found in the documentation for the different device types.

| Feature | Commands |
| --- | --- |
| `cruise` | `travel`, `recall`, `decommission` |
| `surge` | `travel` |
| `stow` | `deploy`, `stow` |
| `attach` | `attach`, `detach` |
| `system_scan` | `system_scan` |
| `survey` | `scan`, `search` |
| `census` | `stellar_census` |
| `prospect` | `prospect` |
| `mine` | `start_mining`, `retarget` |
| `transport` | `collect_resources`, `deposit_resources` |
| `print` | `enqueue_print`, `dequeue_print`, `clear_queue` |
| `repair` | `repair` |
| `cradle` | `replicate` |
| `ami` | `adopt`, `release`, `set_directive`, `clear_directive`, `launch`, `withdraw`, `activate` |
| `modular` | `compact`, `unfurl` |
| `relay` | `activate` |

## Universal commands

Every device, regardless of features, accepts the following:

- `change_owner` - reassign the device to another replicant under your account.
- `deactivate` - stop whatever the device is currently doing (mining, printing, travel, relaying, etc).

## Custom commands

Some device types add their own commands on top of the feature list:

- `propulsor` - `activate`.
- [`system_hub`](../../../system-hubs/index.md) - `set_entry_point`, `rename`.

## Command details

| Command | Details |
| --- | --- |
| *activate* | Activate a deployed device. Most commonly for activating FTL relays, but also for turning on propulsors when diverting. |
| *adopt* | Instruct an AMI controller to adopt another device.  `{command: adopt, targets: [ ... ]}` |
| *attach* | Instruct a device to connect to another device. Commonly used for moving devices between systems with surge plates.  `{command: attach, targets: [ ... ]}` |
| *change_owner* | Reassign a device from one replicant to another (must both be under the same account).  `{command: change_owner, target: <code>}` |
| *clear_directive* | Reset an AMI controller's directive. This will stop the controller from evaluating and issuing new commands to the devices it controls. |
| *compact* | Start the process of collapsing a large modular device into a transportable state so it can be attached to a surge carrier. The device is offline until unfurled. The cruise drive will still work for in-system travel while compacted. |
| *clear_queue* | Remove any items from a 3D printer's queue. Primarily used on autofactories, this can also be issued to any device with the print feature. Useful if you try to print something yourself, then realise you're short of resources and change your mind. |
| *collect_resources* | Instruct a transport device to take resources from the current location's inventory.  `{command: collect_resources, resources: { silicates: 50, rares: 30, ... }}` |
| *deactivate* | Stop a device from printing, mining, scanning, travelling, etc. Most async commands can be stopped with this. |
| *decommission* | Send a device to the nearest autofactory in the current system, where it will be converted into resources. You'll learn the blueprint for the device, if you didn't know it already. The trading system allows you to purchase devices from other players and NPCs. |
| *deploy* | Deploy a stowed device to the current location. |
| *deposit_resources* | Instruct a device with cargo to unload at its location. Skip the resources param to empty everything.  `{command: deposit_resources, resources: { silicates: 50, rares: 30, ... }}` |
| *dequeue_print* | Remove a specific item from a device's print queue. Supply the zero-indexed position in the list.  `{command: dequeue_print, index: 2}` |
| *detach* | Disconnect one device from another. Skip the devices param to detach everything connected to it.  `{command: detach, targets: [ ... ]}` |
| *enqueue_print* | Add an item to the print queue, with optional auto-adoption controller code and on-complete command.  `{command: enqueue_print, device_type: mining_drone, quantity: 1, controller: <code>, oncomplete: <command>}` |
| *launch* | Deploy an AMI controller to your current location. This will also activate the AMI directive and deploy all controlled devices. Fly to a belt and launch your mining swarm with one command. |
| *prospect* | Discover new stars beyond the galaxy catalogue. Deploy a [galactic observatory](../../../galactic-observatory/index.md) at the fringe and prospect outward, or specify a direction to search in any direction where stars look sparse.  `{command: prospect, direction: [x, y, z]}` |
| *recall* | Instruct a device to come home and stow itself. If you fly away while it's returning to you, it will continue to chase you around the system like a wasp in summer. |
| *release* | Tell an AMI controller to stop controlling one or more devices. Opposite of the adopt command.  `{command: release, targets: [ ... ]}` |
| *rename* | Once you're in charge of a system with your own system_hub running, you can ask it to update the star catalogue to change the names of things. Valid target designations are stars, planets and moons.  `{command: rename, designation: SOL-3, new_name: Earth}` |
| *repair* | Ask a maintenance drone to repair a specific device, when it's not running a directive.  `{command: repair, target: <code>}` |
| *replicate* | Once you have an empty replicant matrix at your current location, issue this command to your own internal matrix, and it will initiate a tight-beam replication process. Similar to a teleport, except we don't destroy the original, avoiding the existential crisis.  `{command: replicate, target: <code>}` |
| *retarget* | Instruct a mining drone to switch focus to a new resource, without stopping the current operation.  `{command: retarget, resource_type: <code>}` |
| *scan* | Conduct a scan of the current location with your survey drone (or other survey-powered devices). Results will be posted to your webhook, or found in your event log later on. It's faster to scan moons than planets. Scanning a belt will also tell you the exact number of resources available, instead of the percentage view you get from the location endpoints. |
| *search* | Search an asteroid belt, looking for a new resource site to mine. Once discovered, the device will track the site continuously while mining. |
| *set_directive* | Configure an AMI device directive. Once configured, the device will periodically check and issue commands to adopted devices to achieve the goal.  `{command: set_directive, directive: ..., configuration: { ... }}` |
| *set_entry_point* | Instruct a system hub to update the star catalogue. Sets the surge entry point of a star system to the hub's current location. |
| *start_mining* | Tell a mining drone to start mining for a specific resource.  `{command: start_mining, resource_type: <code>}` |
| *stellar_census* | Check the star catalogue for nearby stars. This is the device command that runs when you call the more convenient /replicants/<code>/stars. You can call it manually on your vessel device if you prefer, the response will be the same.  `{command: stellar_census, page: 1, per_page: 10}` |
| *stow* | Place a device in the hold of another device. If you don't provide the target param, the device will attempt to stow in its replicant owner's vessel. The target param allows you to stow devices in other devices, where possible.  `{command: stow, target: <code>}` |
| *system_scan* | Carry out a system scan. This is the device command your vessel actually runs when you call /replicants/<code>/scan. Handy if you have an unmanned (unreplicantened?) vessel in a different (but FTL-relay-connected) system and want to check something. |
| *unfurl* | Start the process of reassembling a previously compacted modular device. |
| *travel* | Instruct a device to go somewhere. If the device has a surge drive, this can be a different star system. The internal nav chip will optimise the route for you, configuring surge/cruise steps as needed. Use the "via" param if you would prefer to set your own route. Send a device on a round-trip loop, a tour of the system planets, or on a long, slow, lonely cruise to the Oort cloud if you wish.  `{command: travel, destination: <code>, via: [ ... ]}`   Disable the nav chip with `via:direct`   Pass `dry_run:true` to preview the planned route without departing. |
| *withdraw* | Command your AMI device to recall all its adopted devices back to the replicant vessel, then to stow itself. Make sure you have enough space. |

````
