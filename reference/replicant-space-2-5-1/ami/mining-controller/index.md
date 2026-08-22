---
title: "AMI Mining Controller"
source_url: "https://replicant.space/docs/ami/mining-controller/"
crawled_at: "2026-08-22T22:43:49.701518+00:00"
---

AMI Controllers

# AMI Mining Controller

Decide on the resource gathering strategy that works best for you, configure your controller, and it will set the drones to work.

## Directives

- `gather_resources` - mine a specific amount of each resource as configured.
- `gather_evenly` - attempt to maintain the same amount of each resource at the location.
- `maintain_ratios` - maintain a per-resource ratio across your stockpiles.
- `deplete_smallest` - gather whatever there is least of first.
- `gather_salvage` - send the fleet to a salvage site, mine it to depletion, optionally recall.

## Configuring each directive

### Gather Resources

Perhaps you only want enough to print a specific device. Configure your directive to mine precisely what you need, then print and bug out. Once the target is met, the controller will deactivate and wait for the next command.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/MC91FF22 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "gather_resources",
      "configuration": {
        "carbon": 10,
        "conductive": 30,
        "rares": 5,
        "silicates": 15,
        "structural": 60
      }
    }'
```

### Gather Evenly

The simplest directive. Assign the available drones evenly across the available resources.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/MC91FF22 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "gather_evenly"
    }'
```

### Maintain Ratios

Specify the ratio between a set of resources and the drones will try to keep the amounts relative.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/MC91FF22 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "maintain_ratios",
      "configuration": {
        "structural": 0.5,
        "conductive": 0.25,
        "silicates": 0.15,
        "rares": 0.1
      }
    }'
```

### Deplete Smallest

Figure out which resource is scarcest at the location, assign the majority of the available drones to mining that resource. The rest focus on the next-least available. This directive is useful for quickly reducing the number of different resources available, so that the plentiful are mined last.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/MC91FF22 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "deplete_smallest"
    }'
```

### Gather Salvage

Useful when a system scan uncovers several interesting bits of wreckage and you want to simplify your admin work. The controller dispatches every adopted mining drone to the salvage location, works it to depletion, then - if `recall` is set - sends the fleet back to your replicant vessel to be stowed.

POST /v1/devices/{code}   200 OK

```
$ curl -X POST https://api.replicant.space/v1/devices/MC91FF22 \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{
      "command": "set_directive",
      "directive": "gather_salvage",
      "configuration": {
        "location": "SOL-3-L4",
        "recall": true
      }
    }'
```

## How directives work

Every tick, the controller measures how much of each resource is in the inventory, compares this with what is available in resource sites, then retargets the available mining drones the best way to satisfy the directive configuration.

For example, if you launch the controller with 12 adopted mining drones at a new belt with the `gather_evenly` directive, the controller will assign 2 drones to each resource. When one resource is depleted, those 2 drones will be assigned randomly to the other available resources. This continues until the site is depleted.

## Controller Location

Your mining controller doesn't need to be in the same location as your drones. You can spread the drones across multiple sites if the directive makes sense to do so. The `gather_salvage` directive moves all available mining drones to a specific location. THe `gather_resources` directive will fail if the drones aren't all at the same location.

This means you can power many different sites with a single controller. High levels of multi-tasking can introduce overheating and unreliable behaviour. You've been warned.
