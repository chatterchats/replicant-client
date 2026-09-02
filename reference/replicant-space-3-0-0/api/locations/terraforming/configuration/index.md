---
title: "Configuration"
source_url: "https://replicant.space/docs/api/locations/terraforming/configuration/"
crawled_at: "2026-09-02T20:03:41.355025+00:00"
---

Terraforming

# Configuration

Fine-tune your terraforming devices. Adjust strength to control the rate of change, and set direction on reversible devices to invert their effects.

## Endpoint

`PATCH /v1/devices/{code}`

Terraforming devices are configured through the standard device PATCH endpoint. Pass a `configuration.settings` object to update strength, direction, or both. Settings take effect on the next tick.

## Settings

| Name | Type | Description |
| --- | --- | --- |
| `strength` | number | Operating strength from `0.0` to `1.0`. At full strength the device runs at its listed rate. Lower values reduce the effect proportionally. Defaults to `1.0`. |
| `direction` | string | `increase` or `decrease`. Only applies to reversible devices: atmo processor, gas separator, and aquifer tap. Non-reversible devices ignore this field. Defaults to `increase`. |

## Example

PATCH /v1/devices/{code}   200 OK

```
$ curl -X PATCH https://api.replicant.space/v1/devices/7A3F1B2E \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"configuration": {"settings": {"direction": "increase", "strength": 0.75}}}'
```

response response

```
{
  "device_code": "7A3F1B2E",
  "device_type": "atmo_processor",
  "status": "idle",
  "configuration": {
    "settings": {
      "direction": "increase",
      "strength": 0.75
    }
  }
}
```

## Reversing direction

Three devices support the *decrease* direction. When reversed, their primary effect and side effects invert:

- **Atmo Processor** - normally increases pressure and temperature. Reversed: decreases both.
- **Gas Separator** - normally increases oxygen and decreases toxicity. Reversed: decreases oxygen and increases toxicity.
- **Aquifer Tap** - normally increases hydrosphere and pressure. Reversed: drains surface water and reduces pressure.

Setting *direction* on a non-reversible device (orbital mirror, thermal lance, etc.) has no effect. The device will continue operating in its default direction.

PATCH /v1/devices/{code}   200 OK

```
# reverse a gas separator to decrease oxygen
$ curl -X PATCH https://api.replicant.space/v1/devices/9C2D4E6F \
    -H "Authorization: Bearer $API_KEY" \
    -H "Content-Type: application/json" \
    -d '{"configuration": {"settings": {"direction": "decrease", "strength": 0.3}}}'
```

response response

```
{
  "device_code": "9C2D4E6F",
  "device_type": "gas_separator",
  "status": "idle",
  "configuration": {
    "settings": {
      "direction": "decrease",
      "strength": 0.3
    }
  }
}
```

## Partial updates

You can update strength and direction independently. Sending only *strength* leaves the current direction unchanged, and vice versa. To reset a device to defaults, set *strength* to `1.0` and *direction* to `increase`.

## Effective rate

The actual rate of a device per tick is its base rate multiplied by the strength value. An atmo processor at strength `0.5` produces pressure +0.04/tick instead of the listed +0.08/tick. Side effects scale the same way.

Strength stacks with diminishing returns from multiple devices of the same type. If you have two atmo processors both at strength `0.75`, the effective rate per device is further reduced by the stacking formula.
