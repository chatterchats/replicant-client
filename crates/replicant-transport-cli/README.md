# replicant-transport-cli

Point-to-point delivery CLI backed by `replicant-transport`.

```text
replicant-transport --origin SCEPTURUM \
  --devices 36 exotic_matter_injector \
  --carrier 1 mobile_fleet \
  --destination TWAFFY-OBJ-1

replicant-transport --origin SCEPTURUM \
  --device-tag twaffy-obj-1 \
  --carrier 1 mobile_fleet \
  --destination TWAFFY-OBJ-1

replicant-transport --origin SCEPTURUM-BELT-1 \
  --rares 400 --volatiles 100 \
  --carrier cargo_freighter \
  --destination TWAFFY-OBJ-1
```
