# replicant-bootstrap-planner

Pure planning primitives for an autonomous regional bootstrap mission. It
defines the ark profile, expands complete mining-site device requirements,
calculates Surge Carrier capacity, produces bounded reservation tags, and
selects the nearest distinct dense-belt systems after a survey.

The crate performs no network or filesystem I/O. The executable workflow is
in `replicant-cli bootstrap`.

```sh
cargo test -p replicant-bootstrap-planner
```
