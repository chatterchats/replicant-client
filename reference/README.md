# Replicant Space reference corpus

Pinned snapshots of the Replicant Space rendered documentation and OpenAPI
document. This is the contract the client is verified against.

## Read-only, byte-for-byte

Each snapshot is pinned by its `manifest.json` and **must not be reformatted or
hand-edited**. The repository-level `.prettierignore` excludes this directory
so that a manual `prettier . --write` cannot alter verified reference material.
Do not defeat that exclusion.

Refresh only through:

```sh
make docs-reference-sync
```

which runs `replicant-docs-crawler/crawl_replicant_docs.py --refresh`.

## Which snapshot is authoritative

The **highest semantic version** under `reference/replicant-space-*` is the
current contract. Tooling resolves it automatically via
`scripts/reference_snapshot.py`; nothing should hard-code a version.

Older snapshots are retained for regression work — `tests/contract_2_3_3.rs`,
`contract_2_4_0.rs`, `contract_2_5_0.rs`, and `contract_2_5_1.rs` each pin
behaviour introduced by their version.

The active pin, including sha256 digests of both the documentation manifest and
the OpenAPI document, is recorded in the root `Cargo.toml` under
`[package.metadata.replicant-space]`. Those digests are what make a silent
corpus edit detectable.

## Snapshot layout

```
replicant-space-<version>/
  INDEX.md          generated page index with source URL and crawl timestamp
  index.md          the docs landing page
  manifest.json     per-file digests — the pin
  openapi.json      the OpenAPI document
  crawl-errors.json pages the crawler could not retrieve
  changelog/        per-release notes; the fastest way to find version deltas
  api/              endpoint reference, grouped by tag
  concepts/         domain model prose
  ami/ drones/ trading/ simulations/ ...
  _assets/          images referenced by the pages
  postman/          exported collection
```

`changelog/index.md` is the highest-value file for version work: it lists doc
changes and API updates per release, which is usually enough to scope a
contract change without diffing whole trees.

## Precedence

`openapi.json` outranks the rendered documentation for schema shape. Two
exceptions:

- Rendered deprecation asides override a missing OpenAPI `deprecated` flag —
  see `policy/contract-metadata.json`.
- Operations documented but absent from the spec are declared in
  `policy/documented-operation-deltas.json`.

Any *other* disagreement between the two is a finding to record, not a choice
to make silently.

## Crawler

`replicant-docs-crawler/` holds the crawler that produces these snapshots. It
is tooling, not contract material, and is excluded from the published package
by `scripts/package_contents_check.py`.
