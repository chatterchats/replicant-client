# Replicant Space Documentation Crawler

Mirrors `https://replicant.space/docs/` and the changelog into a local Markdown tree, and saves the live OpenAPI document alongside it.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r reference/replicant-docs-crawler/requirements.txt
```

## Run

From the repository root, the normal update is:

```bash
make docs-reference-sync
```

The crawler detects the highest `vX.Y.Z` release in the live changelog and writes that snapshot to a sibling directory such as:

```text
reference/replicant-space-2-5-1/
```

Older version directories are left untouched, so the repository keeps historical contract snapshots for regression work.

To run the crawler directly:

```bash
python3 reference/replicant-docs-crawler/crawl_replicant_docs.py --refresh
```

To force a particular versioned destination while still using the automatic reference layout:

```bash
python3 reference/replicant-docs-crawler/crawl_replicant_docs.py \
  --version 2.5.1 \
  --refresh
```

To write to an exact custom directory instead of the versioned reference layout:

```bash
python3 reference/replicant-docs-crawler/crawl_replicant_docs.py \
  --output /tmp/replicant-space-docs \
  --refresh
```

To mirror images as well, add `--download-assets`.

Ordinary subsequent runs against the same version directory send `If-None-Match` and `If-Modified-Since` where the server supplied ETag or Last-Modified values.

## Output

```text
reference/replicant-space-2-5-1/
├── INDEX.md
├── manifest.json
├── openapi.json
├── crawl-errors.json
├── index.md
├── changelog/
│   └── index.md
├── concepts/
│   └── civilisations/
│       └── index.md
├── api/
│   └── devices/
│       └── command/
│           └── index.md
└── _assets/                 # only with --download-assets
```

Each Markdown page includes YAML front matter with its original URL and crawl timestamp. Documentation links are rewritten to local relative Markdown paths. External links remain absolute.

The repository contract/policy scripts select the highest semantic version under `reference/replicant-space-*` automatically. After adding a new snapshot, update the checked-in contract metadata and compatibility code before expecting `make ci` to pass; this intentionally prevents a newly crawled API contract from becoming silently authoritative.

## Local crawler state

The crawler also maintains these per-version local artifacts:

```text
reference/replicant-space-2-5-1/.source-html/
reference/replicant-space-2-5-1/.crawl-cache.json
```

They are crawler-internal cache data and do not define the checked-in API contract. `repo_zip.py` explicitly includes the complete versioned snapshot tree even when broad ignore rules such as `*.json` or `logs/` would otherwise omit contract files. It still excludes `.source-html/` and `.crawl-cache.json`.

## Behavior and safety

- The crawler remains on the original host and under `/docs/`, plus `/changelog/`.
- `robots.txt` is honored by default.
- Requests are delayed by 350 ms by default.
- Transient errors and HTTP 429 responses use bounded retries.
- `--max-pages` prevents accidental unbounded crawling.
- No JavaScript runtime or browser automation is required.
