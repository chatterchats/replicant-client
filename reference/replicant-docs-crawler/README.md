# Replicant Space Documentation Crawler

Mirrors `https://replicant.space/docs/` into a local Markdown tree for Codex and other offline tools.

## Setup

```bash
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Run

```bash
python3 scripts/replicant-docs-crawler/crawl_replicant_docs.py \
  --output docs/reference/replicant-space
```

To mirror images as well:

```bash
python3 scripts/replicant-docs-crawler/crawl_replicant_docs.py \
  --output docs/reference/replicant-space \
  --download-assets
```

To force a complete refresh:

```bash
make docs-reference-sync
```

Ordinary subsequent runs send `If-None-Match` and `If-Modified-Since` where the server supplied
ETag or Last-Modified values.

## Output

```text
replicant-space-docs/
├── INDEX.md
├── manifest.json
├── crawl-errors.json
├── index.md
├── concepts/
│   └── civilisations/
│       └── index.md
├── api/
│   └── devices/
│       └── command/
│           └── index.md
└── _assets/                 # only with --download-assets
```

Each Markdown page includes YAML front matter with its original URL and crawl timestamp.
Documentation links are rewritten to local relative Markdown paths. External links remain absolute.

## Suggested repository integration

The repository Make target resolves both the crawler and output from the repository root:

```make
docs-reference-sync:
	python3 scripts/replicant-docs-crawler/crawl_replicant_docs.py \
	  --output docs/reference/replicant-space \
	  --refresh
```

To keep generated documentation out of the main repository:

```gitignore
docs/reference/replicant-space/.source-html/
docs/reference/replicant-space/.crawl-cache.json
```

The source HTML cache is needed for conditional `304 Not Modified` responses. Keep it locally or in
a build cache even when it is excluded from Git.

## Behavior and safety

- The crawler remains on the original host and under `/docs/`.
- `robots.txt` is honored by default.
- Requests are delayed by 350 ms by default.
- Transient errors and HTTP 429 responses use bounded retries.
- `--max-pages` prevents accidental unbounded crawling.
- No JavaScript runtime or browser automation is required.
