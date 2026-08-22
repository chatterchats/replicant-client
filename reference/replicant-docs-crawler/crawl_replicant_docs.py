#!/usr/bin/env python3
"""
Mirror Replicant Space docs and changelog as local Markdown, plus its OpenAPI JSON.

Features:
- Crawls same-origin pages under /docs/ plus /changelog/
- Honors robots.txt by default
- Polite configurable request delay
- Retries transient failures
- Incremental HTTP caching with ETag and Last-Modified
- Extracts the main article instead of site navigation/footer
- Converts HTML, tables, code blocks, links, and images to Markdown
- Rewrites documentation links to local .md files
- Writes INDEX.md, manifest.json, and crawl-errors.json
- Optionally downloads same-origin documentation assets

Usage:
    python crawl_replicant_docs.py
    python crawl_replicant_docs.py --output docs/reference/replicant-space
    python crawl_replicant_docs.py --refresh --download-assets
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import re
import sys
import time
from collections import deque
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Iterable
from urllib.parse import (
    parse_qsl,
    urldefrag,
    urlencode,
    urljoin,
    urlparse,
    urlunparse,
)
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup, Tag
from markdownify import MarkdownConverter
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DEFAULT_START_URL = "https://replicant.space/docs/"
DEFAULT_CHANGELOG_PATH = "/changelog/"
DEFAULT_OPENAPI_URL = "https://api.replicant.space/swagger/openapi.json"
DEFAULT_USER_AGENT = "replicant-rs-docs-mirror/1.0 (+local reference crawler)"
CACHE_FILE = ".crawl-cache.json"
MANIFEST_FILE = "manifest.json"
ERROR_FILE = "crawl-errors.json"


@dataclass
class PageRecord:
    url: str
    title: str
    local_path: str
    source_sha256: str
    markdown_sha256: str
    fetched_at: str
    status: str
    etag: str | None = None
    last_modified: str | None = None


class DocsMarkdownConverter(MarkdownConverter):
    """Markdownify customizations suited to technical documentation."""

    def convert_pre(self, el: Tag, text: str, parent_tags: set[str]) -> str:
        code = el.find("code")
        raw = code.get_text("", strip=False) if code else el.get_text("", strip=False)
        raw = raw.strip("\n")

        language = ""
        classes: list[str] = []
        for node in (el, code):
            if node and isinstance(node, Tag):
                cls = node.get("class", [])
                classes.extend(cls if isinstance(cls, list) else [str(cls)])

        for cls in classes:
            match = re.search(r"(?:language|lang)-([A-Za-z0-9_+#.-]+)", cls)
            if match:
                language = match.group(1)
                break

        # Use a fence longer than any backtick run in the payload.
        longest = max((len(m.group(0)) for m in re.finditer(r"`+", raw)), default=0)
        fence = "`" * max(3, longest + 1)
        return f"\n\n{fence}{language}\n{raw}\n{fence}\n\n"

    def convert_code(self, el: Tag, text: str, parent_tags: set[str]) -> str:
        if "pre" in parent_tags:
            return text
        raw = el.get_text("", strip=False)
        longest = max((len(m.group(0)) for m in re.finditer(r"`+", raw)), default=0)
        fence = "`" * max(1, longest + 1)
        return f"{fence}{raw}{fence}"

    def convert_table(self, el: Tag, text: str, parent_tags: set[str]) -> str:
        # markdownify handles ordinary tables. Keep this override as a stable
        # extension point and normalize excessive surrounding whitespace.
        converted = super().convert_table(el, text, parent_tags)
        return f"\n\n{converted.strip()}\n\n"

    def convert_span(self, el: Tag, text: str, parent_tags: set[str]) -> str:
        classes = el.get("class", [])
        if "doc-note-label" in classes:
            text = text.strip(" \t\r\n")
            return f"**{text}**" if text else ""
        return text

    def convert_aside(self, el: Tag, text: str, parent_tags: set[str]) -> str:
        classes = el.get("class", [])
        if "doc-note" not in classes:
            return text

        body = text.strip(" \t\r\n")
        if not body:
            return ""

        quoted = "\n".join(f"> {line}" if line else ">" for line in body.splitlines())
        return f"\n\n{quoted}\n\n"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Mirror Replicant Space documentation as local Markdown."
    )
    parser.add_argument("--start-url", default=DEFAULT_START_URL)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("replicant-space-docs"),
        help="Destination directory (default: ./replicant-space-docs)",
    )
    parser.add_argument(
        "--delay",
        type=float,
        default=0.35,
        help="Minimum delay between requests in seconds (default: 0.35)",
    )
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--max-pages", type=int, default=1000)
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore HTTP validators and fetch every page again.",
    )
    parser.add_argument(
        "--download-assets",
        action="store_true",
        help="Download same-origin images/files referenced by articles.",
    )
    parser.add_argument(
        "--no-robots",
        action="store_true",
        help="Do not consult robots.txt (not recommended).",
    )
    parser.add_argument(
        "--include-query",
        action="store_true",
        help="Treat query-string variants as distinct pages.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
    )
    return parser.parse_args()


def build_session(user_agent: str) -> requests.Session:
    retry = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,
        backoff_factor=0.6,
        status_forcelist=(408, 425, 429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET", "HEAD"}),
        respect_retry_after_header=True,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.5",
            "Accept-Encoding": "gzip, deflate",
        }
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def normalize_url(
    raw_url: str,
    *,
    base_url: str,
    docs_origin: tuple[str, str],
    docs_prefix: str,
    include_query: bool,
) -> str | None:
    joined = urljoin(base_url, raw_url)
    joined, _fragment = urldefrag(joined)
    parsed = urlparse(joined)

    if parsed.scheme not in {"http", "https"}:
        return None
    if (parsed.scheme, parsed.netloc) != docs_origin:
        return None

    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if not path.startswith(docs_prefix) and path != DEFAULT_CHANGELOG_PATH:
        return None

    # Ignore obvious non-page assets during page discovery.
    if re.search(
        r"\.(?:png|jpe?g|gif|svg|webp|ico|pdf|zip|json|xml|txt|css|js|woff2?|ttf)$",
        path,
        re.IGNORECASE,
    ):
        return None

    # Canonicalize directory-like docs URLs to a trailing slash.
    final_segment = PurePosixPath(path).name
    if "." not in final_segment and not path.endswith("/"):
        path += "/"

    query = parsed.query if include_query else ""
    if include_query and query:
        query = urlencode(sorted(parse_qsl(query, keep_blank_values=True)))

    return urlunparse((parsed.scheme, parsed.netloc, path, "", query, ""))


def url_to_local_path(url: str, docs_prefix: str) -> Path:
    parsed = urlparse(url)
    path = parsed.path

    relative = (
        "changelog"
        if path == DEFAULT_CHANGELOG_PATH
        else path[len(docs_prefix) :].strip("/")
    )
    if not relative:
        result = Path("index.md")
    else:
        result = Path(*PurePosixPath(relative).parts) / "index.md"

    if parsed.query:
        digest = hashlib.sha256(parsed.query.encode("utf-8")).hexdigest()[:10]
        result = result.with_name(f"index-query-{digest}.md")
    return result


def load_json(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default
    except json.JSONDecodeError:
        logging.warning("Ignoring malformed JSON cache: %s", path)
        return default


def save_json(path: Path, value) -> None:
    path.write_text(
        json.dumps(value, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def build_robots(
    session: requests.Session,
    start_url: str,
    user_agent: str,
    disabled: bool,
) -> RobotFileParser | None:
    if disabled:
        return None

    parsed = urlparse(start_url)
    robots_url = urlunparse((parsed.scheme, parsed.netloc, "/robots.txt", "", "", ""))
    robot = RobotFileParser()
    robot.set_url(robots_url)
    try:
        response = session.get(robots_url, timeout=20)
        if response.ok:
            robot.parse(response.text.splitlines())
            return robot
        logging.warning(
            "robots.txt returned HTTP %s; continuing conservatively with docs crawl",
            response.status_code,
        )
    except requests.RequestException as exc:
        logging.warning("Could not read robots.txt: %s", exc)
    return None


def article_candidates(soup: BeautifulSoup) -> list[Tag]:
    selectors = (
        "main article",
        "article",
        "main",
        "[role='main']",
        ".docs-content",
        ".documentation-content",
        ".content",
    )
    found: list[Tag] = []
    seen: set[int] = set()
    for selector in selectors:
        for node in soup.select(selector):
            if id(node) not in seen:
                seen.add(id(node))
                found.append(node)
    return found


def choose_article(soup: BeautifulSoup) -> Tag:
    candidates = article_candidates(soup)
    if candidates:
        # Navigation-heavy containers often contain many links and little prose.
        # Prefer the candidate with the most non-link text.
        def score(node: Tag) -> int:
            clone = BeautifulSoup(str(node), "html.parser")
            for unwanted in clone.select(
                "nav, header, footer, aside, script, style, noscript, template, "
                ".sidebar, .toc, .table-of-contents, [aria-label='breadcrumb']"
            ):
                unwanted.decompose()
            text_len = len(clone.get_text(" ", strip=True))
            link_len = sum(
                len(a.get_text(" ", strip=True)) for a in clone.find_all("a")
            )
            return text_len - min(link_len, text_len // 2)

        return max(candidates, key=score)

    body = soup.body
    if not body:
        raise ValueError("HTML document has no body")
    return body


def clean_article(article: Tag) -> Tag:
    clone_soup = BeautifulSoup(str(article), "html.parser")
    clone = clone_soup.find()
    assert isinstance(clone, Tag)

    selectors = (
        "script, style, noscript, template, nav, footer, aside:not(.doc-note), "
        ".sidebar, .docs-menu, .mobile-menu, .table-of-contents, .toc, "
        ".breadcrumb, .breadcrumbs, .page-navigation, .pagination, "
        "[aria-label='breadcrumb'], [aria-label='Docs menu']"
    )
    for unwanted in clone.select(selectors):
        unwanted.decompose()

    for node in clone.select("[hidden], [aria-hidden='true']"):
        node.decompose()

    return clone


def discover_page_links(
    soup: BeautifulSoup,
    *,
    page_url: str,
    docs_origin: tuple[str, str],
    docs_prefix: str,
    include_query: bool,
) -> set[str]:
    found: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        normalized = normalize_url(
            str(anchor["href"]),
            base_url=page_url,
            docs_origin=docs_origin,
            docs_prefix=docs_prefix,
            include_query=include_query,
        )
        if normalized:
            found.add(normalized)
    return found


def local_relative_link(source_file: Path, target_file: Path) -> str:
    from os.path import relpath

    rel = relpath(target_file, start=source_file.parent)
    return Path(rel).as_posix()


def rewrite_article_urls(
    article: Tag,
    *,
    page_url: str,
    page_path: Path,
    url_map: dict[str, Path],
    docs_origin: tuple[str, str],
    docs_prefix: str,
    include_query: bool,
    asset_map: dict[str, Path],
) -> None:
    for anchor in article.find_all("a", href=True):
        raw = str(anchor["href"])
        fragment = urlparse(urljoin(page_url, raw)).fragment
        normalized = normalize_url(
            raw,
            base_url=page_url,
            docs_origin=docs_origin,
            docs_prefix=docs_prefix,
            include_query=include_query,
        )
        if normalized and normalized in url_map:
            value = local_relative_link(page_path, url_map[normalized])
            if fragment:
                value += f"#{fragment}"
            anchor["href"] = value
        else:
            anchor["href"] = urljoin(page_url, raw)

    for image in article.find_all("img", src=True):
        absolute = urldefrag(urljoin(page_url, str(image["src"])))[0]
        if absolute in asset_map:
            image["src"] = local_relative_link(page_path, asset_map[absolute])
        else:
            image["src"] = absolute


def page_title(soup: BeautifulSoup, article: Tag, url: str) -> str:
    heading = article.find("h1")
    if heading:
        title = heading.get_text(" ", strip=True)
        if title:
            return title
    if soup.title:
        title = soup.title.get_text(" ", strip=True)
        title = re.sub(r"\s*[·|]\s*Replicant Space Docs\s*$", "", title)
        if title:
            return title
    return PurePosixPath(urlparse(url).path.rstrip("/")).name or "Introduction"


def html_to_markdown(article: Tag) -> str:
    converter = DocsMarkdownConverter(
        heading_style="ATX",
        bullets="-",
        strong_em_symbol="*",
        escape_asterisks=False,
        escape_underscores=False,
        wrap=False,
        strip=["button"],
    )
    markdown = converter.convert_soup(article)
    markdown = re.sub(r"\n{4,}", "\n\n\n", markdown)
    markdown = re.sub(r"[ \t]+\n", "\n", markdown)
    return markdown.strip() + "\n"


def collect_asset_urls(
    article: Tag, page_url: str, origin: tuple[str, str]
) -> set[str]:
    urls: set[str] = set()
    for node, attr in [(n, "src") for n in article.find_all("img", src=True)]:
        absolute = urldefrag(urljoin(page_url, str(node[attr])))[0]
        parsed = urlparse(absolute)
        if (parsed.scheme, parsed.netloc) == origin:
            urls.add(absolute)
    return urls


def asset_path_for(url: str) -> Path:
    parsed = urlparse(url)
    raw = PurePosixPath(parsed.path)
    filename = raw.name or hashlib.sha256(url.encode()).hexdigest()[:16]
    safe_parts = [p for p in raw.parts if p not in {"/", ".", ".."}]
    if not safe_parts:
        safe_parts = [filename]
    return Path("_assets", *safe_parts)


def download_assets(
    session: requests.Session,
    asset_urls: Iterable[str],
    output: Path,
    delay: float,
    timeout: float,
) -> tuple[dict[str, Path], list[dict[str, str]]]:
    mapping: dict[str, Path] = {}
    errors: list[dict[str, str]] = []
    last_request = 0.0

    for url in sorted(set(asset_urls)):
        local = asset_path_for(url)
        destination = output / local
        mapping[url] = local
        if destination.exists():
            continue

        wait = delay - (time.monotonic() - last_request)
        if wait > 0:
            time.sleep(wait)

        try:
            response = session.get(url, timeout=timeout, stream=True)
            last_request = time.monotonic()
            response.raise_for_status()
            destination.parent.mkdir(parents=True, exist_ok=True)
            with destination.open("wb") as handle:
                for chunk in response.iter_content(64 * 1024):
                    if chunk:
                        handle.write(chunk)
        except requests.RequestException as exc:
            logging.warning("Asset failed: %s: %s", url, exc)
            errors.append({"url": url, "error": str(exc)})
            mapping.pop(url, None)
    return mapping, errors


def write_index(output: Path, records: list[PageRecord], start_url: str) -> None:
    lines = [
        "# Replicant Space Documentation Mirror",
        "",
        f"- Source: {start_url}",
        f"- Generated: {datetime.now(timezone.utc).isoformat()}",
        f"- Pages: {len(records)}",
        "",
        "## Pages",
        "",
    ]
    for record in sorted(
        records, key=lambda item: (item.local_path, item.title.lower())
    ):
        lines.append(f"- [{record.title}]({record.local_path})")
    lines.append("")
    (output / "INDEX.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s %(message)s",
    )

    start_url = args.start_url
    parsed_start = urlparse(start_url)
    if parsed_start.scheme not in {"http", "https"} or not parsed_start.netloc:
        logging.error("Invalid --start-url: %s", start_url)
        return 2

    docs_origin = (parsed_start.scheme, parsed_start.netloc)
    docs_prefix = parsed_start.path
    if not docs_prefix.endswith("/"):
        docs_prefix += "/"

    normalized_start = normalize_url(
        start_url,
        base_url=start_url,
        docs_origin=docs_origin,
        docs_prefix=docs_prefix,
        include_query=args.include_query,
    )
    if not normalized_start:
        logging.error("Start URL is outside its own documentation scope")
        return 2

    output: Path = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)

    cache_path = output / CACHE_FILE
    cache: dict[str, dict[str, str]] = load_json(cache_path, {})
    session = build_session(DEFAULT_USER_AGENT)
    robots = build_robots(session, normalized_start, DEFAULT_USER_AGENT, args.no_robots)

    changelog_url = urlunparse(
        (*docs_origin, DEFAULT_CHANGELOG_PATH, "", "", "")
    )
    queue: deque[str] = deque([normalized_start, changelog_url])
    queued: set[str] = set(queue)
    found_from: dict[str, str] = {}
    visited: set[str] = set()
    html_by_url: dict[str, str] = {}
    status_by_url: dict[str, str] = {}
    response_meta: dict[str, dict[str, str | None]] = {}
    errors: list[dict[str, str | int]] = []
    all_asset_urls: set[str] = set()
    last_request = 0.0

    while queue and len(visited) < args.max_pages:
        url = queue.popleft()
        if url in visited:
            continue
        visited.add(url)

        if robots and not robots.can_fetch(DEFAULT_USER_AGENT, url):
            logging.warning(
                "Blocked by robots.txt: %s (found on %s)",
                url,
                found_from.get(url, "start URL"),
            )
            errors.append(
                {
                    "url": url,
                    "error": "blocked by robots.txt",
                    "found_on": found_from.get(url, ""),
                }
            )
            continue

        headers: dict[str, str] = {}
        cached = cache.get(url, {})
        if not args.refresh:
            if cached.get("etag"):
                headers["If-None-Match"] = cached["etag"]
            if cached.get("last_modified"):
                headers["If-Modified-Since"] = cached["last_modified"]

        wait = args.delay - (time.monotonic() - last_request)
        if wait > 0:
            time.sleep(wait)

        logging.info("[%d/%d] %s", len(visited), args.max_pages, url)
        try:
            response = session.get(url, headers=headers, timeout=args.timeout)
            last_request = time.monotonic()
        except requests.RequestException as exc:
            logging.error(
                "Request failed: %s (found on %s)",
                exc,
                found_from.get(url, "start URL"),
            )
            errors.append(
                {"url": url, "error": str(exc), "found_on": found_from.get(url, "")}
            )
            continue

        if response.status_code == 304:
            local_source = cached.get("source_file")
            if local_source and (output / local_source).exists():
                html = (output / local_source).read_text(encoding="utf-8")
                status_by_url[url] = "not-modified"
            else:
                # Cache metadata survived but source cache did not. Fetch normally.
                try:
                    response = session.get(url, timeout=args.timeout)
                    last_request = time.monotonic()
                    response.raise_for_status()
                    html = response.text
                    status_by_url[url] = "fetched"
                except requests.RequestException as exc:
                    errors.append(
                        {
                            "url": url,
                            "error": str(exc),
                            "found_on": found_from.get(url, ""),
                        }
                    )
                    continue
        elif response.ok:
            content_type = response.headers.get("Content-Type", "")
            if "html" not in content_type.lower():
                logging.warning(
                    "Skipping non-HTML response: %s (%s) (found on %s)",
                    url,
                    content_type,
                    found_from.get(url, "start URL"),
                )
                errors.append(
                    {
                        "url": url,
                        "status": response.status_code,
                        "error": f"non-HTML content type: {content_type}",
                        "found_on": found_from.get(url, ""),
                    }
                )
                continue
            html = response.text
            status_by_url[url] = "fetched"
        else:
            logging.error(
                "HTTP %s: %s (found on %s)",
                response.status_code,
                url,
                found_from.get(url, "start URL"),
            )
            errors.append(
                {
                    "url": url,
                    "status": response.status_code,
                    "error": response.reason,
                    "found_on": found_from.get(url, ""),
                }
            )
            continue

        html_by_url[url] = html
        response_meta[url] = {
            "etag": response.headers.get("ETag") or cached.get("etag"),
            "last_modified": response.headers.get("Last-Modified")
            or cached.get("last_modified"),
        }

        soup = BeautifulSoup(html, "html.parser")
        discovered = discover_page_links(
            soup,
            page_url=url,
            docs_origin=docs_origin,
            docs_prefix=docs_prefix,
            include_query=args.include_query,
        )
        for candidate in sorted(discovered):
            if candidate not in visited and candidate not in queued:
                queued.add(candidate)
                queue.append(candidate)
                found_from[candidate] = url

        try:
            article = clean_article(choose_article(soup))
            all_asset_urls.update(collect_asset_urls(article, url, docs_origin))
        except Exception as exc:
            logging.warning("Article discovery failed while scanning assets: %s", exc)

    if queue:
        logging.warning(
            "Stopped at --max-pages=%d with %d URLs still queued",
            args.max_pages,
            len(queue),
        )

    url_map = {url: url_to_local_path(url, docs_prefix) for url in sorted(html_by_url)}

    asset_map: dict[str, Path] = {}
    if args.download_assets:
        asset_map, asset_errors = download_assets(
            session,
            all_asset_urls,
            output,
            args.delay,
            args.timeout,
        )
        errors.extend(asset_errors)

    records: list[PageRecord] = []
    new_cache: dict[str, dict[str, str]] = {}

    source_cache_dir = output / ".source-html"
    source_cache_dir.mkdir(exist_ok=True)

    for url, html in sorted(html_by_url.items()):
        soup = BeautifulSoup(html, "html.parser")
        try:
            article = clean_article(choose_article(soup))
            title = page_title(soup, article, url)
            local_path = url_map[url]
            rewrite_article_urls(
                article,
                page_url=url,
                page_path=local_path,
                url_map=url_map,
                docs_origin=docs_origin,
                docs_prefix=docs_prefix,
                include_query=args.include_query,
                asset_map=asset_map,
            )
            body = html_to_markdown(article)
        except Exception as exc:
            logging.error("Conversion failed for %s: %s", url, exc)
            errors.append({"url": url, "error": f"conversion: {exc}"})
            continue

        frontmatter = (
            "---\n"
            f"title: {json.dumps(title, ensure_ascii=False)}\n"
            f"source_url: {json.dumps(url)}\n"
            f"crawled_at: {json.dumps(datetime.now(timezone.utc).isoformat())}\n"
            "---\n\n"
        )

        # Avoid duplicate H1 when title already begins the converted article.
        document = frontmatter + body
        destination = output / local_path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(document, encoding="utf-8")

        source_hash = hashlib.sha256(html.encode("utf-8")).hexdigest()
        markdown_hash = hashlib.sha256(document.encode("utf-8")).hexdigest()
        source_filename = f"{hashlib.sha256(url.encode()).hexdigest()}.html"
        (source_cache_dir / source_filename).write_text(html, encoding="utf-8")

        meta = response_meta.get(url, {})
        record = PageRecord(
            url=url,
            title=title,
            local_path=local_path.as_posix(),
            source_sha256=source_hash,
            markdown_sha256=markdown_hash,
            fetched_at=datetime.now(timezone.utc).isoformat(),
            status=status_by_url.get(url, "converted"),
            etag=meta.get("etag"),
            last_modified=meta.get("last_modified"),
        )
        records.append(record)
        new_cache[url] = {
            "etag": record.etag or "",
            "last_modified": record.last_modified or "",
            "source_file": f".source-html/{source_filename}",
            "local_path": record.local_path,
            "source_sha256": source_hash,
        }

    write_index(output, records, normalized_start)
    save_json(
        output / MANIFEST_FILE,
        {
            "source": normalized_start,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "page_count": len(records),
            "pages": [asdict(record) for record in records],
        },
    )
    save_json(output / ERROR_FILE, errors)
    save_json(cache_path, new_cache)

    try:
        response = session.get(
            DEFAULT_OPENAPI_URL,
            headers={"Accept": "application/json"},
            timeout=args.timeout,
        )
        response.raise_for_status()
        openapi = response.json()
        if not isinstance(openapi, dict) or not openapi.get("openapi"):
            raise ValueError("response is not an OpenAPI document")
        save_json(output / "openapi.json", openapi)
    except (requests.RequestException, requests.JSONDecodeError, ValueError) as exc:
        logging.error("OpenAPI download failed: %s", exc)
        errors.append({"url": DEFAULT_OPENAPI_URL, "error": str(exc)})
        save_json(output / ERROR_FILE, errors)

    logging.info("Wrote %d Markdown pages to %s", len(records), output)
    if errors:
        logging.warning(
            "%d crawl/conversion issues recorded in %s", len(errors), ERROR_FILE
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
