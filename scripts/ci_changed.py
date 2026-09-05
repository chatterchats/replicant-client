#!/usr/bin/env python3
"""Classify changed repository paths into independently runnable CI domains."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen


GROUPS = ("core", "policy", "galaxy", "web", "desktop", "docs", "docker")
GITHUB_API_VERSION = "2022-11-28"

# Changes to build orchestration or the classifier itself can invalidate every
# domain assumption, so they deliberately force a complete run.
GLOBAL_BUILD_PATHS = {
    "Makefile",
    "rust-toolchain.toml",
    ".cargo/config.toml",
    ".github/workflows/ci.yml",
    "scripts/ci_changed.py",
    "scripts/test_ci_changed.py",
}


def _under(path: str, prefix: str) -> bool:
    return path == prefix.rstrip("/") or path.startswith(prefix)


def classify_paths(paths: Iterable[str]) -> dict[str, bool]:
    """Return the CI domains affected by *paths*.

    Rules model dependency impact rather than directory ownership. In
    particular, Galaxy renderer changes also validate the web application,
    while policy checks follow the contract-bearing root client sources.
    """

    result = {group: False for group in GROUPS}
    normalized = set()
    for raw_path in paths:
        path = raw_path.strip()
        if not path:
            continue
        if path.startswith("./"):
            path = path[2:]
        normalized.add(path)

    if normalized & GLOBAL_BUILD_PATHS:
        return {group: True for group in GROUPS}

    for path in normalized:
        is_galaxy = _under(path, "crates/galaxy-renderer/")
        is_core_crate = _under(path, "crates/") and not is_galaxy

        if (
            path in {"Cargo.toml", "Cargo.lock", "clippy.toml", "rustfmt.toml"}
            or _under(path, "src/")
            or _under(path, "migrations/")
            or _under(path, "tests/")
            or _under(path, "examples/")
            or is_core_crate
        ):
            result["core"] = True

        if path in {"Cargo.toml", "Cargo.lock", "clippy.toml", "rustfmt.toml"}:
            result["desktop"] = True
        if path in {"clippy.toml", "rustfmt.toml"}:
            result["galaxy"] = True

        if (
            path == "Cargo.toml"
            or _under(path, "src/")
            or _under(path, "migrations/")
            or _under(path, "tests/")
            or _under(path, "examples/")
            or _under(path, "policy/")
            or (_under(path, "scripts/") and path.endswith(".py"))
            or path.startswith("reference/replicant-space-")
        ):
            result["policy"] = True

        if is_galaxy:
            result["galaxy"] = True
            result["web"] = True

        if _under(path, "apps/web/") and path not in {
            "apps/web/Dockerfile",
            "apps/web/nginx.conf.template",
        }:
            result["web"] = True

        if _under(path, "apps/desktop/"):
            result["desktop"] = True

        if path == ".prettierignore":
            result["web"] = True
            result["desktop"] = True

        if _under(path, "reference/replicant-docs-crawler/"):
            result["docs"] = True

        if (
            path in {"Dockerfile", ".dockerignore", ".env.example"}
            or path.startswith("compose") and path.endswith((".yaml", ".yml"))
            or path in {"apps/web/Dockerfile", "apps/web/nginx.conf.template"}
            or _under(path, "deploy/")
        ):
            result["docker"] = True

    return result


def parse_name_status(output: str) -> list[str]:
    """Return every old/new path represented by ``git diff --name-status``."""

    paths: list[str] = []
    for line in output.splitlines():
        if not line:
            continue
        parts = line.split("\t")
        status = parts[0]
        if status.startswith(("R", "C")) and len(parts) >= 3:
            paths.extend(parts[1:3])
        elif len(parts) >= 2:
            paths.append(parts[-1])
    return paths


def changed_paths(base: str, head: str) -> list[str]:
    """Return paths changed from *base* to *head*.

    A zero/missing base is treated as an unclassifiable history boundary and
    therefore raises ``ValueError`` so callers can conservatively run all CI.
    """

    if not base or set(base) == {"0"}:
        raise ValueError("base revision is unavailable")

    for revision in (base, head):
        probe = subprocess.run(
            ["git", "cat-file", "-e", f"{revision}^{{commit}}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=False,
        )
        if probe.returncode != 0:
            raise ValueError(f"revision is unavailable: {revision}")

    completed = subprocess.run(
        [
            "git",
            "diff",
            "--name-status",
            "--find-renames",
            "--find-copies",
            "--diff-filter=ACMRD",
            base,
            head,
        ],
        check=True,
        text=True,
        capture_output=True,
    )
    return parse_name_status(completed.stdout)


def successful_run_sha(payload: object) -> str:
    """Return the newest usable head SHA from a workflow-runs API response."""

    if not isinstance(payload, dict):
        raise ValueError("GitHub Actions history response is not an object")
    runs = payload.get("workflow_runs")
    if not isinstance(runs, list):
        raise ValueError("GitHub Actions history response has no workflow_runs list")

    for run in runs:
        if not isinstance(run, dict):
            continue
        sha = run.get("head_sha")
        if isinstance(sha, str) and sha:
            return sha

    raise ValueError("no successful workflow run with a head SHA was found")


def latest_successful_sha(
    repository: str,
    workflow: str,
    branch: str,
    token: str | None = None,
) -> str:
    """Resolve the newest successful workflow head SHA for *branch*."""

    if "/" not in repository:
        raise ValueError(f"invalid GitHub repository: {repository!r}")
    if not workflow:
        raise ValueError("workflow is required")
    if not branch:
        raise ValueError("branch is required")

    query = urlencode(
        {
            "branch": branch,
            "status": "success",
            "per_page": "50",
        }
    )
    url = (
        f"https://api.github.com/repos/{quote(repository, safe='/')}/actions/"
        f"workflows/{quote(workflow, safe='')}/runs?{query}"
    )
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "replicant-client-ci",
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"

    request = Request(url, headers=headers)
    try:
        with urlopen(request, timeout=15) as response:
            payload = json.load(response)
    except (HTTPError, URLError, OSError, ValueError) as exc:
        raise ValueError(f"GitHub Actions history lookup failed: {exc}") from exc

    return successful_run_sha(payload)


def write_github_output(path: Path, result: dict[str, bool]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        for group in GROUPS:
            handle.write(f"{group}={'true' if result[group] else 'false'}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group()
    source.add_argument("--base", help="base commit for git diff")
    source.add_argument(
        "--last-successful",
        action="store_true",
        help="diff from the newest successful GitHub Actions run on the selected branch",
    )
    source.add_argument("--all", action="store_true", help="mark every CI domain affected")

    parser.add_argument("--head", default="HEAD", help="head commit for git diff")
    parser.add_argument("--repository", help="GitHub repository in owner/name form")
    parser.add_argument("--workflow", help="GitHub Actions workflow file name or ID")
    parser.add_argument("--branch", help="GitHub branch used to resolve successful runs")
    parser.add_argument(
        "--github-output",
        type=Path,
        help="append domain booleans to this GitHub Actions output file",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    base: str | None = args.base

    if args.all:
        paths: list[str] = []
        result = {group: True for group in GROUPS}
    else:
        if args.last_successful:
            missing = [
                name
                for name, value in (
                    ("--repository", args.repository),
                    ("--workflow", args.workflow),
                    ("--branch", args.branch),
                )
                if not value
            ]
            if missing:
                raise SystemExit(
                    "--last-successful requires " + ", ".join(missing)
                )
            try:
                base = latest_successful_sha(
                    args.repository,
                    args.workflow,
                    args.branch,
                    os.environ.get("GITHUB_TOKEN"),
                )
                print(f"Last successful CI baseline: {base}")
            except ValueError as exc:
                print(
                    "CI change detection cannot resolve the last successful "
                    f"validation ({exc}); running all domains"
                )
                paths = []
                result = {group: True for group in GROUPS}
                base = None

        if base is not None:
            try:
                paths = changed_paths(base, args.head)
                result = classify_paths(paths)
            except ValueError as exc:
                print(
                    f"CI change detection cannot classify this push ({exc}); "
                    "running all domains"
                )
                paths = []
                result = {group: True for group in GROUPS}
        elif not args.last_successful:
            raise SystemExit("--base, --last-successful, or --all is required")

    if paths:
        print("Changed paths:")
        for path in paths:
            print(f"  {path}")
    elif args.all:
        print("Full CI requested explicitly")

    print("CI domains:")
    for group in GROUPS:
        print(f"  {group}: {'run' if result[group] else 'skip'}")

    if args.github_output:
        write_github_output(args.github_output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
