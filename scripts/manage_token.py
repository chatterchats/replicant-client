#!/usr/bin/env python3
"""Create or rotate the local replicantd API token without exposing it."""

from __future__ import annotations

import argparse
from pathlib import Path
import secrets
from typing import Callable


TokenFactory = Callable[[], str]


def update_token(
    env_path: Path,
    example_path: Path,
    *,
    rotate: bool = False,
    token_factory: TokenFactory | None = None,
) -> str:
    """Ensure ``REPLICANTD_TOKEN`` exists in *env_path* and return an action label."""

    if not env_path.exists():
        if not example_path.exists():
            raise FileNotFoundError(f"no {env_path} and no {example_path} to copy from")
        env_path.write_text(example_path.read_text())
        created = True
    else:
        created = False

    lines = env_path.read_text().splitlines()
    current = next(
        (line.split("=", 1)[1] for line in lines if line.startswith("REPLICANTD_TOKEN=")),
        "",
    )

    if current and not rotate:
        return "existing"

    token = (token_factory or (lambda: secrets.token_urlsafe(32)))()
    replacement = f"REPLICANTD_TOKEN={token}"
    if any(line.startswith("REPLICANTD_TOKEN=") for line in lines):
        lines = [
            replacement if line.startswith("REPLICANTD_TOKEN=") else line for line in lines
        ]
    else:
        lines.append(replacement)
    env_path.write_text("\n".join(lines) + "\n")

    if rotate:
        return "rotated"
    return "created-from-example" if created else "created"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rotate", action="store_true", help="replace an existing token")
    parser.add_argument("--env", type=Path, default=Path(".env"), help="environment file")
    parser.add_argument(
        "--example",
        type=Path,
        default=Path(".env.example"),
        help="template copied when --env does not exist",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        action = update_token(args.env, args.example, rotate=args.rotate)
    except FileNotFoundError as exc:
        raise SystemExit(str(exc)) from exc

    if action == "existing":
        print('REPLICANTD_TOKEN is already set in .env; use "make token-rotate" to replace it')
    elif action == "rotated":
        print("rotated REPLICANTD_TOKEN in .env")
        print("re-run `docker compose up -d` (or `make docker-restart`) so services use it")
    elif action == "created-from-example":
        print("created .env from .env.example")
        print("wrote a new REPLICANTD_TOKEN to .env")
    else:
        print("wrote a new REPLICANTD_TOKEN to .env")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
