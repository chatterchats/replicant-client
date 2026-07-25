#!/usr/bin/env python3
"""Keep normalized snapshots forward-compatible and independent of raw DTOs."""

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
vocab = (ROOT / "src/domain/vocab.rs").read_text()
models = (ROOT / "src/domain/model.rs").read_text()
errors = []
if vocab.count("open_value!(") < 10:
    errors.append("open server vocabularies must retain unknown strings")
if re.search(r"pub struct .*?\{.*?raw::", models, re.S):
    errors.append("public normalized snapshot contains a raw DTO")
if errors:
    print("forward compatibility policy check FAILED:", *errors, sep="\n  - ", file=sys.stderr)
    sys.exit(1)
print("forward compatibility policy check passed: open vocabularies and normalized snapshots verified")
