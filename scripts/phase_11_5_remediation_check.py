#!/usr/bin/env python3
"""Validate the Phase 11.5 remediation ledger and its release blockers."""

import copy
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
LEDGER = ROOT / "policy" / "phase-11.5-remediation.json"
EXPECTED_IDS = {f"B-{number:02d}" for number in range(1, 13)}
EXPECTED_IDS |= {f"H-{number:02d}" for number in range(1, 15)}
EXPECTED_IDS |= {f"M-{number:02d}" for number in range(1, 14)}
ALLOWED_STATUSES = {"open", "in_progress", "resolved", "removed", "accepted_risk"}
REQUIRED_FIELDS = {
    "id", "severity", "owning_remediation_prompt", "affected_modules",
    "regression_test_category", "status", "resolution_evidence",
}
EVIDENCE_FIELDS = {"code_references", "test_references", "policy_references", "notes"}


def validate(ledger: object) -> list[str]:
    if not isinstance(ledger, dict):
        return ["ledger root must be an object"]
    findings = ledger.get("findings")
    if not isinstance(findings, list):
        return ["ledger.findings must be a list"]

    errors: list[str] = []
    ids: list[str] = []
    for index, finding in enumerate(findings):
        prefix = f"findings[{index}]"
        if not isinstance(finding, dict):
            errors.append(f"{prefix} must be an object")
            continue
        missing = REQUIRED_FIELDS - finding.keys()
        if missing:
            errors.append(f"{prefix} missing fields: {', '.join(sorted(missing))}")
            continue
        finding_id = finding["id"]
        if not isinstance(finding_id, str):
            errors.append(f"{prefix}.id must be a string")
            continue
        ids.append(finding_id)
        for field in ("severity", "owning_remediation_prompt", "regression_test_category"):
            if not isinstance(finding[field], str) or not finding[field].strip():
                errors.append(f"{finding_id}.{field} must be a non-empty string")
        if not isinstance(finding["affected_modules"], list) or not finding["affected_modules"] or not all(isinstance(item, str) and item for item in finding["affected_modules"]):
            errors.append(f"{finding_id}.affected_modules must be a non-empty string list")
        status = finding["status"]
        if status not in ALLOWED_STATUSES:
            errors.append(f"{finding_id}.status {status!r} is not allowed")
        evidence = finding["resolution_evidence"]
        if not isinstance(evidence, dict) or EVIDENCE_FIELDS - evidence.keys():
            errors.append(f"{finding_id}.resolution_evidence must contain {', '.join(sorted(EVIDENCE_FIELDS))}")
            continue
        if any(not isinstance(evidence[key], list) or not all(isinstance(item, str) and item for item in evidence[key]) for key in EVIDENCE_FIELDS):
            errors.append(f"{finding_id}.resolution_evidence values must be string lists")
        if status == "resolved":
            supporting = evidence["code_references"] + evidence["policy_references"] + evidence["notes"]
            if not evidence["test_references"] or not supporting:
                errors.append(f"resolved finding {finding_id} requires test and supporting evidence references")

    actual_ids = set(ids)
    missing_ids = EXPECTED_IDS - actual_ids
    extra_ids = actual_ids - EXPECTED_IDS
    duplicate_ids = sorted({finding_id for finding_id in ids if ids.count(finding_id) > 1})
    if missing_ids:
        errors.append(f"missing finding IDs: {', '.join(sorted(missing_ids))}")
    if extra_ids:
        errors.append(f"unknown finding IDs: {', '.join(sorted(extra_ids))}")
    if duplicate_ids:
        errors.append(f"duplicate finding IDs: {', '.join(duplicate_ids)}")
    return errors


def self_test() -> None:
    ledger = json.loads(LEDGER.read_text())
    assert not validate(ledger), "checked-in ledger must validate"

    missing = copy.deepcopy(ledger)
    missing["findings"] = [finding for finding in missing["findings"] if finding["id"] != "B-01"]
    assert any("missing finding IDs: B-01" in error for error in validate(missing))

    duplicate = copy.deepcopy(ledger)
    duplicate["findings"].append(copy.deepcopy(duplicate["findings"][0]))
    assert any("duplicate finding IDs: B-01" in error for error in validate(duplicate))

    unresolved = copy.deepcopy(ledger)
    unresolved["findings"][0]["status"] = "resolved"
    assert any("resolved finding B-01 requires test and supporting evidence" in error for error in validate(unresolved))

    print("phase 11.5 remediation checker self-test passed")


def main() -> None:
    if sys.argv[1:] == ["--self-test"]:
        self_test()
        return
    if sys.argv[1:]:
        raise SystemExit("usage: phase_11_5_remediation_check.py [--self-test]")
    try:
        ledger = json.loads(LEDGER.read_text())
    except (OSError, json.JSONDecodeError) as error:
        raise SystemExit(f"cannot read Phase 11.5 remediation ledger: {error}") from error
    errors = validate(ledger)
    if errors:
        print("phase 11.5 remediation check FAILED:", *errors, sep="\n  - ", file=sys.stderr)
        raise SystemExit(1)
    blockers = [finding["id"] for finding in ledger["findings"] if finding["severity"] == "blocker" and finding["status"] != "resolved"]
    print(f"phase 11.5 remediation check passed: {len(ledger['findings'])} findings tracked")
    print("remaining release blockers: " + ", ".join(blockers) if blockers else "remaining release blockers: none")


if __name__ == "__main__":
    main()
