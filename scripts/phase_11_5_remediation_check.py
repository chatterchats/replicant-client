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
PHASE_11_6_REPORTS = {
    "11.6.00": "docs/implementation/phase-11.6/00-baseline-and-validator.md",
    "11.6.01": "docs/implementation/phase-11.6/01-full-sync-and-managed-coverage.md",
    "11.6.01b": "docs/implementation/phase-11.6/01b-location-domain-and-predicates.md",
    "11.6.01c": "docs/implementation/phase-11.6/01c-colony-database-initializer.md",
    "11.6.02": "docs/implementation/phase-11.6/02-typed-mutation-adapters.md",
    "11.6.03": "docs/implementation/phase-11.6/03-async-safe-persistence.md",
    "11.6.04": "docs/implementation/phase-11.6/04-readiness-scheduler-and-query-performance.md",
    "11.6.05": "docs/implementation/phase-11.6/05-fault-stress-and-restoration-evidence.md",
}
ALLOWED_REPORT_STATUSES = {"required", "complete"}
REQUIRED_FIELDS = {
    "id", "severity", "owning_remediation_prompt", "affected_modules",
    "regression_test_category", "status", "resolution_evidence",
}
EVIDENCE_FIELDS = {"code_references", "test_references", "policy_references", "notes"}


def release_blockers(ledger: dict) -> list[str]:
    return [
        finding["id"]
        for finding in ledger["findings"]
        if finding["severity"] == "blocker" and finding["status"] != "resolved"
    ]


def validate(ledger: object) -> list[str]:
    if not isinstance(ledger, dict):
        return ["ledger root must be an object"]
    findings = ledger.get("findings")
    if not isinstance(findings, list):
        return ["ledger.findings must be a list"]

    errors: list[str] = []
    validator_evidence = ledger.get("phase_11_6_validator_evidence")
    if not isinstance(validator_evidence, dict) or set(validator_evidence) != {"report", "test_references"}:
        errors.append("ledger.phase_11_6_validator_evidence must contain report and test_references")
    else:
        if validator_evidence["report"] != PHASE_11_6_REPORTS["11.6.00"]:
            errors.append("Phase 11.6 validator evidence must reference the required baseline report")
        tests = validator_evidence["test_references"]
        if not isinstance(tests, list) or not tests or not all(isinstance(test, str) and test for test in tests):
            errors.append("Phase 11.6 validator evidence requires test references")

    reports = ledger.get("phase_11_6_reports")
    if not isinstance(reports, list):
        errors.append("ledger.phase_11_6_reports must be a list")
    else:
        report_prompts: list[str] = []
        for index, report in enumerate(reports):
            prefix = f"phase_11_6_reports[{index}]"
            if not isinstance(report, dict):
                errors.append(f"{prefix} must be an object")
                continue
            if set(report) != {"prompt", "path", "status"}:
                errors.append(f"{prefix} must contain prompt, path, and status")
                continue
            prompt, path, status = report["prompt"], report["path"], report["status"]
            if not isinstance(prompt, str) or not isinstance(path, str) or not isinstance(status, str):
                errors.append(f"{prefix} values must be strings")
                continue
            report_prompts.append(prompt)
            expected_path = PHASE_11_6_REPORTS.get(prompt)
            if expected_path is None:
                errors.append(f"unknown Phase 11.6 report prompt: {prompt}")
            elif path != expected_path:
                errors.append(f"{prompt} report path must be {expected_path}")
            if not isinstance(status, str) or status not in ALLOWED_REPORT_STATUSES:
                errors.append(f"{prompt} report status {status!r} is not allowed")
            if expected_path is not None and not (ROOT / path).is_file():
                errors.append(f"required Phase 11.6 report is missing: {path}")
        duplicate_prompts = sorted({prompt for prompt in report_prompts if report_prompts.count(prompt) > 1})
        if duplicate_prompts:
            errors.append(f"duplicate Phase 11.6 report prompts: {', '.join(duplicate_prompts)}")
        missing_prompts = sorted(set(PHASE_11_6_REPORTS) - set(report_prompts))
        if missing_prompts:
            errors.append(f"missing Phase 11.6 report prompts: {', '.join(missing_prompts)}")

    evidence_gaps = ledger.get("evidence_gaps")
    if not isinstance(evidence_gaps, list):
        errors.append("ledger.evidence_gaps must be a list")
    else:
        gap_ids: list[str] = []
        for index, gap in enumerate(evidence_gaps):
            prefix = f"evidence_gaps[{index}]"
            if not isinstance(gap, dict) or set(gap) != {"id", "findings", "status", "required_evidence", "report", "test_references"}:
                errors.append(f"{prefix} must contain id, findings, status, required_evidence, report, and test_references")
                continue
            gap_id, gap_findings, gap_status, required_evidence, report, tests = (
                gap["id"], gap["findings"], gap["status"], gap["required_evidence"], gap["report"], gap["test_references"]
            )
            if not isinstance(gap_id, str) or not gap_id:
                errors.append(f"{prefix}.id must be a non-empty string")
            else:
                gap_ids.append(gap_id)
            if not isinstance(gap_findings, list) or not all(isinstance(item, str) and item for item in gap_findings):
                errors.append(f"{prefix}.findings must be a string list")
            if not isinstance(gap_status, str) or gap_status not in ALLOWED_STATUSES:
                errors.append(f"{prefix}.status {gap_status!r} is not allowed")
            if not isinstance(required_evidence, str) or not required_evidence.strip():
                errors.append(f"{prefix}.required_evidence must be a non-empty string")
            if report not in PHASE_11_6_REPORTS.values() or not (ROOT / str(report)).is_file():
                errors.append(f"{prefix}.report must name an existing required Phase 11.6 report")
            if not isinstance(tests, list) or not tests or not all(isinstance(test, str) and test for test in tests):
                errors.append(f"{prefix}.test_references requires named tests")
        duplicate_gap_ids = sorted({gap_id for gap_id in gap_ids if gap_ids.count(gap_id) > 1})
        if duplicate_gap_ids:
            errors.append(f"duplicate evidence gap IDs: {', '.join(duplicate_gap_ids)}")

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
        if not isinstance(status, str) or status not in ALLOWED_STATUSES:
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

    fully_evidenced = copy.deepcopy(ledger)
    for finding in fully_evidenced["findings"]:
        finding["status"] = "resolved"
        for evidence in finding["resolution_evidence"].values():
            if not evidence:
                evidence.append("self-test evidence")
    assert not validate(fully_evidenced), "fully evidenced ledger must validate"

    missing = copy.deepcopy(ledger)
    missing["findings"] = [finding for finding in missing["findings"] if finding["id"] != "B-01"]
    assert any("missing finding IDs: B-01" in error for error in validate(missing))

    duplicate = copy.deepcopy(ledger)
    duplicate["findings"].append(copy.deepcopy(duplicate["findings"][0]))
    assert any("duplicate finding IDs: B-01" in error for error in validate(duplicate))

    unresolved = copy.deepcopy(ledger)
    unresolved["findings"][0]["status"] = "resolved"
    for evidence in unresolved["findings"][0]["resolution_evidence"].values():
        evidence.clear()
    assert any("resolved finding B-01 requires test and supporting evidence" in error for error in validate(unresolved))

    unknown_status = copy.deepcopy(ledger)
    unknown_status["findings"][0]["status"] = "unknown"
    assert any("B-01.status 'unknown' is not allowed" in error for error in validate(unknown_status))

    missing_report = copy.deepcopy(ledger)
    missing_report["phase_11_6_reports"] = []
    assert any(
        "missing Phase 11.6 report prompts: 11.6.00, 11.6.01, 11.6.01b, 11.6.01c, 11.6.02, 11.6.03, 11.6.04, 11.6.05" in error
        for error in validate(missing_report)
    )

    missing_report_file = copy.deepcopy(ledger)
    missing_report_file["phase_11_6_reports"][0]["path"] = "docs/implementation/phase-11.6/missing.md"
    errors = validate(missing_report_file)
    assert any("11.6.00 report path must be" in error for error in errors)
    assert any("required Phase 11.6 report is missing" in error for error in errors)

    missing_gap_evidence = copy.deepcopy(ledger)
    missing_gap_evidence["evidence_gaps"][0]["test_references"] = []
    assert any("test_references requires named tests" in error for error in validate(missing_gap_evidence))
    missing_gap_evidence = copy.deepcopy(ledger)
    missing_gap_evidence["evidence_gaps"][0].pop("report")
    assert any("must contain id, findings, status, required_evidence, report, and test_references" in error for error in validate(missing_gap_evidence))

    assert release_blockers(ledger) == []

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
    blockers = release_blockers(ledger)
    print(f"phase 11.5 remediation check passed: {len(ledger['findings'])} findings tracked")
    print("remaining release blockers: " + ", ".join(blockers) if blockers else "remaining release blockers: none")


if __name__ == "__main__":
    main()
