#!/usr/bin/env python3
"""Tests for scripts/ci_changed.py."""

import unittest

import ci_changed


class CiChangedTests(unittest.TestCase):
    def classify(self, *paths: str) -> dict[str, bool]:
        return ci_changed.classify_paths(paths)

    def test_rename_classification_keeps_old_and_new_paths(self) -> None:
        paths = ci_changed.parse_name_status(
            "R100\tapps/web/src/old.ts\tdocs/old.ts\n"
        )
        self.assertEqual(paths, ["apps/web/src/old.ts", "docs/old.ts"])
        self.assertTrue(ci_changed.classify_paths(paths)["web"])

    def test_web_only_change_runs_only_web(self) -> None:
        result = self.classify("apps/web/src/pages/DevicesPage.tsx")
        self.assertTrue(result["web"])
        self.assertFalse(result["core"])
        self.assertFalse(result["desktop"])
        self.assertFalse(result["galaxy"])

    def test_galaxy_change_runs_galaxy_and_web(self) -> None:
        result = self.classify("crates/galaxy-renderer/src/lib.rs")
        self.assertTrue(result["galaxy"])
        self.assertTrue(result["web"])
        self.assertFalse(result["core"])

    def test_core_change_runs_core_and_policy(self) -> None:
        result = self.classify("src/managed/store.rs")
        self.assertTrue(result["core"])
        self.assertTrue(result["policy"])
        self.assertFalse(result["web"])

    def test_non_galaxy_workspace_crate_is_core(self) -> None:
        result = self.classify("crates/replicant-runtime/src/orchestration.rs")
        self.assertTrue(result["core"])
        self.assertFalse(result["galaxy"])

    def test_desktop_only_change_does_not_run_web(self) -> None:
        result = self.classify("apps/desktop/src-tauri/src/main.rs")
        self.assertTrue(result["desktop"])
        self.assertFalse(result["web"])
        self.assertFalse(result["core"])

    def test_root_rust_lint_config_reaches_all_rust_domains(self) -> None:
        result = self.classify("clippy.toml")
        self.assertTrue(result["core"])
        self.assertTrue(result["desktop"])
        self.assertTrue(result["galaxy"])
        self.assertFalse(result["web"])

    def test_root_cargo_lock_reaches_core_and_desktop(self) -> None:
        result = self.classify("Cargo.lock")
        self.assertTrue(result["core"])
        self.assertTrue(result["desktop"])
        self.assertFalse(result["galaxy"])

    def test_reference_snapshot_runs_policy(self) -> None:
        result = self.classify("reference/replicant-space-3.0.0/openapi.json")
        self.assertTrue(result["policy"])
        self.assertFalse(result["docs"])

    def test_crawler_change_runs_docs(self) -> None:
        result = self.classify("reference/replicant-docs-crawler/crawl_replicant_docs.py")
        self.assertTrue(result["docs"])
        self.assertFalse(result["policy"])

    def test_compose_change_runs_docker(self) -> None:
        result = self.classify("compose.headless.yaml")
        self.assertTrue(result["docker"])
        self.assertFalse(result["core"])

    def test_web_container_file_does_not_run_web_application_ci(self) -> None:
        result = self.classify("apps/web/Dockerfile")
        self.assertTrue(result["docker"])
        self.assertFalse(result["web"])

    def test_scripts_readme_is_docs_only(self) -> None:
        result = self.classify("scripts/README.md")
        self.assertFalse(any(result.values()))

    def test_build_orchestration_change_runs_everything(self) -> None:
        result = self.classify("Makefile")
        self.assertTrue(all(result.values()))

    def test_workflow_change_runs_everything(self) -> None:
        result = self.classify(".github/workflows/ci.yml")
        self.assertTrue(all(result.values()))

    def test_docs_only_change_runs_no_build_domain(self) -> None:
        result = self.classify("docs/design-notes.md")
        self.assertFalse(any(result.values()))


if __name__ == "__main__":
    unittest.main()
