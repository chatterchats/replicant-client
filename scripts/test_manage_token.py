#!/usr/bin/env python3
"""Tests for scripts/manage_token.py."""

from pathlib import Path
import tempfile
import unittest

import manage_token


class ManageTokenTests(unittest.TestCase):
    def test_creates_env_from_example_and_sets_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = root / ".env"
            example = root / ".env.example"
            example.write_text("RS_API_TOKEN=placeholder\nREPLICANTD_TOKEN=\n")

            action = manage_token.update_token(
                env,
                example,
                token_factory=lambda: "generated-token",
            )

            self.assertEqual(action, "created-from-example")
            self.assertEqual(
                env.read_text(),
                "RS_API_TOKEN=placeholder\nREPLICANTD_TOKEN=generated-token\n",
            )

    def test_existing_token_is_preserved_without_rotate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = root / ".env"
            example = root / ".env.example"
            env.write_text("REPLICANTD_TOKEN=keep-me\n")
            example.write_text("REPLICANTD_TOKEN=\n")

            action = manage_token.update_token(
                env,
                example,
                token_factory=lambda: "must-not-be-used",
            )

            self.assertEqual(action, "existing")
            self.assertEqual(env.read_text(), "REPLICANTD_TOKEN=keep-me\n")

    def test_rotate_replaces_existing_token(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            env = root / ".env"
            example = root / ".env.example"
            env.write_text("OTHER=value\nREPLICANTD_TOKEN=old\n")
            example.write_text("REPLICANTD_TOKEN=\n")

            action = manage_token.update_token(
                env,
                example,
                rotate=True,
                token_factory=lambda: "new-token",
            )

            self.assertEqual(action, "rotated")
            self.assertEqual(env.read_text(), "OTHER=value\nREPLICANTD_TOKEN=new-token\n")

    def test_missing_env_and_example_is_an_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with self.assertRaises(FileNotFoundError):
                manage_token.update_token(root / ".env", root / ".env.example")


if __name__ == "__main__":
    unittest.main()
