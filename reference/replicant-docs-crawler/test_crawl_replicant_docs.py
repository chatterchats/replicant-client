#!/usr/bin/env python3
"""Regression tests for the documentation crawler."""

from __future__ import annotations

import unittest
from unittest.mock import Mock

from bs4 import BeautifulSoup

from crawl_replicant_docs import (
    choose_article,
    clean_article,
    detect_latest_release_version,
    html_to_markdown,
)


CHANGED_CHANGELOG_HTML = """
<!doctype html>
<html lang="en">
  <body>
    <main>
      <h1>Changelog</h1>
      <ol class="changelog-list">
        <li class="changelog-entry">
          <details class="entry-details" data-version="2.5.1">
            <summary class="entry-summary">
              <span class="entry-version">v2.5.1</span>
              <time datetime="2026-08-21">21 August 2026</time>
            </summary>
            <article class="entry-body">
              <h3 id="api-updates">API updates</h3>
              <p>New filters are available.</p>
            </article>
          </details>
        </li>
        <li class="changelog-entry" id="v2.5.0">
          <details class="entry-details" data-version="2.5.0">
            <summary class="entry-summary">
              <span class="entry-version">v2.5.0</span>
              <time datetime="2026-08-10">10 August 2026</time>
            </summary>
            <article class="entry-body">
              <p>The new player experience changed.</p>
            </article>
          </details>
        </li>
      </ol>
    </main>
  </body>
</html>
"""


class ChangelogFormatTests(unittest.TestCase):
    def test_data_version_determines_latest_version_without_release_headings(self) -> None:
        response = Mock()
        response.text = CHANGED_CHANGELOG_HTML
        response.raise_for_status.return_value = None
        session = Mock()
        session.get.return_value = response

        version = detect_latest_release_version(
            session, "https://replicant.space/changelog/", 30.0
        )

        self.assertEqual(version, (2, 5, 1))

    def test_details_entries_are_preserved_in_markdown(self) -> None:
        soup = BeautifulSoup(CHANGED_CHANGELOG_HTML, "html.parser")

        markdown = html_to_markdown(clean_article(choose_article(soup)))

        self.assertIn("v2.5.1", markdown)
        self.assertIn("21 August 2026", markdown)
        self.assertIn("### API updates", markdown)
        self.assertIn("New filters are available.", markdown)
        self.assertIn("v2.5.0", markdown)
        self.assertIn("The new player experience changed.", markdown)


if __name__ == "__main__":
    unittest.main()
