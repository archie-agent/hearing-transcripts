"""Tests for alerts.py — alert formatting and check_and_alert flow."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from alerts import _format_alert, check_and_alert


# ---------------------------------------------------------------------------
# _format_alert
# ---------------------------------------------------------------------------

class TestFormatAlert:

    def test_empty_list(self):
        """Empty failing list produces a header line with count 0."""
        result = _format_alert([])
        assert "0 source(s) failing" in result

    def test_single_failure(self):
        """One failing scraper is formatted with all expected fields."""
        failing = [{
            "committee_key": "house.judiciary",
            "source_type": "youtube",
            "consecutive_failures": 5,
            "last_success": "2026-02-01T12:00:00",
            "last_failure": "2026-02-10T08:30:00",
        }]
        result = _format_alert(failing)

        assert "1 source(s) failing" in result
        assert "house.judiciary / youtube" in result
        assert "Consecutive failures : 5" in result
        assert "2026-02-01T12:00:00" in result
        assert "2026-02-10T08:30:00" in result

    def test_multiple_failures(self):
        """Multiple scrapers each get their own section."""
        failing = [
            {
                "committee_key": "house.judiciary",
                "source_type": "youtube",
                "consecutive_failures": 3,
                "last_success": "2026-01-15",
                "last_failure": "2026-02-10",
            },
            {
                "committee_key": "senate.finance",
                "source_type": "congress_api",
                "consecutive_failures": 7,
                "last_success": None,
                "last_failure": None,
            },
        ]
        result = _format_alert(failing)

        assert "2 source(s) failing" in result
        assert "house.judiciary / youtube" in result
        assert "senate.finance / congress_api" in result

    def test_missing_optional_fields(self):
        """last_success and last_failure may be absent or None."""
        failing = [{
            "committee_key": "senate.banking",
            "source_type": "website",
            "consecutive_failures": 4,
        }]
        result = _format_alert(failing)

        assert "senate.banking / website" in result
        assert "Last success         : never" in result
        assert "Last failure          : unknown" in result


# ---------------------------------------------------------------------------
# check_and_alert
# ---------------------------------------------------------------------------

class TestCheckAndAlert:

    def test_returns_empty_when_no_failures(self, tmp_path):
        """No failing scrapers => returns empty list, no alert file written."""
        state = MagicMock()
        state.get_failing_scrapers.return_value = []

        result = check_and_alert(state, threshold=3)

        assert result == []

    def test_returns_empty_when_precomputed_failing_empty(self, tmp_path):
        """Passing empty failing list directly => returns empty, no DB query."""
        state = MagicMock()

        result = check_and_alert(state, failing=[])

        assert result == []
        state.get_failing_scrapers.assert_not_called()

    def test_writes_alert_file_on_failures(self, tmp_path):
        """Failing scrapers => writes alert file and returns the list."""
        state = MagicMock()
        failing = [{
            "committee_key": "house.ways_and_means",
            "source_type": "youtube",
            "consecutive_failures": 5,
            "last_success": "2026-01-20",
            "last_failure": "2026-02-10",
        }]

        with patch("alerts._write_alert_file") as mock_write, \
             patch.dict("os.environ", {}, clear=False):
            # Ensure SLACK_WEBHOOK_URL is not set
            import os
            os.environ.pop("SLACK_WEBHOOK_URL", None)
            result = check_and_alert(state, failing=failing)

        assert result == failing
        mock_write.assert_called_once()
        # Verify the alert message content was passed
        written_message = mock_write.call_args[0][0]
        assert "house.ways_and_means" in written_message

    def test_queries_state_when_failing_not_provided(self):
        """When failing=None, queries state.get_failing_scrapers."""
        state = MagicMock()
        state.get_failing_scrapers.return_value = []

        check_and_alert(state, threshold=5, failing=None)

        state.get_failing_scrapers.assert_called_once_with(threshold=5)

    def test_posts_to_slack_when_webhook_set(self):
        """If SLACK_WEBHOOK_URL is set, posts the alert to Slack."""
        state = MagicMock()
        failing = [{
            "committee_key": "senate.finance",
            "source_type": "congress_api",
            "consecutive_failures": 3,
            "last_success": None,
            "last_failure": "2026-02-12",
        }]

        with patch("alerts._write_alert_file"), \
             patch("alerts._post_to_slack") as mock_slack, \
             patch.dict("os.environ", {"SLACK_WEBHOOK_URL": "https://hooks.slack.com/test"}):
            check_and_alert(state, failing=failing)

        mock_slack.assert_called_once()
        url_arg = mock_slack.call_args[0][1]
        assert url_arg == "https://hooks.slack.com/test"

    def test_no_slack_when_webhook_not_set(self):
        """If SLACK_WEBHOOK_URL is not set, should not attempt Slack post."""
        state = MagicMock()
        failing = [{
            "committee_key": "house.judiciary",
            "source_type": "youtube",
            "consecutive_failures": 4,
            "last_success": "2026-02-01",
            "last_failure": "2026-02-10",
        }]

        with patch("alerts._write_alert_file"), \
             patch("alerts._post_to_slack") as mock_slack, \
             patch.dict("os.environ", {}, clear=False):
            import os
            os.environ.pop("SLACK_WEBHOOK_URL", None)
            check_and_alert(state, failing=failing)

        mock_slack.assert_not_called()
