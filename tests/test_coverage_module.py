"""Tests for coverage.py — coverage analysis CLI."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


class TestCoverageMain:

    def test_default_args_pass_skip_cspan_true(self, monkeypatch):
        """Default invocation (no args) calls discover_all with skip_cspan=True."""
        monkeypatch.setattr("sys.argv", ["coverage.py"])

        with patch("coverage.discover_all", return_value=[]) as mock_discover, \
             patch("coverage.State") as mock_state_cls, \
             patch("coverage.config") as mock_config:
            mock_config.get_committees.return_value = {"house.judiciary": {"tier": 1}}
            mock_state_cls.return_value = MagicMock()

            from coverage import main
            main()

        mock_discover.assert_called_once()
        call_kwargs = mock_discover.call_args
        assert call_kwargs.kwargs.get("skip_cspan") is True

    def test_with_cspan_flag_overrides_skip(self, monkeypatch):
        """--with-cspan flag causes skip_cspan=False."""
        monkeypatch.setattr("sys.argv", ["coverage.py", "--with-cspan"])

        with patch("coverage.discover_all", return_value=[]) as mock_discover, \
             patch("coverage.State") as mock_state_cls, \
             patch("coverage.config") as mock_config:
            mock_config.get_committees.return_value = {"house.judiciary": {"tier": 1}}
            mock_state_cls.return_value = MagicMock()

            from coverage import main
            main()

        mock_discover.assert_called_once()
        call_kwargs = mock_discover.call_args
        assert call_kwargs.kwargs.get("skip_cspan") is False

    def test_custom_days(self, monkeypatch):
        """--days N is passed through to discover_all."""
        monkeypatch.setattr("sys.argv", ["coverage.py", "--days", "7"])

        with patch("coverage.discover_all", return_value=[]) as mock_discover, \
             patch("coverage.State") as mock_state_cls, \
             patch("coverage.config") as mock_config:
            mock_config.get_committees.return_value = {"senate.finance": {"tier": 1}}
            mock_state_cls.return_value = MagicMock()

            from coverage import main
            main()

        mock_discover.assert_called_once()
        call_kwargs = mock_discover.call_args
        assert call_kwargs.kwargs.get("days") == 7

    def test_returns_without_error_on_empty_hearings(self, monkeypatch, capsys):
        """When discover_all returns [], main prints the table and doesn't crash."""
        monkeypatch.setattr("sys.argv", ["coverage.py"])

        with patch("coverage.discover_all", return_value=[]) as mock_discover, \
             patch("coverage.State") as mock_state_cls, \
             patch("coverage.config") as mock_config:
            mock_config.get_committees.return_value = {}
            mock_state_cls.return_value = MagicMock()

            from coverage import main
            main()

        captured = capsys.readouterr()
        assert "No hearings found" in captured.out
