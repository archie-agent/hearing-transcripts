"""Tests for config.py — API key getters, MODEL_PRICING validation, committee loading, defaults."""

from __future__ import annotations

import logging
import subprocess
import sys

import pytest

import config


# ---------------------------------------------------------------------------
# API key getters
# ---------------------------------------------------------------------------

class TestGetOpenaiApiKey:
    def test_returns_env_value(self, monkeypatch):
        monkeypatch.setenv("OPENAI_API_KEY", "sk-test-123")
        assert config.get_openai_api_key() == "sk-test-123"

    def test_returns_empty_when_unset(self, monkeypatch):
        monkeypatch.delenv("OPENAI_API_KEY", raising=False)
        assert config.get_openai_api_key() == ""


class TestGetOpenrouterApiKey:
    def test_returns_env_value(self, monkeypatch):
        monkeypatch.setenv("OPENROUTER_API_KEY", "or-key-abc")
        assert config.get_openrouter_api_key() == "or-key-abc"

    def test_returns_empty_when_unset(self, monkeypatch):
        monkeypatch.delenv("OPENROUTER_API_KEY", raising=False)
        assert config.get_openrouter_api_key() == ""


class TestGetGovInfoApiKey:
    def test_returns_env_value(self, monkeypatch):
        monkeypatch.setenv("GOVINFO_API_KEY", "real-key-xyz")
        assert config.get_govinfo_api_key() == "real-key-xyz"

    def test_returns_demo_key_when_unset(self, monkeypatch):
        monkeypatch.delenv("GOVINFO_API_KEY", raising=False)
        # Reset the warning flag so DEMO_KEY path is exercised
        monkeypatch.setattr(config, "_demo_key_warned", True)
        assert config.get_govinfo_api_key() == "DEMO_KEY"

    def test_demo_key_warns_on_first_call(self, monkeypatch, caplog):
        monkeypatch.delenv("GOVINFO_API_KEY", raising=False)
        monkeypatch.setattr(config, "_demo_key_warned", False)

        with caplog.at_level(logging.WARNING, logger="config"):
            result = config.get_govinfo_api_key()

        assert result == "DEMO_KEY"
        assert config._demo_key_warned is True
        assert "DEMO_KEY" in caplog.text

    def test_demo_key_does_not_warn_on_subsequent_calls(self, monkeypatch, caplog):
        monkeypatch.delenv("GOVINFO_API_KEY", raising=False)
        # Simulate that the first-call warning already fired
        monkeypatch.setattr(config, "_demo_key_warned", True)

        with caplog.at_level(logging.WARNING, logger="config"):
            config.get_govinfo_api_key()

        assert "DEMO_KEY" not in caplog.text

    def test_real_key_does_not_warn(self, monkeypatch, caplog):
        monkeypatch.setenv("GOVINFO_API_KEY", "real-key")
        monkeypatch.setattr(config, "_demo_key_warned", False)

        with caplog.at_level(logging.WARNING, logger="config"):
            config.get_govinfo_api_key()

        assert "DEMO_KEY" not in caplog.text


class TestGetCongressApiKey:
    def test_returns_own_env_value(self, monkeypatch):
        monkeypatch.setenv("CONGRESS_API_KEY", "congress-key-1")
        assert config.get_congress_api_key() == "congress-key-1"

    def test_falls_back_to_govinfo_key(self, monkeypatch):
        monkeypatch.delenv("CONGRESS_API_KEY", raising=False)
        monkeypatch.setenv("GOVINFO_API_KEY", "govinfo-key-2")
        assert config.get_congress_api_key() == "govinfo-key-2"

    def test_falls_back_to_demo_key(self, monkeypatch):
        monkeypatch.delenv("CONGRESS_API_KEY", raising=False)
        monkeypatch.delenv("GOVINFO_API_KEY", raising=False)
        # Suppress the DEMO_KEY warning for this test
        monkeypatch.setattr(config, "_demo_key_warned", True)
        assert config.get_congress_api_key() == "DEMO_KEY"


# ---------------------------------------------------------------------------
# MODEL_PRICING validation
# ---------------------------------------------------------------------------

class TestModelPricingValidation:
    """The validation loop in config.py runs at import time. We test it by
    spawning a subprocess that patches MODEL_PRICING before the validation
    runs, verifying it raises ValueError for bad values."""

    def _run_import_with_pricing(self, pricing_repr: str, tmp_path) -> subprocess.CompletedProcess:
        """Write a temp script that patches MODEL_PRICING in config source, then exec it."""
        script = tmp_path / "test_pricing.py"
        script.write_text(
            "import sys, types, re\n"
            "fake_dotenv = types.ModuleType('dotenv')\n"
            "fake_dotenv.load_dotenv = lambda *a, **kw: None\n"
            "sys.modules['dotenv'] = fake_dotenv\n"
            "with open('config.py') as f:\n"
            "    source = f.read()\n"
            "source = re.sub(\n"
            "    r'MODEL_PRICING:.*?^\\}',\n"
            "    'MODEL_PRICING: dict[str, tuple[float, float]] = '\n"
            f"    + {pricing_repr!r}\n"
            "    + '\\n',\n"
            "    source,\n"
            "    flags=re.DOTALL | re.MULTILINE,\n"
            ")\n"
            "exec(compile(source, 'config.py', 'exec'), {'__name__': 'config', '__file__': 'config.py'})\n",
            encoding="utf-8",
        )
        return subprocess.run(
            [sys.executable, str(script)],
            capture_output=True,
            text=True,
            cwd=str(config.ROOT),
        )

    def test_negative_input_price_rejected(self, tmp_path):
        result = self._run_import_with_pricing('{"bad/model": (-1.0, 0.5)}', tmp_path)
        assert result.returncode != 0
        assert "input price must be non-negative" in result.stderr

    def test_negative_output_price_rejected(self, tmp_path):
        result = self._run_import_with_pricing('{"bad/model": (0.5, -1.0)}', tmp_path)
        assert result.returncode != 0
        assert "output price must be non-negative" in result.stderr

    def test_string_input_price_rejected(self, tmp_path):
        result = self._run_import_with_pricing('{"bad/model": ("free", 0.5)}', tmp_path)
        assert result.returncode != 0
        assert "input price must be non-negative" in result.stderr

    def test_string_output_price_rejected(self, tmp_path):
        result = self._run_import_with_pricing('{"bad/model": (0.5, "free")}', tmp_path)
        assert result.returncode != 0
        assert "output price must be non-negative" in result.stderr

    def test_valid_pricing_accepted(self, tmp_path):
        result = self._run_import_with_pricing('{"ok/model": (0.10, 0.40)}', tmp_path)
        assert result.returncode == 0

    def test_zero_prices_accepted(self, tmp_path):
        result = self._run_import_with_pricing('{"free/model": (0, 0)}', tmp_path)
        assert result.returncode == 0

    def test_existing_pricing_is_valid(self):
        """Sanity check: the actual MODEL_PRICING in config.py passes validation."""
        for model_name, (in_price, out_price) in config.MODEL_PRICING.items():
            assert isinstance(in_price, (int, float)) and in_price >= 0, (
                f"{model_name} input price invalid: {in_price}"
            )
            assert isinstance(out_price, (int, float)) and out_price >= 0, (
                f"{model_name} output price invalid: {out_price}"
            )


# ---------------------------------------------------------------------------
# Committee loading and tier filtering
# ---------------------------------------------------------------------------

class TestGetCommittees:
    def test_returns_all_by_default(self):
        all_committees = config.get_all_committees()
        filtered = config.get_committees()
        # max_tier=99 means everything passes
        assert filtered == all_committees

    def test_tier_1_only(self):
        tier1 = config.get_committees(max_tier=1)
        assert len(tier1) > 0
        for key, data in tier1.items():
            assert data.get("tier", 3) <= 1

    def test_tier_2_includes_tier_1(self):
        tier1 = config.get_committees(max_tier=1)
        tier2 = config.get_committees(max_tier=2)
        assert len(tier2) >= len(tier1)
        for key in tier1:
            assert key in tier2

    def test_tier_0_returns_empty(self):
        result = config.get_committees(max_tier=0)
        assert result == {}

    def test_tier_3_includes_all(self):
        tier3 = config.get_committees(max_tier=3)
        all_committees = config.get_all_committees()
        assert tier3 == all_committees

    def test_committees_have_required_fields(self):
        for key, data in config.get_all_committees().items():
            assert "name" in data, f"{key} missing 'name'"
            assert "chamber" in data, f"{key} missing 'chamber'"
            assert "tier" in data, f"{key} missing 'tier'"


class TestGetCommitteeMeta:
    def test_known_committee(self):
        meta = config.get_committee_meta("senate.finance")
        assert meta is not None
        assert meta["name"] == "Senate Finance"
        assert meta["chamber"] == "senate"

    def test_unknown_committee(self):
        meta = config.get_committee_meta("senate.nonexistent")
        assert meta is None


class TestLoadCommitteesErrors:
    def test_missing_file_raises(self, monkeypatch, tmp_path):
        monkeypatch.setattr(config, "COMMITTEES_JSON", tmp_path / "missing.json")
        # Clear cache so _load_committees is called again
        monkeypatch.setattr(config, "_committees_cache", None)
        with pytest.raises(ValueError, match="not found"):
            config.get_all_committees()

    def test_corrupt_json_raises(self, monkeypatch, tmp_path):
        bad_file = tmp_path / "bad.json"
        bad_file.write_text("{not valid json", encoding="utf-8")
        monkeypatch.setattr(config, "COMMITTEES_JSON", bad_file)
        monkeypatch.setattr(config, "_committees_cache", None)
        with pytest.raises(ValueError, match="corrupt"):
            config.get_all_committees()


# ---------------------------------------------------------------------------
# Defaults and constants
# ---------------------------------------------------------------------------

class TestDefaults:
    def test_agentmail_sender_default(self):
        assert config.AGENTMAIL_SENDER == "archie-agent@agentmail.to"

    def test_transcription_backend_default(self, monkeypatch):
        # The module-level default; verify it exists and is one of the valid options
        assert config.TRANSCRIPTION_BACKEND in ("captions-only", "openai")

    def test_max_cost_per_run_is_positive(self):
        assert config.MAX_COST_PER_RUN > 0

    def test_congress_is_reasonable(self):
        assert 119 <= config.CONGRESS <= 125

    def test_cleanup_model_default(self):
        # CLEANUP_MODEL has a non-empty default unless overridden
        assert isinstance(config.CLEANUP_MODEL, str)
