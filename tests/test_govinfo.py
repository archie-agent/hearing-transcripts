"""Tests for GovInfo committee mapping in discover.py."""

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent.parent))

from discover import (
    Hearing,
    _fetch_govinfo_committee,
    _map_govinfo_to_committee,
    discover_govinfo,
)


# ---------------------------------------------------------------------------
# _map_govinfo_to_committee — title-based matching
# ---------------------------------------------------------------------------


class TestMapGovInfoToCommittee:
    """Test title-based committee extraction from GovInfo package titles."""

    def test_ways_and_means(self):
        title = "HEARING BEFORE THE COMMITTEE ON WAYS AND MEANS"
        assert _map_govinfo_to_committee(title, "house") == "house.ways_and_means"

    def test_finance_with_senate_suffix(self):
        title = "COMMITTEE ON FINANCE--UNITED STATES SENATE"
        assert _map_govinfo_to_committee(title, "senate") == "senate.finance"

    def test_banking_full_name(self):
        title = "HEARING BEFORE COMMITTEE ON BANKING, HOUSING, AND URBAN AFFAIRS"
        assert _map_govinfo_to_committee(title, "senate") == "senate.banking"

    def test_generic_title_returns_none(self):
        title = "NOMINATIONS"
        assert _map_govinfo_to_committee(title, "senate") is None

    def test_appropriations_in_title(self):
        # The title contains "APPROPRIATIONS" which should match
        title = "DEPARTMENT OF DEFENSE APPROPRIATIONS FOR FISCAL YEAR 2026"
        result = _map_govinfo_to_committee(title, "senate")
        assert result == "senate.appropriations"

    def test_case_insensitive(self):
        title = "hearing before the committee on ways and means"
        assert _map_govinfo_to_committee(title, "house") == "house.ways_and_means"

    def test_energy_and_commerce(self):
        title = "HEARING BEFORE THE COMMITTEE ON ENERGY AND COMMERCE"
        assert _map_govinfo_to_committee(title, "house") == "house.energy_commerce"

    def test_judiciary_house(self):
        title = "HEARING BEFORE THE COMMITTEE ON THE JUDICIARY"
        assert _map_govinfo_to_committee(title, "house") == "house.judiciary"

    def test_judiciary_senate(self):
        title = "HEARING BEFORE THE COMMITTEE ON THE JUDICIARY"
        assert _map_govinfo_to_committee(title, "senate") == "senate.judiciary"

    def test_armed_services_senate(self):
        title = "DEPARTMENT OF DEFENSE AUTHORIZATION FOR APPROPRIATIONS FOR FISCAL YEAR 2026 BEFORE THE COMMITTEE ON ARMED SERVICES"
        assert _map_govinfo_to_committee(title, "senate") == "senate.armed_services"

    def test_chamber_filtering(self):
        """Chamber constraint should prevent cross-chamber matches."""
        title = "HEARING BEFORE THE COMMITTEE ON FINANCE"
        # "finance" only exists as senate.finance, so house chamber should not match
        result = _map_govinfo_to_committee(title, "house")
        # house.financial_services has "financial services", not "finance"
        assert result is None

    def test_unknown_chamber_matches_any(self):
        """With unknown chamber, any matching committee should be returned."""
        title = "HEARING BEFORE THE COMMITTEE ON WAYS AND MEANS"
        result = _map_govinfo_to_committee(title, "unknown")
        assert result == "house.ways_and_means"

    def test_environment_and_public_works(self):
        title = "HEARING BEFORE THE COMMITTEE ON ENVIRONMENT AND PUBLIC WORKS"
        assert _map_govinfo_to_committee(title, "senate") == "senate.environment"

    def test_homeland_security_senate(self):
        title = "HEARING BEFORE THE COMMITTEE ON HOMELAND SECURITY AND GOVERNMENTAL AFFAIRS"
        assert _map_govinfo_to_committee(title, "senate") == "senate.homeland_security"

    def test_empty_title(self):
        assert _map_govinfo_to_committee("", "house") is None

    def test_budget_house(self):
        title = "THE FISCAL YEAR 2026 BUDGET BEFORE THE COMMITTEE ON THE BUDGET"
        assert _map_govinfo_to_committee(title, "house") == "house.budget"


# ---------------------------------------------------------------------------
# discover_govinfo — integration of title mapping with fallback
# ---------------------------------------------------------------------------


class TestDiscoverGovInfoFallback:
    """Test that unmappable titles fall back to govinfo.house / govinfo.senate."""

    def _make_response(self, packages):
        """Create a mock httpx.Response with the given packages."""
        resp = MagicMock()
        resp.json.return_value = {"packages": packages}
        return resp

    @patch("discover._http_get")
    def test_mapped_title_uses_committee_key(self, mock_get):
        mock_get.return_value = self._make_response([{
            "packageId": "CHRG-119hhrg12345",
            "dateIssued": "2026-02-01",
            "title": "HEARING BEFORE THE COMMITTEE ON WAYS AND MEANS",
        }])
        hearings = discover_govinfo(days=7)
        assert len(hearings) == 1
        assert hearings[0].committee_key == "house.ways_and_means"
        assert hearings[0].committee_name == "House Ways and Means"

    @patch("discover._http_get")
    def test_unmapped_title_falls_back_to_generic_house(self, mock_get):
        mock_get.return_value = self._make_response([{
            "packageId": "CHRG-119hhrg99999",
            "dateIssued": "2026-02-01",
            "title": "NOMINATIONS",
        }])
        hearings = discover_govinfo(days=7)
        assert len(hearings) == 1
        assert hearings[0].committee_key == "govinfo.house"
        assert hearings[0].committee_name == "House (via GovInfo)"

    @patch("discover._http_get")
    def test_unmapped_title_falls_back_to_generic_senate(self, mock_get):
        mock_get.return_value = self._make_response([{
            "packageId": "CHRG-119shrg88888",
            "dateIssued": "2026-02-01",
            "title": "NOMINATIONS",
        }])
        hearings = discover_govinfo(days=7)
        assert len(hearings) == 1
        assert hearings[0].committee_key == "govinfo.senate"
        assert hearings[0].committee_name == "Senate (via GovInfo)"

    @patch("discover._http_get")
    def test_mixed_mapped_and_unmapped(self, mock_get):
        mock_get.return_value = self._make_response([
            {
                "packageId": "CHRG-119shrg11111",
                "dateIssued": "2026-02-01",
                "title": "HEARING BEFORE THE COMMITTEE ON FINANCE--UNITED STATES SENATE",
            },
            {
                "packageId": "CHRG-119shrg22222",
                "dateIssued": "2026-02-02",
                "title": "EXECUTIVE SESSION",
            },
        ])
        hearings = discover_govinfo(days=7)
        assert len(hearings) == 2
        assert hearings[0].committee_key == "senate.finance"
        assert hearings[1].committee_key == "govinfo.senate"

    @patch("discover._http_get")
    def test_govinfo_package_id_in_sources(self, mock_get):
        mock_get.return_value = self._make_response([{
            "packageId": "CHRG-119hhrg55555",
            "dateIssued": "2026-01-15",
            "title": "HEARING BEFORE THE COMMITTEE ON THE BUDGET",
        }])
        hearings = discover_govinfo(days=7)
        assert hearings[0].sources == {"govinfo_package_id": "CHRG-119hhrg55555"}


# ---------------------------------------------------------------------------
# _fetch_govinfo_committee — summary endpoint fallback
# ---------------------------------------------------------------------------


class TestFetchGovInfoCommittee:
    """Test the summary endpoint fallback for committee extraction."""

    @patch("discover._http_get")
    def test_committee_from_summary_committees_field(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {
            "title": "NOMINATIONS",
            "committees": [
                {"committeeName": "Committee on Finance"}
            ],
        }
        mock_get.return_value = resp
        result = _fetch_govinfo_committee("CHRG-119shrg12345")
        assert result == "senate.finance"

    @patch("discover._http_get")
    def test_committee_from_summary_string_list(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {
            "title": "NOMINATIONS",
            "committees": ["Committee on Ways and Means"],
        }
        mock_get.return_value = resp
        result = _fetch_govinfo_committee("CHRG-119hhrg12345")
        assert result == "house.ways_and_means"

    @patch("discover._http_get")
    def test_fallback_to_summary_title(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = {
            "title": "HEARING BEFORE THE COMMITTEE ON THE JUDICIARY",
            "committees": [],
        }
        mock_get.return_value = resp
        result = _fetch_govinfo_committee("CHRG-119shrg12345")
        assert result == "senate.judiciary"

    @patch("discover._http_get")
    def test_returns_none_on_failure(self, mock_get):
        mock_get.return_value = None
        result = _fetch_govinfo_committee("CHRG-119shrg12345")
        assert result is None

    @patch("discover._http_get")
    def test_returns_none_on_non_json(self, mock_get):
        resp = MagicMock()
        resp.json.side_effect = ValueError("not JSON")
        mock_get.return_value = resp
        result = _fetch_govinfo_committee("CHRG-119shrg12345")
        assert result is None

    @patch("discover._http_get")
    def test_detail_fetch_not_called_by_default(self, mock_get):
        """With GOVINFO_FETCH_DETAILS unset (default false), summary endpoint not called."""
        mock_get.return_value = MagicMock()
        mock_get.return_value.json.return_value = {"packages": [{
            "packageId": "CHRG-119shrg77777",
            "dateIssued": "2026-02-01",
            "title": "NOMINATIONS",
        }]}
        # Ensure env var is not set
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("GOVINFO_FETCH_DETAILS", None)
            hearings = discover_govinfo(days=7)
        # Should only call _http_get once (for the collections endpoint),
        # not a second time for the summary
        assert mock_get.call_count == 1
        assert hearings[0].committee_key == "govinfo.senate"

    @patch("discover._http_get")
    def test_detail_fetch_called_when_enabled(self, mock_get):
        """With GOVINFO_FETCH_DETAILS=true, summary endpoint IS called for unmapped packages."""
        collections_resp = MagicMock()
        collections_resp.json.return_value = {"packages": [{
            "packageId": "CHRG-119shrg77777",
            "dateIssued": "2026-02-01",
            "title": "NOMINATIONS",
        }]}
        summary_resp = MagicMock()
        summary_resp.json.return_value = {
            "title": "NOMINATIONS",
            "committees": [{"committeeName": "Committee on Banking, Housing, and Urban Affairs"}],
        }
        mock_get.side_effect = [collections_resp, summary_resp]

        with patch.dict(os.environ, {"GOVINFO_FETCH_DETAILS": "true"}):
            hearings = discover_govinfo(days=7)

        assert mock_get.call_count == 2
        assert hearings[0].committee_key == "senate.banking"
