"""Tests for fabric_metadata_dags.fabric_client — Fabric REST API client."""

from __future__ import annotations

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from fabric_metadata_dags import fabric_client
from fabric_metadata_dags.fabric_client import (
    _cache_path,
    _read_cache,
    _write_cache,
    _get_access_token,
    _list_notebooks,
    _resolve_workspace_id,
    get_workspace_notebooks,
)


# ---------------------------------------------------------------------------
# Token acquisition
# ---------------------------------------------------------------------------


class TestGetAccessToken:
    def test_returns_token_string(self):
        mock_token = MagicMock()
        mock_token.token = "my-token"
        with patch(
            "fabric_metadata_dags.fabric_client.AzureCliCredential"
        ) as mock_cred:
            mock_cred.return_value.get_token.return_value = mock_token
            result = _get_access_token()
        assert result == "my-token"

    def test_raises_runtime_error_on_auth_failure(self):
        with patch(
            "fabric_metadata_dags.fabric_client.AzureCliCredential"
        ) as mock_cred:
            mock_cred.return_value.get_token.side_effect = Exception("not logged in")
            with pytest.raises(RuntimeError, match="az login"):
                _get_access_token()


# ---------------------------------------------------------------------------
# Workspace resolution
# ---------------------------------------------------------------------------


class TestResolveWorkspaceId:
    def _mock_response(
        self, workspaces: list[dict], continuation_uri: str | None = None
    ):
        body = {"value": workspaces}
        if continuation_uri:
            body["continuationUri"] = continuation_uri
        mock = MagicMock()
        mock.json.return_value = body
        mock.raise_for_status = MagicMock()
        return mock

    def test_returns_id_for_matching_workspace(self):
        workspaces = [
            {"id": "ws-1", "displayName": "Other Workspace"},
            {"id": "ws-2", "displayName": "My Workspace"},
        ]
        with patch("fabric_metadata_dags.fabric_client.requests.get") as mock_get:
            mock_get.return_value = self._mock_response(workspaces)
            result = _resolve_workspace_id("token", "My Workspace")
        assert result == "ws-2"

    def test_raises_value_error_when_not_found(self):
        with patch("fabric_metadata_dags.fabric_client.requests.get") as mock_get:
            mock_get.return_value = self._mock_response([])
            with pytest.raises(ValueError, match="My Workspace"):
                _resolve_workspace_id("token", "My Workspace")

    def test_follows_pagination(self):
        page1_response = MagicMock()
        page1_response.json.return_value = {
            "value": [{"id": "ws-1", "displayName": "Other"}],
            "continuationUri": "https://api.fabric.microsoft.com/v1/workspaces?page=2",
        }
        page1_response.raise_for_status = MagicMock()

        page2_response = MagicMock()
        page2_response.json.return_value = {
            "value": [{"id": "ws-2", "displayName": "Target Workspace"}],
        }
        page2_response.raise_for_status = MagicMock()

        with patch("fabric_metadata_dags.fabric_client.requests.get") as mock_get:
            mock_get.side_effect = [page1_response, page2_response]
            result = _resolve_workspace_id("token", "Target Workspace")
        assert result == "ws-2"
        assert mock_get.call_count == 2


# ---------------------------------------------------------------------------
# Notebook listing
# ---------------------------------------------------------------------------


class TestListNotebooks:
    def test_returns_display_names(self):
        body = {
            "value": [
                {"displayName": "ingest_sales", "type": "Notebook"},
                {"displayName": "transform_sales", "type": "Notebook"},
            ]
        }
        mock_resp = MagicMock()
        mock_resp.json.return_value = body
        mock_resp.raise_for_status = MagicMock()

        with patch("fabric_metadata_dags.fabric_client.requests.get") as mock_get:
            mock_get.return_value = mock_resp
            result = _list_notebooks("token", "ws-1")
        assert result == ["ingest_sales", "transform_sales"]

    def test_follows_pagination(self):
        page1 = MagicMock()
        page1.json.return_value = {
            "value": [{"displayName": "nb_one"}],
            "continuationUri": "https://next-page",
        }
        page1.raise_for_status = MagicMock()

        page2 = MagicMock()
        page2.json.return_value = {"value": [{"displayName": "nb_two"}]}
        page2.raise_for_status = MagicMock()

        with patch("fabric_metadata_dags.fabric_client.requests.get") as mock_get:
            mock_get.side_effect = [page1, page2]
            result = _list_notebooks("token", "ws-1")
        assert result == ["nb_one", "nb_two"]
        assert mock_get.call_count == 2

    def test_returns_empty_list_for_no_notebooks(self):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"value": []}
        mock_resp.raise_for_status = MagicMock()
        with patch("fabric_metadata_dags.fabric_client.requests.get") as mock_get:
            mock_get.return_value = mock_resp
            result = _list_notebooks("token", "ws-1")
        assert result == []


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------


class TestCache:
    def test_write_and_read_cache(self, tmp_path):
        with patch.object(
            fabric_client, "_cache_path", return_value=tmp_path / "ws.json"
        ):
            _write_cache("ws-1", ["nb_a", "nb_b"])
            result = _read_cache("ws-1")
        assert result == ["nb_a", "nb_b"]

    def test_stale_cache_returns_none(self, tmp_path):
        cache_file = tmp_path / "ws.json"
        old_time = (datetime.now(tz=timezone.utc) - timedelta(seconds=700)).isoformat()
        cache_file.write_text(
            json.dumps({"fetched_at": old_time, "notebooks": ["nb_a"]}),
            encoding="utf-8",
        )
        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            result = _read_cache("ws-1")
        assert result is None

    def test_missing_cache_returns_none(self, tmp_path):
        with patch.object(
            fabric_client, "_cache_path", return_value=tmp_path / "missing.json"
        ):
            result = _read_cache("ws-1")
        assert result is None

    def test_corrupt_cache_returns_none(self, tmp_path):
        cache_file = tmp_path / "ws.json"
        cache_file.write_text("not json", encoding="utf-8")
        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            result = _read_cache("ws-1")
        assert result is None

    def test_fresh_cache_is_returned_without_api_call(self, tmp_path):
        cache_file = tmp_path / "ws.json"
        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            _write_cache("ws-1", ["nb_cached"])

        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            with patch(
                "fabric_metadata_dags.fabric_client._list_notebooks"
            ) as mock_list:
                with patch(
                    "fabric_metadata_dags.fabric_client._get_access_token",
                    return_value="tok",
                ):
                    with patch(
                        "fabric_metadata_dags.fabric_client._resolve_workspace_id",
                        return_value="ws-1",
                    ):
                        result = get_workspace_notebooks("My Workspace")
        mock_list.assert_not_called()
        assert result == {"nb_cached"}


# ---------------------------------------------------------------------------
# get_workspace_notebooks (integration of all steps)
# ---------------------------------------------------------------------------


class TestGetWorkspaceNotebooks:
    def test_returns_set_of_notebook_names(self, tmp_path):
        cache_file = tmp_path / "ws-1.json"
        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            with patch(
                "fabric_metadata_dags.fabric_client._get_access_token",
                return_value="tok",
            ):
                with patch(
                    "fabric_metadata_dags.fabric_client._resolve_workspace_id",
                    return_value="ws-1",
                ):
                    with patch(
                        "fabric_metadata_dags.fabric_client._list_notebooks",
                        return_value=["nb_a", "nb_b"],
                    ):
                        result = get_workspace_notebooks("My Workspace")
        assert result == {"nb_a", "nb_b"}

    def test_writes_cache_after_fresh_fetch(self, tmp_path):
        cache_file = tmp_path / "ws-1.json"
        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            with patch(
                "fabric_metadata_dags.fabric_client._get_access_token",
                return_value="tok",
            ):
                with patch(
                    "fabric_metadata_dags.fabric_client._resolve_workspace_id",
                    return_value="ws-1",
                ):
                    with patch(
                        "fabric_metadata_dags.fabric_client._list_notebooks",
                        return_value=["nb_a"],
                    ):
                        get_workspace_notebooks("My Workspace")
        assert cache_file.exists()
        data = json.loads(cache_file.read_text())
        assert data["notebooks"] == ["nb_a"]

    def test_refresh_cache_bypasses_fresh_cache(self, tmp_path):
        cache_file = tmp_path / "ws-1.json"
        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            _write_cache("ws-1", ["stale_nb"])

        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            with patch(
                "fabric_metadata_dags.fabric_client._get_access_token",
                return_value="tok",
            ):
                with patch(
                    "fabric_metadata_dags.fabric_client._resolve_workspace_id",
                    return_value="ws-1",
                ):
                    with patch(
                        "fabric_metadata_dags.fabric_client._list_notebooks",
                        return_value=["fresh_nb"],
                    ) as mock_list:
                        result = get_workspace_notebooks(
                            "My Workspace", refresh_cache=True
                        )
        mock_list.assert_called_once()
        assert result == {"fresh_nb"}

    def test_refresh_cache_overwrites_cached_data(self, tmp_path):
        cache_file = tmp_path / "ws-1.json"
        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            _write_cache("ws-1", ["old_nb"])

        with patch.object(fabric_client, "_cache_path", return_value=cache_file):
            with patch(
                "fabric_metadata_dags.fabric_client._get_access_token",
                return_value="tok",
            ):
                with patch(
                    "fabric_metadata_dags.fabric_client._resolve_workspace_id",
                    return_value="ws-1",
                ):
                    with patch(
                        "fabric_metadata_dags.fabric_client._list_notebooks",
                        return_value=["new_nb"],
                    ):
                        get_workspace_notebooks("My Workspace", refresh_cache=True)
        data = json.loads(cache_file.read_text())
        assert data["notebooks"] == ["new_nb"]
