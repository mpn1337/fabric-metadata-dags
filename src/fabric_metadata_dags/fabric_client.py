"""Fabric REST API client — authentication, workspace resolution, and notebook listing.

Handles the I/O concerns only:
  - Acquiring an access token via Azure CLI credentials
  - Resolving a workspace display name to its ID
  - Listing notebook display names in a workspace
  - Caching results to avoid repeated API calls

The cache is stored in the OS temp directory with a 10-minute TTL:
    {tempdir}/fabric_metadata_dags/{workspace_id}.json
"""

from __future__ import annotations

import json
import logging
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from azure.identity import AzureCliCredential

logger = logging.getLogger(__name__)

_FABRIC_API = "https://api.fabric.microsoft.com/v1"
_TOKEN_SCOPE = "https://api.fabric.microsoft.com/.default"
_CACHE_TTL_SECONDS = 600  # 10 minutes


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_workspace_notebooks(
    workspace_name: str, refresh_cache: bool = False
) -> set[str]:
    """Return the set of notebook display names in *workspace_name*.

    Results are cached per workspace for :data:`_CACHE_TTL_SECONDS` seconds in
    the OS temp directory.  A cache miss (or stale cache) triggers a fresh API
    call.

    Args:
        workspace_name: Display name of the Fabric workspace (case-sensitive).
        refresh_cache: When ``True``, bypass the cache and force a fresh API
            call, overwriting any existing cached data.

    Returns:
        Set of notebook display names, e.g. ``{"ingest_sales", "transform_sales"}``.

    Raises:
        RuntimeError: If the Azure CLI is not authenticated (``az login`` required).
        ValueError: If *workspace_name* does not exist in the tenant.
        requests.HTTPError: If any API call returns a non-2xx response.
    """
    token = _get_access_token()
    workspace_id = _resolve_workspace_id(token, workspace_name)

    if not refresh_cache:
        cached = _read_cache(workspace_id)
        if cached is not None:
            logger.debug(
                "Cache hit for workspace %s (%s)", workspace_name, workspace_id
            )
            return set(cached)

    logger.debug(
        "Fetching notebooks for workspace %s (refresh_cache=%s)",
        workspace_name,
        refresh_cache,
    )
    notebooks = _list_notebooks(token, workspace_id)
    _write_cache(workspace_id, notebooks)
    return set(notebooks)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _get_access_token() -> str:
    try:
        credential = AzureCliCredential()
        token = credential.get_token(_TOKEN_SCOPE)
        return token.token
    except Exception as exc:
        raise RuntimeError(
            "Failed to acquire Fabric API token via Azure CLI. "
            "Run 'az login' and try again."
        ) from exc


def _resolve_workspace_id(token: str, workspace_name: str) -> str:
    """Return the workspace ID for the given display name."""
    headers = _auth_headers(token)
    url = f"{_FABRIC_API}/workspaces"

    while url:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        body: dict[str, Any] = response.json()

        for ws in body.get("value", []):
            if ws.get("displayName") == workspace_name:
                return ws["id"]

        url = body.get("continuationUri")  # follow pagination

    raise ValueError(
        f'Workspace "{workspace_name}" not found. '
        "Check the name or your Fabric permissions."
    )


def _list_notebooks(token: str, workspace_id: str) -> list[str]:
    """Return all notebook display names in *workspace_id*, following pagination."""
    headers = _auth_headers(token)
    url = f"{_FABRIC_API}/workspaces/{workspace_id}/items?type=Notebook"
    names: list[str] = []

    while url:
        response = requests.get(url, headers=headers, timeout=30)
        response.raise_for_status()
        body: dict[str, Any] = response.json()

        names.extend(item["displayName"] for item in body.get("value", []))
        url = body.get("continuationUri")

    logger.debug("Found %d notebook(s) in workspace %s", len(names), workspace_id)
    return names


def _auth_headers(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Cache helpers
# ---------------------------------------------------------------------------


def _cache_path(workspace_id: str) -> Path:
    cache_dir = Path(tempfile.gettempdir()) / "fabric_metadata_dags"
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir / f"{workspace_id}.json"


def _read_cache(workspace_id: str) -> list[str] | None:
    """Return cached notebook names if the cache exists and is fresh, else None."""
    path = _cache_path(workspace_id)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        fetched_at = datetime.fromisoformat(data["fetched_at"])
        age = (datetime.now(tz=timezone.utc) - fetched_at).total_seconds()
        if age > _CACHE_TTL_SECONDS:
            logger.debug(
                "Cache expired (age %.0fs) for workspace %s", age, workspace_id
            )
            return None
        return data["notebooks"]
    except (KeyError, ValueError, OSError):
        return None


def _write_cache(workspace_id: str, notebooks: list[str]) -> None:
    path = _cache_path(workspace_id)
    data = {
        "fetched_at": datetime.now(tz=timezone.utc).isoformat(),
        "notebooks": notebooks,
    }
    try:
        path.write_text(json.dumps(data), encoding="utf-8")
    except OSError as exc:
        logger.warning("Could not write notebook cache: %s", exc)
