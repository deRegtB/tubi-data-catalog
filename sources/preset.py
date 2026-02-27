"""Preset (Superset) REST API client."""

import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)


def fetch(config: dict) -> list[dict]:
    """Fetch dashboards from Preset workspace."""
    api_key = config.get("PRESET_API_KEY", "")
    api_secret = config.get("PRESET_API_SECRET", "")
    workspace_url = config.get("PRESET_WORKSPACE_URL", "").rstrip("/")

    if not all([api_key, api_secret, workspace_url]):
        logger.warning("Preset credentials not configured, skipping")
        return []

    try:
        token = _login(api_key, api_secret)
    except Exception as e:
        logger.error("Preset auth failed: %s", e)
        return []

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    assets = []

    try:
        dashboards = _paginate(f"{workspace_url}/api/v1/dashboard/", headers)
        for d in dashboards:
            owners = d.get("owners", [])
            owner_name = owners[0].get("username") if owners else None

            # Preset stores URLs as relative paths
            relative_url = d.get("url", "")
            full_url = f"{workspace_url}{relative_url}" if relative_url.startswith("/") else relative_url

            assets.append({
                "tool": "preset",
                "name": d.get("dashboard_title", ""),
                "description": None,
                "owner": owner_name,
                "updated_at": _parse_dt(d.get("changed_on_utc") or d.get("changed_on")),
                "url": full_url,
                "status": "unknown",
            })
    except Exception as e:
        logger.error("Preset dashboards fetch failed: %s", e)

    return assets


def _login(api_key: str, api_secret: str) -> str:
    # Preset API keys authenticate via the Preset Manager API, not the workspace login endpoint
    url = "https://api.app.preset.io/v1/auth/"
    resp = requests.post(url, json={"name": api_key, "secret": api_secret}, timeout=30)
    resp.raise_for_status()
    return resp.json()["payload"]["token"]


def _paginate(url: str, headers: dict) -> list[dict]:
    """Fetch all pages from a Superset-style REST API list endpoint."""
    results = []
    page = 0
    page_size = 100

    while True:
        resp = requests.get(
            url,
            headers=headers,
            params={"q": f"(page:{page},page_size:{page_size})"},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        items = data.get("result", [])
        results.extend(items)

        total = data.get("count", 0)
        if len(results) >= total or not items:
            break
        page += 1

    return results


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
        try:
            dt = datetime.strptime(value, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
