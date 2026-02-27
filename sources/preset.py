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
        user_emails = _fetch_user_emails(workspace_url, headers)
        dashboards = _paginate(f"{workspace_url}/api/v1/dashboard/", headers)
        for d in dashboards:
            owners = d.get("owners", [])
            if owners:
                o = owners[0]
                # Prefer email (from user lookup); fall back to "First Last" string
                owner_name = user_emails.get(o.get("id"))
                if not owner_name:
                    first = o.get("first_name", "")
                    last = o.get("last_name", "")
                    owner_name = f"{first} {last}".strip() if (first or last) else None
            else:
                owner_name = None

            # Preset stores URLs as relative paths
            relative_url = d.get("url", "")
            full_url = f"{workspace_url}{relative_url}" if relative_url.startswith("/") else relative_url

            assets.append({
                "tool": "preset",
                "name": d.get("dashboard_title", ""),
                "description": d.get("description") or None,
                "owner": owner_name,
                "updated_at": _parse_dt(d.get("changed_on_utc") or d.get("changed_on")),
                "url": full_url,
                "published": d.get("published", True),
                "status": "unknown",
            })
    except Exception as e:
        logger.error("Preset dashboards fetch failed: %s", e)

    return assets


def _fetch_user_emails(workspace_url: str, headers: dict) -> dict:
    """Return {user_id: email} for all Preset workspace users.

    Tries the FAB /api/v1/security/users/ endpoint (may be restricted in Preset).
    Falls back gracefully — callers use first/last name string if this returns {}.
    """
    result = {}
    for path in ("/api/v1/security/users/", "/api/v1/security/users"):
        try:
            users = _paginate(f"{workspace_url}{path}", headers)
            for u in users:
                uid = u.get("id")
                email = u.get("username") or u.get("email")
                if uid and email and "@" in email:
                    result[uid] = email
            logger.info("Preset: resolved %d user emails via %s", len(result), path)
            return result
        except Exception as e:
            logger.debug("Preset user lookup via %s failed: %s", path, e)
    logger.warning("Preset: user email lookup unavailable; owners will display as 'First Last'")
    return result


def _login(api_key: str, api_secret: str) -> str:
    # Preset API keys authenticate via the Preset Manager API, not the workspace login endpoint
    url = "https://api.app.preset.io/v1/auth/"
    resp = requests.post(url, json={"name": api_key, "secret": api_secret}, timeout=30)
    resp.raise_for_status()
    payload = resp.json().get("payload", {})
    token = payload.get("access_token") or payload.get("token")
    if not token:
        raise ValueError(f"No token in Preset auth response. Keys: {list(payload.keys())}")
    return token


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
