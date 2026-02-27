"""Tableau Cloud REST API client."""

import os
import logging
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)


def fetch(config: dict) -> list[dict]:
    """Fetch workbooks and views from Tableau Cloud."""
    server_url = config.get("TABLEAU_SERVER_URL", "").rstrip("/")
    site_id = config.get("TABLEAU_SITE_ID", "")
    token_name = config.get("TABLEAU_TOKEN_NAME", "")
    token_value = config.get("TABLEAU_TOKEN_VALUE", "")

    if not all([server_url, site_id, token_name, token_value]):
        logger.warning("Tableau credentials not configured, skipping")
        return []

    try:
        token, site_luid = _signin(server_url, site_id, token_name, token_value)
    except Exception as e:
        logger.error("Tableau auth failed: %s", e)
        return []

    headers = {"x-tableau-auth": token, "Accept": "application/json"}
    base = f"{server_url}/api/3.21/sites/{site_luid}"

    assets = []

    try:
        workbooks = _paginate(f"{base}/workbooks", headers)
        for wb in workbooks:
            assets.append({
                "tool": "tableau",
                "name": wb.get("name", ""),
                "description": wb.get("description") or None,
                "owner": wb.get("owner", {}).get("name") or None,
                "updated_at": _parse_dt(wb.get("updatedAt")),
                "url": wb.get("webpageUrl", ""),
                "status": "unknown",
            })
    except Exception as e:
        logger.error("Tableau workbooks fetch failed: %s", e)

    try:
        _signout(server_url, token)
    except Exception:
        pass

    return assets


def _signin(server_url: str, site_id: str, token_name: str, token_value: str) -> tuple[str, str]:
    url = f"{server_url}/api/3.21/auth/signin"
    payload = {
        "credentials": {
            "personalAccessTokenName": token_name,
            "personalAccessTokenSecret": token_value,
            "site": {"contentUrl": site_id},
        }
    }
    resp = requests.post(url, json=payload, headers={"Accept": "application/json"}, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    token = data["credentials"]["token"]
    site_luid = data["credentials"]["site"]["id"]  # UUID, required for subsequent API calls
    return token, site_luid


def _signout(server_url: str, token: str) -> None:
    url = f"{server_url}/api/3.21/auth/signout"
    requests.post(url, headers={"x-tableau-auth": token}, timeout=10)


def _paginate(url: str, headers: dict) -> list[dict]:
    """Fetch all pages of results from a Tableau REST API list endpoint."""
    results = []
    page_size = 100
    page_num = 1

    while True:
        resp = requests.get(
            url,
            headers=headers,
            params={"pageSize": page_size, "pageNumber": page_num},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        # Determine the key that holds the list items
        # Tableau wraps results in a key matching the resource name
        pagination = data.get("pagination", {})
        total = int(pagination.get("totalAvailable", 0))

        # Find the list in the response (first key that is a list and not pagination)
        items = []
        for key, val in data.items():
            if key != "pagination" and isinstance(val, dict):
                for subkey, subval in val.items():
                    if isinstance(subval, list):
                        items = subval
                        break
                if items:
                    break

        results.extend(items)

        if len(results) >= total or not items:
            break
        page_num += 1

    return results


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
