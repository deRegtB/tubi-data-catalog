"""Databricks Lakeview (AI/BI) Dashboard API client."""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import requests

logger = logging.getLogger(__name__)


def fetch(config: dict) -> list[dict]:
    """Fetch active Lakeview dashboards from Databricks."""
    host = config.get("DATABRICKS_HOST", "").rstrip("/")
    token = config.get("DATABRICKS_TOKEN", "")

    if not all([host, token]):
        logger.warning("Databricks credentials not configured, skipping")
        return []

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    assets = []

    try:
        dashboards = _list_dashboards(host, headers)
        # Fetch individual details in parallel to get path/owner
        details = _fetch_details(host, headers, dashboards)
        for d in details:
            if d.get("lifecycle_state") == "TRASHED":
                continue

            dashboard_id = d.get("dashboard_id", "")
            url = f"{host}/sql/dashboardsv3/{dashboard_id}" if dashboard_id else ""

            updated_at = _parse_dt(d.get("update_time")) or _parse_dt(d.get("create_time"))

            # Extract email from workspace path: /Users/email@tubi.tv/DashboardName
            path = d.get("path", "")
            owner_match = re.search(r"/Users/([^/]+@[^/]+)/", path) if path else None
            owner = owner_match.group(1) if owner_match else (d.get("owner") or None)

            assets.append({
                "tool": "databricks",
                "name": d.get("display_name", ""),
                "description": None,
                "owner": owner,
                "updated_at": updated_at,
                "url": url,
                "status": "unknown",
            })
    except Exception as e:
        logger.error("Databricks dashboards fetch failed: %s", e)

    return assets


def _fetch_details(host: str, headers: dict, dashboards: list[dict]) -> list[dict]:
    """Fetch individual dashboard details to obtain the workspace path (owner)."""
    def fetch_one(d: dict) -> dict:
        did = d.get("dashboard_id", "")
        if not did:
            return d
        try:
            url = f"{host}/api/2.0/lakeview/dashboards/{did}"
            resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 429:
                time.sleep(int(resp.headers.get("Retry-After", 5)))
                resp = requests.get(url, headers=headers, timeout=15)
            if resp.status_code == 200:
                detail = resp.json()
                if detail.get("path"):
                    merged = dict(d)
                    merged["path"] = detail["path"]
                    return merged
        except Exception:
            pass
        return d

    results = []
    with ThreadPoolExecutor(max_workers=15) as executor:
        futures = {executor.submit(fetch_one, d): d for d in dashboards}
        for future in as_completed(futures):
            results.append(future.result())
    return results


def _list_dashboards(host: str, headers: dict) -> list[dict]:
    """Paginate through all Lakeview dashboards."""
    results = []
    url = f"{host}/api/2.0/lakeview/dashboards"
    params: dict = {"page_size": 100}

    while True:
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code == 429:
            time.sleep(int(resp.headers.get("Retry-After", 10)))
            continue
        resp.raise_for_status()
        data = resp.json()

        items = data.get("dashboards", [])
        results.extend(items)

        next_token = data.get("next_page_token")
        if not next_token:
            break
        params["page_token"] = next_token

    return results


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return None
