"""Generate the Tubi Data Catalog static HTML page."""

import os
import sys
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from sources import tableau, preset, databricks

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

STALE_DAYS = 30


def compute_status(asset: dict) -> str:
    updated_at = asset.get("updated_at")
    if updated_at is None:
        return "unknown"
    now = datetime.now(timezone.utc)
    if (now - updated_at) <= timedelta(days=STALE_DAYS):
        return "active"
    return "stale"


def load_config() -> dict:
    keys = [
        "TABLEAU_SERVER_URL", "TABLEAU_SITE_ID", "TABLEAU_TOKEN_NAME", "TABLEAU_TOKEN_VALUE",
        "PRESET_API_KEY", "PRESET_API_SECRET", "PRESET_WORKSPACE_URL",
        "DATABRICKS_HOST", "DATABRICKS_TOKEN",
    ]
    return {k: os.environ.get(k, "") for k in keys}


def main() -> None:
    config = load_config()

    fetchers = {
        "tableau": lambda: tableau.fetch(config),
        "preset": lambda: preset.fetch(config),
        "databricks": lambda: databricks.fetch(config),
    }

    all_assets: list[dict] = []
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(fn): name for name, fn in fetchers.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                assets = future.result()
                logger.info("%s: fetched %d assets", name, len(assets))
                all_assets.extend(assets)
            except Exception as e:
                logger.error("%s: fetch raised exception: %s", name, e)
                errors.append(f"{name}: {e}")

    # Compute freshness status for every asset
    for asset in all_assets:
        asset["status"] = compute_status(asset)

    # Sort: active first, then stale, then unknown; within each group sort by name
    status_order = {"active": 0, "stale": 1, "unknown": 2}
    all_assets.sort(key=lambda a: (status_order.get(a["status"], 2), a["name"].lower()))

    generated_at = datetime.now(timezone.utc)

    env = Environment(loader=FileSystemLoader(str(Path(__file__).parent)), autoescape=True)
    template = env.get_template("template.html")
    html = template.render(
        assets=all_assets,
        generated_at=generated_at,
        errors=errors,
        counts={
            "total": len(all_assets),
            "tableau": sum(1 for a in all_assets if a["tool"] == "tableau"),
            "preset": sum(1 for a in all_assets if a["tool"] == "preset"),
            "databricks": sum(1 for a in all_assets if a["tool"] == "databricks"),
        },
    )

    output_path = Path(__file__).parent / "catalog.html"
    output_path.write_text(html, encoding="utf-8")
    print(f"Generated {len(all_assets)} assets → {output_path}")
    if errors:
        print(f"Errors: {', '.join(errors)}", file=sys.stderr)


if __name__ == "__main__":
    main()
