"""Generate the Tubi Data Catalog static HTML page."""

import os
import sys
import logging
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

from sources import tableau, preset, databricks, glossary

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
logger = logging.getLogger(__name__)

STALE_DAYS = 30

_BAD_NAME_PREFIXES = ("untitled", "copy of ", "[test]", "[draft]")
_BAD_NAME_EXACT = {"test", "draft", "temp", "untitled", "untitled dashboard"}
_BAD_NAME_SUBSTRINGS = ("\btmp\b",)


def compute_quality(asset: dict) -> bool:
    name = asset.get("name", "").strip()
    if not name:
        return False
    n = name.lower()
    if n in _BAD_NAME_EXACT:
        return False
    if any(n.startswith(p) for p in _BAD_NAME_PREFIXES):
        return False
    if n.startswith("test ") or n.endswith(" test"):
        return False
    if "tmp" == n or n.startswith("tmp ") or n.endswith(" tmp"):
        return False
    if asset.get("tool") == "preset" and not asset.get("published", True):
        return False
    return True


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
        "GLOSSARY_GITHUB_TOKEN",
    ]
    return {k: os.environ.get(k, "") for k in keys}


def load_metadata() -> dict:
    path = Path(__file__).parent / "metadata.yml"
    if not path.exists():
        return {"featured": [], "name_to_domains": {}, "name_to_tags": {}}
    with path.open() as f:
        raw = yaml.safe_load(f) or {}
    featured = [n.lower() for n in raw.get("featured", [])]
    name_to_domains = {}
    for domain, names in (raw.get("domains") or {}).items():
        for name in (names or []):
            name_to_domains.setdefault(name.lower(), []).append(domain)
    name_to_tags = {}
    for tag, names in (raw.get("tags") or {}).items():
        for name in (names or []):
            name_to_tags.setdefault(name.lower(), []).append(tag)
    return {"featured": featured, "name_to_domains": name_to_domains, "name_to_tags": name_to_tags}


def enrich_assets(assets: list[dict], metadata: dict) -> None:
    featured_names = metadata["featured"]
    name_to_domains = metadata["name_to_domains"]
    name_to_tags = metadata["name_to_tags"]
    for asset in assets:
        name_lower = asset["name"].lower()
        asset["featured"] = any(f in name_lower for f in featured_names)
        asset["domains"] = name_to_domains.get(name_lower, [])
        # Auto-populate domain from Tableau project if not manually assigned
        if not asset["domains"] and asset.get("project"):
            asset["domains"] = [asset["project"]]
        asset["tags"] = name_to_tags.get(name_lower, [])
        asset["related_terms"] = []
        asset["quality"] = compute_quality(asset)


def link_glossary(terms: list[dict], assets: list[dict]) -> None:
    for term in terms:
        term_lower = term["term"].lower()
        for asset in assets:
            in_name = term_lower in asset["name"].lower()
            in_description = term_lower in (asset.get("description") or "").lower()
            if in_name or in_description:
                term["dashboards"].append(asset["name"])
                asset["related_terms"].append(term["term"])


def main() -> None:
    config = load_config()

    fetchers = {
        "tableau": lambda: tableau.fetch(config),
        "preset": lambda: preset.fetch(config),
        "databricks": lambda: databricks.fetch(config),
        "glossary": lambda: glossary.fetch(config),
    }

    all_assets: list[dict] = []
    glossary_terms: list[dict] = []
    errors: list[str] = []

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {executor.submit(fn): name for name, fn in fetchers.items()}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results = future.result()
                logger.info("%s: fetched %d items", name, len(results))
                if name == "glossary":
                    glossary_terms.extend(results)
                else:
                    all_assets.extend(results)
            except Exception as e:
                logger.error("%s: fetch raised exception: %s", name, e)
                errors.append(f"{name}: {e}")

    # Compute freshness status for every asset
    for asset in all_assets:
        asset["status"] = compute_status(asset)

    # Load metadata and enrich assets
    metadata = load_metadata()
    enrich_assets(all_assets, metadata)

    # Link glossary terms to assets
    link_glossary(glossary_terms, all_assets)

    # Collect all domains across assets
    all_domains = sorted({d for asset in all_assets for d in asset["domains"]})

    # Sort: active first, then stale, then unknown; within each group featured first, then by name
    status_order = {"active": 0, "stale": 1, "unknown": 2}
    all_assets.sort(key=lambda a: (
        status_order.get(a["status"], 2),
        0 if a["featured"] else 1,
        a["name"].lower(),
    ))

    generated_at = datetime.now(timezone.utc)

    env = Environment(loader=FileSystemLoader(str(Path(__file__).parent)), autoescape=True)
    template = env.get_template("template.html")
    html = template.render(
        assets=all_assets,
        glossary_terms=glossary_terms,
        all_domains=all_domains,
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
    print(f"Generated {len(all_assets)} assets, {len(glossary_terms)} glossary terms → {output_path}")
    if errors:
        print(f"Errors: {', '.join(errors)}", file=sys.stderr)


if __name__ == "__main__":
    main()
