"""Generate the Tubi Data Catalog static HTML page."""

import json
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

CANONICAL_DOMAINS = [
    "General Tubi",
    "Core Experiences",
    "Viewer Growth",
    "Finance & BizOps",
    "Experimentation",
    "Ads",
    "DiscoAI",
    "Infra/Tools",
    "Content",
]

# Maps every known Tableau project name → canonical domain.
# None means personal/sandbox — fall through to keyword matching.
_PROJECT_TO_DOMAIN = {
    "2. DEVELOPMENT":                                    "Infra/Tools",
    "Accounting":                                        "Finance & BizOps",
    "Admin Insights":                                    "General Tubi",
    "Ads_Account Management Tools":                      "Ads",
    "Ads_Ad Deactiviation Dashboard (S&P)":              "Ads",
    "Ads_Ad Monetization Dashboards":                    "Ads",
    "Ads_AdOps Tools":                                   "Ads",
    "Certified KPIs":                                    "General Tubi",
    "Complex Dashboard Templates":                       "Infra/Tools",
    "Content Partnerships":                              "Content",
    "Core Dashboards":                                   "General Tubi",
    "Core Experiences":                                  "Core Experiences",
    "Dashboard Templates & Style Guide":                 "Infra/Tools",
    "Dev_Ads":                                           "Ads",
    "Dev_Ads_Account Management Tools":                  "Ads",
    "Dev_Ads_Ad Deactiviation Dashboard (S&P)":          "Ads",
    "Dev_Ads_Ad Monetization Dashboards":                "Ads",
    "Dev_Ads_AdOps Tools":                               "Ads",
    "Dev_Ads_Product Tools":                             "Ads",
    "Dev_Ads_Sales Ops Tools":                           "Ads",
    "Dev_Ads_Yield":                                     "Ads",
    "Dev_BizOps":                                        "Finance & BizOps",
    "Dev_Content":                                       "Content",
    "Dev_Content_Analytics":                             "Content",
    "Dev_DSA":                                           "General Tubi",
    "Dev_DSA Viewer Growth":                             "Viewer Growth",
    "Dev_DSA_Core_Experience_Retention":                 "Core Experiences",
    "Dev_Ecosystems_Experiments and Holdouts":           "Experimentation",
    "Dev_Ecosystems_Finance":                            "Finance & BizOps",
    "Dev_Finance & BizOps":                              "Finance & BizOps",
    "Dev_Growth Analytics_Adhoc_Analysis":               "Viewer Growth",
    "Dev_Growth Analytics_Core Dashboards":              "Viewer Growth",
    "Dev_Growth Analytics_Retargeting":                  "Viewer Growth",
    "Dev_Tubi KPIs (Certified)_Distribution Dashboard":  "General Tubi",
    "Ecosystems_Finance":                                "Finance & BizOps",
    "Experimentation":                                   "Experimentation",
    "FP&A":                                              "Finance & BizOps",
    "Month End Close":                                   "Finance & BizOps",
    "PRO Reporting":                                     "Ads",
    "Revenue Accounting":                                "Finance & BizOps",
    "Rightsline":                                        "Content",
    "Samples":                                           "Infra/Tools",
    "Sandbox":                                           "Infra/Tools",
    "Simple Dashboard Templates":                        "Infra/Tools",
    "Test Workbooks and Data Sources":                   "Infra/Tools",
    "default":                                           "General Tubi",
    # Personal folders — no project domain, try keyword fallback
    "Xueling Chen":                                      None,
    "Yuting Chen":                                       None,
}

# Keyword rules for Preset/Databricks (and Tableau with unrecognised projects).
# All matching rules contribute — a dashboard can get multiple domains.
_KEYWORD_DOMAIN_RULES = [
    (["experiment", "a/b", "holdout", "treatment vs", "ab test", "control group"],
     "Experimentation"),
    (["adops", "ad ops", "ad monetiz", "ad account", "ad deactiv", "ad product",
      "yield", "programmatic", "avod", "ecpm", "fill rate", "impression",
      "advertis", "sales ops", "cpm"],
     "Ads"),
    (["discoai", "disco ai", "recommendation", "personali", "content ranking",
      "ml model", "algorithmic"],
     "DiscoAI"),
    (["fp&a", "month end", "revenue account", "content licensing",
      "rights management", "rightsline", "finance", "accounting",
      "bizops", "biz ops", "budget forecast"],
     "Finance & BizOps"),
    (["content catalog", "content partner", "title catalog", "show catalog",
      "content rights", "content licensing"],
     "Content"),
    (["viewer growth", "user growth", "user acquisition", "user retention",
      "churn", "new user", "signup", "registr",
      "dau", "mau", "wau", "daily active", "monthly active", "weekly active"],
     "Viewer Growth"),
    (["core experience", "playback", "video start", "buffering",
      "video player", "watch session"],
     "Core Experiences"),
    (["data quality", "data pipeline", "data infra", "data engineering",
      "dbt", "airflow", "etl", "infra monitor", "tooling", "admin tool"],
     "Infra/Tools"),
    (["certified kpi", "north star", "executive", "company kpi",
      "tubi kpi", "tubi overview", "company overview"],
     "General Tubi"),
]


def _infer_domains(asset: dict) -> list[str]:
    """Map an asset to one or more canonical domains."""
    # 1. Tableau project mapping (authoritative when known)
    project = asset.get("project")
    if project and project in _PROJECT_TO_DOMAIN:
        mapped = _PROJECT_TO_DOMAIN[project]
        if mapped:
            return [mapped]
        # None → personal folder; fall through to keyword matching

    # 2. Keyword matching on name
    name_lower = asset.get("name", "").lower()
    domains: list[str] = []
    for keywords, domain in _KEYWORD_DOMAIN_RULES:
        if any(kw in name_lower for kw in keywords):
            if domain not in domains:
                domains.append(domain)

    # 3. General Tubi catch-all so every dashboard has at least one domain
    if not domains:
        domains = ["General Tubi"]

    return domains


# ── Owner / pod mapping ───────────────────────────────────────────────────────

DSA_PODS = [
    "Viewer Growth",
    "Core Experiences",
    "Ads",
    "Content",
    "DiscoAI",
    "Experimentation",
    "Finance & BizOps",
    "Infra/Tools",
]

_POD_SLUG = {
    "Viewer Growth":    "viewer-growth",
    "Core Experiences": "core-experiences",
    "Ads":              "ads",
    "Content":          "content",
    "DiscoAI":          "discoai",
    "Experimentation":  "experimentation",
    "Finance & BizOps": "finance-bizops",
    "Infra/Tools":      "infra-tools",
}

# Canonical display name for every known owner identifier (email or "First Last")
_OWNER_DISPLAY = {
    # Tableau owners (email → display name)
    "agutierrez@tubi.tv":                               "A. Gutierrez",
    "akshatshah@tubi.tv":                               "Akshat Shah",
    "aray@tubi.tv":                                     "Ashley Ray",
    "bderegt@tubi.tv":                                  "Bryan deRegt",
    "bmccue@tubi.tv":                                   "Bryan McCue",
    "elau@tubi.tv":                                     "Elvis Lau",
    "esthertsui@tubi.tv":                               "Esther Tsui",
    "gbroe@tubi.tv":                                    "Greg Broe",
    "jfan@tubi.tv":                                     "J. Fan",
    "jvora@tubi.tv":                                    "Jaynil Vora",
    "knguyen@tubi.tv":                                  "K. Nguyen",
    "stephenkim@tubi.tv":                               "Stephen Kim",
    "tdecampo@tubi.tv":                                 "Tim DeCampo",
    "tol.admin.api.broker.service.usera@tableau.com":   "Tableau Service",
    "vhernandez@tubi.tv":                               "V. Hernandez",
    "wgao@tubi.tv":                                     "Wenting Gao",
    "xchen@tubi.tv":                                    "Xueling Chen",
    "ychen@tubi.tv":                                    "Yixin Chen",
    # DSA members not yet seen in Tableau (for future dashboards / Preset matching)
    "ajain@tubi.tv":        "Aashi Jain",
    "amirina@tubi.tv":      "Alex Mirina",
    "bpulikkal@tubi.tv":    "Binoop Pulikkal",
    "cbejjani@tubi.tv":     "Christina Bejjani",
    "cdileo@tubi.tv":       "Christopher DiLeo",
    "gcavdar@tubi.tv":      "Gozde Cavdar",
    "gnguyen@tubi.tv":      "GiGi Nguyen",
    "hmahoney@tubi.tv":     "Hillary Mahoney",
    "jdeng@tubi.tv":        "Jieyi Deng",
    "jsingh@tubi.tv":       "Jaskaran Singh",
    "lmantena@tubi.tv":     "Lakshmi Mantena",
    "lwiebolt@tubi.tv":     "Luke Wiebolt",
    "mdorofiyenko@tubi.tv": "Maxim Dorofiyenko",
    "myoung@tubi.tv":       "Mariel Young",
    "rvaswani@tubi.tv":     "Rahul Vaswani",
    "slu@tubi.tv":          "Sang Lu",
    "syin@tubi.tv":         "Sunny Yin",
    "vlu@tubi.tv":          "Vivian Lu",
    "ywang@tubi.tv":        "Yixin Wang",
    "yliu@tubi.tv":         "Yonglin Liu",
}

# DSA pod membership: pod name → list of owner identifiers (email or "First Last")
_DSA_POD_MEMBERS: dict[str, list[str]] = {
    "Viewer Growth": [
        "bderegt@tubi.tv", "gnguyen@tubi.tv", "gbroe@tubi.tv",
        "hmahoney@tubi.tv", "slu@tubi.tv",
        # Preset name variants
        "Greg Broe", "GiGi Nguyen", "Hillary Mahoney", "Sang Lu",
    ],
    "Infra/Tools": [
        "bderegt@tubi.tv", "lwiebolt@tubi.tv",
        "Luke Wiebolt",
    ],
    "Finance & BizOps": [
        "stephenkim@tubi.tv", "aray@tubi.tv", "bderegt@tubi.tv",
        "Stephen Kim", "Ashley Ray",
    ],
    "Experimentation": [
        "wgao@tubi.tv", "aray@tubi.tv",
        "Wenting Gao", "Ashley Ray",
    ],
    "Content": [
        "rvaswani@tubi.tv", "aray@tubi.tv", "cbejjani@tubi.tv",
        "elau@tubi.tv", "lmantena@tubi.tv", "tdecampo@tubi.tv",
        "Rahul Vaswani", "Ashley Ray", "Christina Bejjani",
        "Elvis Lau", "Lakshmi Mantena", "Tim DeCampo",
    ],
    "DiscoAI": [
        "ajain@tubi.tv", "amirina@tubi.tv", "jsingh@tubi.tv", "ychen@tubi.tv",
        "Aashi Jain", "Alex Mirina", "Jaskaran Singh", "Yixin Chen",
    ],
    "Core Experiences": [
        "cdileo@tubi.tv", "gcavdar@tubi.tv", "jdeng@tubi.tv",
        "myoung@tubi.tv", "syin@tubi.tv", "yliu@tubi.tv",
        "Christopher DiLeo", "Gozde Cavdar", "Jieyi Deng",
        "Mariel Young", "Sunny Yin", "Yonglin Liu",
    ],
    "Ads": [
        "akshatshah@tubi.tv", "bpulikkal@tubi.tv", "jvora@tubi.tv",
        "vlu@tubi.tv", "ywang@tubi.tv", "mdorofiyenko@tubi.tv",
        "Akshat Shah", "Binoop Pulikkal", "Jaynil Vora",
        "Vivian Lu", "Yixin Wang", "Maxim Dorofiyenko",
    ],
}

# Build reverse lookup: owner identifier → [pod_slug, ...]
_OWNER_TO_POD_SLUGS: dict[str, list[str]] = {}
for _pod, _members in _DSA_POD_MEMBERS.items():
    for _identifier in _members:
        _OWNER_TO_POD_SLUGS.setdefault(_identifier, []).append(_POD_SLUG[_pod])

# ── Quality filtering ─────────────────────────────────────────────────────────

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


def load_overrides() -> dict:
    """Load overrides.json — UI-driven per-dashboard overrides with author tracking."""
    path = Path(__file__).parent / "overrides.json"
    if not path.exists():
        return {}
    try:
        with path.open() as f:
            return json.load(f) or {}
    except Exception as e:
        logger.warning("overrides.json load failed: %s", e)
        return {}


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
        return {"featured": [], "name_to_domains": {}, "name_to_tags": {}, "name_to_pods": {}, "name_to_status": {}}
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
    name_to_pods = {}
    for pod_slug, names in (raw.get("teams") or {}).items():
        for name in (names or []):
            name_to_pods.setdefault(name.lower(), []).append(pod_slug)
    name_to_status = {}
    for status, names in (raw.get("status_override") or {}).items():
        for name in (names or []):
            name_to_status[name.lower()] = status
    return {
        "featured": featured,
        "name_to_domains": name_to_domains,
        "name_to_tags": name_to_tags,
        "name_to_pods": name_to_pods,
        "name_to_status": name_to_status,
    }


def enrich_assets(assets: list[dict], metadata: dict, overrides: dict | None = None) -> None:
    featured_names = metadata["featured"]
    name_to_domains = metadata["name_to_domains"]
    name_to_tags = metadata["name_to_tags"]
    name_to_pods = metadata["name_to_pods"]
    name_to_status = metadata["name_to_status"]
    overrides = overrides or {}
    for asset in assets:
        name_lower = asset["name"].lower()
        asset["featured"] = any(f in name_lower for f in featured_names)
        # Manual metadata.yml override takes precedence; otherwise infer
        asset["domains"] = name_to_domains.get(name_lower, []) or _infer_domains(asset)
        asset["tags"] = name_to_tags.get(name_lower, [])
        asset["related_terms"] = []
        asset["quality"] = compute_quality(asset)
        # Owner display name
        owner = asset.get("owner") or ""
        asset["owner_display"] = (
            _OWNER_DISPLAY.get(owner)           # email → display name (Tableau)
            or (owner if "@" not in owner else None)  # "First Last" string (Preset)
            or (owner.split("@")[0] if owner else None)  # email prefix fallback
        )
        # Pod assignment: metadata.yml override takes precedence, else owner-based
        if name_lower in name_to_pods:
            asset["pod_slugs"] = name_to_pods[name_lower]
        else:
            asset["pod_slugs"] = _OWNER_TO_POD_SLUGS.get(owner, ["non-dsa"] if owner else [])
        # Status / visibility override from metadata.yml
        status_override = name_to_status.get(name_lower)
        if status_override == "hidden":
            asset["quality"] = False
        elif status_override in ("active", "stale", "unknown"):
            asset["status"] = status_override

        # overrides.json — highest precedence, includes committer info
        # Key is URL (unique per dashboard); fall back to name for backward compat
        ov = overrides.get(asset.get("url", "")) or overrides.get(asset["name"]) or overrides.get(name_lower)
        asset["override_meta"] = None
        if ov:
            if ov.get("domains"):
                asset["domains"] = ov["domains"]
            if ov.get("pods"):
                asset["pod_slugs"] = ov["pods"]
            if ov.get("featured") is not None:
                asset["featured"] = bool(ov["featured"])
            if ov.get("status") == "hidden":
                asset["quality"] = False
            elif ov.get("status") in ("active", "stale", "unknown"):
                asset["status"] = ov["status"]
            asset["override_meta"] = {
                "updated_by": ov.get("updated_by", ""),
                "updated_at": ov.get("updated_at", ""),
                "note": ov.get("note", ""),
            }


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

    # Load metadata and overrides, then enrich assets
    metadata = load_metadata()
    overrides = load_overrides()
    enrich_assets(all_assets, metadata, overrides)

    # Link glossary terms to assets
    link_glossary(glossary_terms, all_assets)

    # Use canonical domain list for the filter (fixed order, always all 9)
    all_domains = CANONICAL_DOMAINS

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
        canonical_domains=all_domains,
        dsa_pods=DSA_PODS,
        pod_slug=_POD_SLUG,
        generated_at=generated_at,
        errors=errors,
        overrides_json=json.dumps(overrides),
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
