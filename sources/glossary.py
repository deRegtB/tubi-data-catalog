"""Fetch glossary terms and metrics from adRise/data_science SQL files."""

import base64
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

logger = logging.getLogger(__name__)

REPO = "adRise/data_science"
DIRS = {
    "glossary/dimensions": "Dimension",
    "glossary/metrics": "Metric",
}


def fetch(config) -> list[dict]:
    token = config.get("GLOSSARY_GITHUB_TOKEN", "")
    if not token:
        logger.warning("GLOSSARY_GITHUB_TOKEN not set, skipping glossary")
        return []
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
    }
    terms = []
    for path, category in DIRS.items():
        try:
            entries = [f for f in _list_dir(headers, path) if f.get("type") == "file"]
            logger.info("Glossary: %d files in %s", len(entries), path)

            def fetch_one(f, cat=category):
                raw = _fetch_file_by_path(headers, f["path"])
                source_url = f"https://github.com/{REPO}/blob/main/{f['path']}"
                return _parse_sql_file(f["name"], raw, cat, source_url)

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = {executor.submit(fetch_one, f): f for f in entries}
                for future in as_completed(futures):
                    try:
                        term = future.result()
                        if term:
                            terms.append(term)
                    except Exception as e:
                        logger.debug("Glossary: skipping %s: %s", futures[future]["name"], e)
        except Exception as e:
            logger.error("Glossary: failed to list %s: %s", path, e)
    logger.info("Glossary: loaded %d terms total", len(terms))
    return terms


def _list_dir(headers: dict, path: str) -> list[dict]:
    url = f"https://api.github.com/repos/{REPO}/contents/{path}"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _fetch_file_by_path(headers: dict, path: str) -> str:
    """Fetch a single file's content from the GitHub Contents API."""
    url = f"https://api.github.com/repos/{REPO}/contents/{path}"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    raw_bytes = base64.b64decode(resp.json()["content"].replace("\n", ""))
    try:
        return raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        return raw_bytes.decode("latin-1")


def _parse_sql_file(filename: str, content: str, category: str, source_url: str = "") -> dict | None:
    # Term name: strip .sql extension, replace underscores with spaces
    name = filename
    if name.lower().endswith(".sql"):
        name = name[:-4]
    term = name.replace("_", " ").strip()
    if not term:
        return None

    # Split leading comment lines from SQL body
    lines = content.splitlines()
    comment_lines = []
    sql_lines = []
    in_comments = True
    for line in lines:
        stripped = line.strip()
        if in_comments and stripped.startswith("--"):
            # Strip the comment prefix and any leading/trailing whitespace
            comment_lines.append(stripped.lstrip("-").strip())
        else:
            in_comments = False
            sql_lines.append(line)

    definition = " ".join(l for l in comment_lines if l).strip()
    # Clean up common prefixes like "Query Purpose:", "Data Source:" from definition
    for prefix in ("Query Purpose:", "query purpose:"):
        if definition.lower().startswith(prefix.lower()):
            definition = definition[len(prefix):].strip()

    sql = "\n".join(sql_lines).strip()

    return {
        "term": term,
        "definition": definition,
        "category": category,
        "type": "metric" if category == "Metric" else "glossary",
        "tags": [category],
        "formula": sql,
        "source_url": source_url,
        "related_term_keys": [],
        "dashboards": [],
    }
