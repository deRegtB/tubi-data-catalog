"""Fetch glossary and metrics terms from the adRise/dsa-context private GitHub repo."""

import base64
import logging

import requests

logger = logging.getLogger(__name__)

REPO = "adRise/dsa-context"
FILES = ["context/glossary.yaml", "context/metrics.yaml"]


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
    for filepath in FILES:
        try:
            raw = _fetch_file(headers, filepath)
            terms.extend(_parse(raw, filepath))
        except Exception as e:
            logger.error("Glossary: failed to load %s: %s", filepath, e)
    return terms


def _fetch_file(headers, filepath) -> str:
    url = f"https://api.github.com/repos/{REPO}/contents/{filepath}"
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return base64.b64decode(resp.json()["content"].replace("\n", "")).decode("utf-8")


def _parse(raw_yaml, filepath) -> list[dict]:
    import yaml

    data = yaml.safe_load(raw_yaml)
    terms = []
    # glossary.yaml: top-level key is "glossary"
    for key, entry in (data.get("glossary") or {}).items():
        terms.append({
            "term": entry.get("term", key),
            "definition": entry.get("definition", ""),
            "category": "Glossary",
            "type": "glossary",
            "tags": [],
            "related_term_keys": entry.get("related_terms", []),
            "dashboards": [],
        })
    # metrics.yaml: top-level key is "metrics"
    for key, entry in (data.get("metrics") or {}).items():
        terms.append({
            "term": entry.get("name", key),
            "definition": entry.get("definition", ""),
            "category": entry.get("tags", ["Metric"])[0] if entry.get("tags") else "Metric",
            "type": "metric",
            "tags": entry.get("tags", []),
            "formula": entry.get("formula", ""),
            "related_term_keys": [],
            "dashboards": [],
        })
    return terms
