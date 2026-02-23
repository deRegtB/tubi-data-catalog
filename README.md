# Tubi Data Catalog

A nightly-generated static HTML page listing all dashboards from **Tableau Cloud**, **Preset**, and **Databricks (Lakeview/AI-BI)**. Hosted on GitHub Pages — no login required, no infrastructure to manage.

Site URL: `https://tubitv.github.io/tubi-data-catalog`

## Features

- Aggregates dashboards from all three tools into one searchable page
- Freshness signals: **Active** (updated within 30 days), **Stale** (older), **Unknown** (no date)
- Real-time search and tool/stale filters — pure vanilla JS, no frameworks
- Rebuilt nightly via GitHub Actions using the built-in `GITHUB_TOKEN` (no extra deploy credentials)

## Project Structure

```
tubi-data-catalog/
├── generate.py             # Main script: fetch all sources → render HTML
├── sources/
│   ├── tableau.py          # Tableau Cloud REST API client
│   ├── preset.py           # Preset/Superset REST API client
│   └── databricks.py       # Databricks Lakeview Dashboard API client
├── template.html           # Jinja2 HTML template (Bootstrap 5 CDN)
├── .github/workflows/
│   └── refresh.yml         # Nightly cron → GitHub Pages deploy
└── requirements.txt
```

## Local Development

```bash
pip install -r requirements.txt

export TABLEAU_SERVER_URL=https://your-tableau-server
export TABLEAU_SITE_ID=your-site-id
export TABLEAU_TOKEN_NAME=your-token-name
export TABLEAU_TOKEN_VALUE=your-token-value

export PRESET_API_KEY=your-api-key
export PRESET_API_SECRET=your-api-secret
export PRESET_WORKSPACE_URL=https://your-workspace.preset.io

export DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
export DATABRICKS_TOKEN=dapi...

python generate.py
open catalog.html
```

Missing credentials are skipped gracefully — you can test with just one or two sources.

## GitHub Pages Setup (one-time)

### 1. Push the repo to GitHub

```bash
git remote add origin https://github.com/tubitv/tubi-data-catalog.git
git push -u origin main
```

### 2. Enable GitHub Pages

In the GitHub repo: **Settings → Pages → Source → GitHub Actions**

That's it. No S3, no AWS credentials needed for deployment.

### 3. Add API secrets

In the GitHub repo: **Settings → Secrets and variables → Actions → New repository secret**

| Secret | Description |
|--------|-------------|
| `TABLEAU_SERVER_URL` | e.g. `https://10ax.online.tableau.com` |
| `TABLEAU_SITE_ID` | Tableau site content URL |
| `TABLEAU_TOKEN_NAME` | Personal Access Token name |
| `TABLEAU_TOKEN_VALUE` | Personal Access Token secret |
| `PRESET_API_KEY` | Preset API key |
| `PRESET_API_SECRET` | Preset API secret |
| `PRESET_WORKSPACE_URL` | e.g. `https://abc123.us1a.app.preset.io` |
| `DATABRICKS_HOST` | e.g. `https://adb-123.azuredatabricks.net` |
| `DATABRICKS_TOKEN` | Databricks personal access token |

### 4. Trigger the workflow

Go to **Actions → Refresh Data Catalog → Run workflow** to generate and deploy immediately.

After the first run, the site is live at `https://tubitv.github.io/tubi-data-catalog`.
The workflow runs automatically every night at 8 AM UTC (midnight PST).

## Freshness Thresholds

| Status | Criteria |
|--------|----------|
| Active (green) | `updated_at` within last 30 days |
| Stale (yellow) | `updated_at` older than 30 days |
| Unknown (gray) | API returned no date |
