# Setup Status

## Completed

- [x] Repo created: https://github.com/adRise/tubi-data-catalog
- [x] Code pushed to `main`
- [x] GitHub Pages enabled (source: GitHub Actions)
- [x] `TABLEAU_SERVER_URL` secret set → `https://10ay.online.tableau.com`
- [x] `TABLEAU_SITE_ID` secret set → `tubianalytics`

## Remaining

### Tableau (blocked — needs site admin to enable PAT creation)
- [ ] `TABLEAU_TOKEN_NAME` — name of a Personal Access Token
- [ ] `TABLEAU_TOKEN_VALUE` — the token secret
  - To create: Tableau Cloud → avatar → My Account Settings → Personal Access Tokens

### Preset
- [ ] `PRESET_API_KEY`
- [ ] `PRESET_API_SECRET`
  - To create: Preset workspace → Settings → API Keys
- [ ] `PRESET_WORKSPACE_URL` — e.g. `https://abc123.us1a.app.preset.io`

### Databricks
- [ ] `DATABRICKS_HOST` — e.g. `https://adb-123.azuredatabricks.net`
- [ ] `DATABRICKS_TOKEN`
  - To create: Databricks workspace → User Settings → Developer → Access Tokens → Generate

## To Resume in Claude

Once you have credentials, open this repo in Claude Code and say:
> "Resume setting up GitHub secrets for tubi-data-catalog"

Use this command to set each secret:
```bash
gh secret set SECRET_NAME --repo adRise/tubi-data-catalog --body "value"
```

## Final Step (after all secrets are set)

Trigger the first run:
```bash
gh workflow run refresh.yml --repo adRise/tubi-data-catalog
```

Or via GitHub UI: Actions → Refresh Data Catalog → Run workflow
