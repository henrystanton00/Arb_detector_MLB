# Market Gap — MLB prediction market screener

Compares Kalshi, Polymarket, and de-vigged sportsbook consensus for every
MLB game in the next few days, and flags value signals and cross-venue arbs.

## What's in this folder

```
pipeline.py                    the data pipeline (fetch, match, compute)
docs/index.html                the site (static, no build step)
docs/snapshot.json             sample data so the page isn't empty on first load
.github/workflows/update-snapshot.yml   automation: runs the pipeline every 30 min
```

## One-time setup (about 10 minutes)

### 1. Create the repo and push these files
```powershell
cd v1predmarket
git init
git add .
git commit -m "Initial commit: Market Gap screener"
git branch -M main
git remote add origin https://github.com/<your-username>/<repo-name>.git
git push -u origin main
```

### 2. Add your SportsGameOdds key as a secret
GitHub repo -> **Settings** -> **Secrets and variables** -> **Actions** ->
**New repository secret**
- Name: `SGO_API_KEY`
- Value: your key

This keeps the key out of the code and out of the public repo entirely.

### 3. Let Actions write back to the repo
GitHub repo -> **Settings** -> **Actions** -> **General** -> scroll to
**Workflow permissions** -> select **Read and write permissions** -> Save.

(Without this, the workflow can run the pipeline but can't push the
updated `snapshot.json` back — it'll fail silently on the commit step.)

### 4. Turn on GitHub Pages
GitHub repo -> **Settings** -> **Pages** -> **Source**: "Deploy from a
branch" -> **Branch**: `main`, folder **`/docs`** -> Save.

Your site will be live at `https://<your-username>.github.io/<repo-name>/`
within a minute or two.

### 5. Run it once manually to confirm it works
GitHub repo -> **Actions** tab -> **Update snapshot** workflow -> **Run
workflow** button. Watch it go green, then refresh your site — the sample
data should be replaced with a real, live snapshot.

## Adjusting the refresh schedule

Edit the `cron` line in `.github/workflows/update-snapshot.yml`. It's
currently every 30 minutes (`*/30 * * * *`), in UTC. Free-tier SportsGameOdds
budget (2,500 objects/month) is the main constraint — every run costs
roughly one object per game in the horizon window, so 30 minutes is
comfortable; tightening much past 15 minutes on a full MLB slate could
burn through the monthly allowance before the season's over.

## Making changes later

Any edit to `pipeline.py` (new leagues, tweaked filters, etc.) just needs
a normal `git push` — the next scheduled run picks it up automatically.
Edits to `docs/index.html` (styling, copy) go live the moment you push,
no workflow run needed.
