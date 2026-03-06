# Palisades Tahoe Scraper

This scraper targets `https://www.palisadestahoe.com/` and checks `robots.txt` before requesting pages.

## Files

- `scraper.py`: source-specific scraper implementation
- `collector.py`: loop runner (default every 300 seconds / 5 minutes)

## Run once

```powershell
python -m scripts.website_scrapers.palisades_tahoe.collector --once
```

## Run continuously (every 5 minutes)

```powershell
python -m scripts.website_scrapers.palisades_tahoe.collector --interval-seconds 300
```

By default, collector also syncs scraped values to your API (`/resorts`, `/lifts`, `/slopes`).

Disable API sync:

```powershell
python -m scripts.website_scrapers.palisades_tahoe.collector --interval-seconds 300 --no-sync-api
```
