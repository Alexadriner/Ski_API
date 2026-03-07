# Kreuzberg Scraper

This scraper targets `https://www.skilifte-kreuzberg.de/` and reads lift status directly from the homepage HTML.

## Files

- `scraper.py`: source-specific scraper implementation
- `collector.py`: loop runner (default every 300 seconds / 5 minutes)

## Quick run

```powershell
python -c "from pprint import pprint; from scripts.website_scrapers.kreuzberg import KreuzbergScraper; pprint(KreuzbergScraper().run('kreuzberg'))"
```

## Run collector once

```powershell
python -m scripts.website_scrapers.kreuzberg.collector --once
```

## Run continuously (every 5 minutes)

```powershell
python -m scripts.website_scrapers.kreuzberg.collector --interval-seconds 300
```

Disable API sync:

```powershell
python -m scripts.website_scrapers.kreuzberg.collector --interval-seconds 300 --no-sync-api
```

## Output

Normalized schema:

- `resort`: live metrics (open lift count, temperature, snow depth, source URLs)
- `lifts`: one row per lift (`Blicklift`, `Rothang`, `Dreitannen`)
- `slopes`: empty list (no per-slope status feed published on the source site)
