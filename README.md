# Floorball Player Index

Aggregated scorer statistics for all German floorball leagues across all 10 federations and 12 seasons.

Built automatically by GitHub Actions, served via GitHub Pages.

## Data Source

- API: `saisonmanager.de/api/v2`
- 1,943 leagues across 10 federations (Verbände)
- Seasons 6-17

## Usage

The player index is available at:
```
https://jarse32.github.io/floorball--player--index/player-index.json
```

## Build manually

```bash
pip install aiohttp
python scripts/build_index.py
```
