#!/usr/bin/env python3
"""
Builds the Floorball Player Index by fetching all scorer lists from all
leagues across all 10 German floorball federations (Verbände) and seasons.

Output: docs/player-index.json — a single JSON file consumed by the iOS app.
"""

import asyncio
import aiohttp
import json
import os
import sys
import time
from datetime import datetime, timezone

API_BASE = "https://saisonmanager.de/api/v2"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "docs", "player-index.json")

# All 10 German floorball operation IDs (7 doesn't exist)
ALL_OPERATION_IDS = {1, 2, 3, 4, 5, 6, 8, 9, 10, 11}

# Season years loaded from API at runtime
SEASON_YEARS = {}  # populated by fetch_season_years()

# Rate limiting: max concurrent requests
MAX_CONCURRENT = 20
REQUEST_DELAY = 0.05  # 50ms between batches

# Penalty minutes calculation: MS (Matchstrafe) = 25 min
PENALTY_MINUTES = {
    "penalty_2": 2,
    "penalty_2and2": 4,
    "penalty_5": 5,
    "penalty_10": 10,
    "penalty_ms_tech": 25,
    "penalty_ms_full": 25,
    "penalty_ms1": 25,
    "penalty_ms2": 25,
    "penalty_ms3": 25,
}


def calc_penalty_minutes(scorer: dict) -> int:
    """Calculate total penalty minutes from all penalty fields."""
    total = 0
    for field, minutes in PENALTY_MINUTES.items():
        count = scorer.get(field) or 0
        total += count * minutes
    return total


async def fetch_json(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore):
    """Fetch JSON from URL with rate limiting."""
    async with semaphore:
        for attempt in range(3):
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    elif resp.status == 404:
                        return None
                    else:
                        print(f"  HTTP {resp.status} for {url}, retry {attempt+1}")
                        await asyncio.sleep(1 * (attempt + 1))
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"  Error fetching {url}: {e}, retry {attempt+1}")
                await asyncio.sleep(1 * (attempt + 1))
        print(f"  FAILED after 3 retries: {url}")
        return None


async def fetch_season_years(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
    """Fetch season ID → year label mapping from init.json."""
    global SEASON_YEARS
    print("Fetching season years from init.json...")
    data = await fetch_json(session, f"{API_BASE}/init.json", semaphore)
    if data and "seasons" in data:
        SEASON_YEARS = {str(s["id"]): s["name"] for s in data["seasons"]}
        print(f"  Found {len(SEASON_YEARS)} seasons: {', '.join(f'{k}={v}' for k, v in sorted(SEASON_YEARS.items(), key=lambda x: int(x[0])))}")
    else:
        print("  WARNING: Could not fetch season years, using empty mapping")


async def fetch_all_leagues(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
    """Fetch the complete leagues list."""
    print("Fetching all leagues...")
    data = await fetch_json(session, f"{API_BASE}/leagues.json", semaphore)
    if not data:
        print("ERROR: Could not fetch leagues.json")
        sys.exit(1)

    # Filter to German floorball federations only
    leagues = [l for l in data if l.get("operation_id") in ALL_OPERATION_IDS]
    print(f"  Found {len(leagues)} leagues across {len(ALL_OPERATION_IDS)} federations")
    return leagues


async def fetch_scorer_list(session: aiohttp.ClientSession, league_id: int, semaphore: asyncio.Semaphore):
    """Fetch scorer list for a single league."""
    url = f"{API_BASE}/leagues/{league_id}/scorer.json"
    return await fetch_json(session, url, semaphore)


async def build_index():
    """Main index building logic."""
    start_time = time.time()

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    async with aiohttp.ClientSession() as session:
        # Step 0: Fetch season year mapping
        await fetch_season_years(session, semaphore)

        # Step 1: Get all leagues
        leagues = await fetch_all_leagues(session, semaphore)

        # Build league info lookup
        league_info = {}
        found_seasons = set()
        for l in leagues:
            season = l.get("season", "")
            found_seasons.add(season)
            league_info[l["id"]] = {
                "name": l.get("name", ""),
                "season": season,
                "operation_name": l.get("game_operation_name") or l.get("game_operation") or "",
            }

        # Step 2: Fetch all scorer lists in parallel
        league_ids = list(league_info.keys())
        total = len(league_ids)
        print(f"\nFetching {total} scorer lists...")

        # Process in batches
        batch_size = MAX_CONCURRENT
        all_scorers = {}  # league_id -> scorer list

        for i in range(0, total, batch_size):
            batch = league_ids[i:i + batch_size]
            tasks = [fetch_scorer_list(session, lid, semaphore) for lid in batch]
            results = await asyncio.gather(*tasks)

            for lid, result in zip(batch, results):
                if result:
                    all_scorers[lid] = result

            done = min(i + batch_size, total)
            print(f"  Progress: {done}/{total} ({done*100//total}%)")
            await asyncio.sleep(REQUEST_DELAY)

        print(f"\n  Successfully fetched {len(all_scorers)} scorer lists out of {total}")

        # Step 3: Aggregate by player_id
        print("\nAggregating player data...")
        players = {}  # player_id -> { fn, ln, entries: [...] }

        for league_id, scorers in all_scorers.items():
            info = league_info.get(league_id, {})
            season = info.get("season", "")
            league_name = info.get("name", "")
            operation_name = info.get("operation_name", "")

            for scorer in scorers:
                pid = scorer.get("player_id")
                if not pid:
                    continue

                pid_str = str(pid)
                fn = scorer.get("first_name") or ""
                ln = scorer.get("last_name") or ""
                goals = scorer.get("goals") or 0
                assists = scorer.get("assists") or 0
                games = scorer.get("games") or 0
                pm = calc_penalty_minutes(scorer)
                team_name = scorer.get("team_name") or ""
                team_id = scorer.get("team_id") or 0

                if pid_str not in players:
                    players[pid_str] = {
                        "fn": fn,
                        "ln": ln,
                        "entries": []
                    }
                else:
                    # Update name if current one is better (non-empty)
                    if fn and not players[pid_str]["fn"]:
                        players[pid_str]["fn"] = fn
                    if ln and not players[pid_str]["ln"]:
                        players[pid_str]["ln"] = ln

                players[pid_str]["entries"].append({
                    "s": season,
                    "lid": league_id,
                    "ln": league_name,
                    "op": operation_name,
                    "tid": team_id,
                    "tn": team_name,
                    "g": goals,
                    "a": assists,
                    "gp": games,
                    "pm": pm,
                })

        # Step 4: Sort entries per player (newest season first, then by points desc)
        for pid_str in players:
            players[pid_str]["entries"].sort(
                key=lambda e: (-int(e["s"]) if e["s"].isdigit() else 0, -(e["g"] + e["a"]))
            )

        # Step 5: Build season year mapping (only for seasons found in data)
        season_years = {s: SEASON_YEARS[s] for s in sorted(found_seasons) if s in SEASON_YEARS}

        # Step 6: Write output
        output = {
            "version": 2,
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "player_count": len(players),
            "league_count": len(all_scorers),
            "seasons": season_years,
            "players": players,
        }

        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, separators=(",", ":"))

        elapsed = time.time() - start_time
        file_size = os.path.getsize(OUTPUT_PATH)

        print(f"\nDone!")
        print(f"  Players: {len(players):,}")
        print(f"  Leagues processed: {len(all_scorers):,}")
        print(f"  Seasons: {season_years}")
        print(f"  Output: {OUTPUT_PATH}")
        print(f"  File size: {file_size / 1024 / 1024:.1f} MB")
        print(f"  Elapsed: {elapsed:.1f}s")


if __name__ == "__main__":
    asyncio.run(build_index())
