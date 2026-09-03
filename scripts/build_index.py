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
from collections import Counter, defaultdict
from datetime import datetime, timezone

API_BASE = "https://saisonmanager.de/api/v2"
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "docs", "player-index.json")

# API-Key ausschliesslich aus der Umgebung (GitHub Secret) — niemals im Code
API_KEY = os.environ.get("SAISONMANAGER_API_KEY", "")

# All 10 German floorball operation IDs (7 doesn't exist)
ALL_OPERATION_IDS = {1, 2, 3, 4, 5, 6, 8, 9, 10, 11}

# Season years loaded from API at runtime
SEASON_YEARS = {}  # populated by fetch_season_years()

# Rate limiting: Saisonmanager erlaubt laut offizieller API-Doku 60
# Anfragen/Minute pro Key (Header 'Retry-After' bei Ueberschreitung).
# MAX_CONCURRENT=1 + REQUEST_DELAY=1.1s heisst effektiv sequenzielle
# Anfragen, gut eine Sekunde auseinander - knapp unter dem Limit, mit
# kleinem Sicherheitsabstand gegen Timing-Jitter.
#
# Vorher: MAX_CONCURRENT=20 + REQUEST_DELAY=0.05s (~400 Requests/s
# Spitzenlast, ~400x ueber dem dokumentierten Limit) - das war die Ursache
# der 429-Kaskade, die ganze Ligen unbemerkt aus dem Index warf (siehe
# fetch_json: 429 wurde bisher wie ein normaler Fehler behandelt, Retry-
# After ignoriert, nach 3 Versuchen still None zurueckgegeben).
#
# Macht den Build spuerbar laenger (~2460 Ligen x 1.1s =~ 45 Minuten reine
# Scorer-Fetches statt vorher ~2 Minuten), dafuer vollstaendig statt
# loechrig - "lieber langsam und vollstaendig als schnell und loechrig".
# OFFEN: ob fuer diesen Key ein hoeheres Limit gilt (die Doku erwaehnt
# "Brauchst du dauerhaft mehr, melde dich"), ist NICHT verifiziert -
# bewusst konservativ am dokumentierten Wert geplant statt geraten.
MAX_CONCURRENT = 1
REQUEST_DELAY = 1.1

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


def resolve_display_name(candidates):
    """Waehlt (Vorname, Nachname) aus den gesammelten Scorer-Namenskandidaten.

    Regel: Der Name aus der NEUESTEN Saison gewinnt, in der der Spieler in
    einer Scorerliste auftaucht. Das korrigiert Erfassungsfehler in alten
    Ligen (frueher gewann der erste Treffer und wurde nie mehr korrigiert)
    und uebernimmt zugleich echte Namensaenderungen. Eine Mehrheitsregel
    waere hier falsch: bei einer echten Aenderung sind die alten Saisons
    meist in der Ueberzahl und wuerden den alten Namen zementieren.

    Tie-Break bei mehreren Ligen derselben Saison: haeufigstes
    (Vorname, Nachname)-Paar; bleibt es gleich, stabil die hoechste
    league_id. Vor- und Nachname werden immer als Paar uebernommen, nie
    einzeln gemischt.

    `candidates`: Liste von (season_int, league_id, first_name, last_name);
    nur Zeilen mit mindestens einem nicht-leeren Namensfeld. Leere Liste
    ergibt ("", "") — wie im bisherigen Verhalten.
    """
    if not candidates:
        return "", ""

    newest_season = max(season for season, _, _, _ in candidates)
    in_newest = [c for c in candidates if c[0] == newest_season]

    pair_counts = Counter((fn, ln) for _, _, fn, ln in in_newest)
    max_lid_for_pair = {}
    for _, lid, fn, ln in in_newest:
        key = (fn, ln)
        if lid > max_lid_for_pair.get(key, -1):
            max_lid_for_pair[key] = lid

    return max(
        pair_counts,
        key=lambda pair: (pair_counts[pair], max_lid_for_pair[pair]),
    )


async def fetch_json(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore):
    """Fetch JSON from URL, mit Ratelimit-Handling.

    Gibt ein (ok, data)-Tupel zurueck statt nur der Daten - der Aufrufer muss
    einen ECHTEN Fehlschlag (ok=False) von einer legitimen leeren/fehlenden
    Antwort (ok=True, data=None oder []) unterscheiden koennen. Vorher gab
    es nur einen Rueckgabewert und die Aufrufer prueften `if result:` - das
    wirft einen echten Fehlschlag (None) und eine echte leere Scorerliste
    ([], z.B. deaktiviert in U13-Ligen) in denselben Topf, weil beide in
    Python falsy sind. Folge: eine Liga mit legitim leerer Scorerliste sah
    wie ein Fehlschlag aus - und ein echter Fehlschlag verschwand lautlos,
    ohne dass die aufrufende Stelle das ueberhaupt bemerken konnte.
    """
    # Echte Fehler (Timeout, 5xx, Netzwerkfehler): kurze Grenze - eine
    # dauerhaft kaputte Liga soll den Build nicht ewig aufhalten.
    MAX_ERROR_ATTEMPTS = 3
    # HTTP 429 (Ratelimit) zaehlt bewusst NICHT gegen MAX_ERROR_ATTEMPTS und
    # bekommt ein deutlich hoeheres eigenes Limit: ein Ratelimit ist kein
    # Fehler der Liga oder des Servers, sondern bei ~2460 Ligen und einem
    # 60-Anfragen/Minute-Limit ein erwartbarer, temporaerer Zustand. Ohne
    # diese Trennung haette ein 429 denselben Effekt wie ein echter Fehler
    # (Liga faellt aus dem Index) - das war der urspruengliche Bug.
    MAX_RATE_LIMIT_ATTEMPTS = 10

    error_attempt = 0
    rate_limit_attempt = 0

    async with semaphore:
        while True:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                    if resp.status == 200:
                        return True, await resp.json()

                    if resp.status == 404:
                        return True, None

                    if resp.status == 401:
                        # Key fehlt oder ist ungueltig — sofort abbrechen statt
                        # sinnlos ueber alle Ligen zu retrien und am Ende eine
                        # unvollstaendige player-index.json zu bauen.
                        print("ERROR: HTTP 401 Unauthorized — SAISONMANAGER_API_KEY ungueltig oder von der API abgelehnt")
                        sys.exit(1)

                    if resp.status == 429:
                        rate_limit_attempt += 1
                        if rate_limit_attempt > MAX_RATE_LIMIT_ATTEMPTS:
                            print(f"  FAILED (Ratelimit haelt an nach {MAX_RATE_LIMIT_ATTEMPTS} Wartezyklen): {url}")
                            return False, None
                        # Retry-After ist die verbindliche Vorgabe des Servers
                        # (Sekunden) - nur wenn er fehlt, exponentielles
                        # Backoff als Fallback, gedeckelt auf 60s gegen einen
                        # ausufernden Einzel-Wartezyklus.
                        retry_after = resp.headers.get("Retry-After")
                        try:
                            wait = float(retry_after) if retry_after is not None else min(2 ** rate_limit_attempt, 60)
                        except ValueError:
                            wait = min(2 ** rate_limit_attempt, 60)
                        print(f"  HTTP 429 (Ratelimit) fuer {url} - warte {wait:.1f}s (Wartezyklus {rate_limit_attempt}/{MAX_RATE_LIMIT_ATTEMPTS})")
                        await asyncio.sleep(wait)
                        continue

                    # Alle anderen Status (5xx etc.): echter Fehler.
                    error_attempt += 1
                    print(f"  HTTP {resp.status} for {url}, retry {error_attempt}/{MAX_ERROR_ATTEMPTS}")
                    if error_attempt >= MAX_ERROR_ATTEMPTS:
                        print(f"  FAILED after {MAX_ERROR_ATTEMPTS} retries: {url}")
                        return False, None
                    await asyncio.sleep(1 * error_attempt)

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                error_attempt += 1
                print(f"  Error fetching {url}: {e}, retry {error_attempt}/{MAX_ERROR_ATTEMPTS}")
                if error_attempt >= MAX_ERROR_ATTEMPTS:
                    print(f"  FAILED after {MAX_ERROR_ATTEMPTS} retries: {url}")
                    return False, None
                await asyncio.sleep(1 * error_attempt)


async def fetch_season_years(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
    """Fetch season ID → year label mapping from init.json."""
    global SEASON_YEARS
    print("Fetching season years from init.json...")
    ok, data = await fetch_json(session, f"{API_BASE}/init.json", semaphore)
    if ok and data and "seasons" in data:
        SEASON_YEARS = {str(s["id"]): s["name"] for s in data["seasons"]}
        print(f"  Found {len(SEASON_YEARS)} seasons: {', '.join(f'{k}={v}' for k, v in sorted(SEASON_YEARS.items(), key=lambda x: int(x[0])))}")
    else:
        print("  WARNING: Could not fetch season years, using empty mapping")


async def fetch_all_leagues(session: aiohttp.ClientSession, semaphore: asyncio.Semaphore):
    """Fetch the complete leagues list."""
    print("Fetching all leagues...")
    ok, data = await fetch_json(session, f"{API_BASE}/leagues.json", semaphore)
    if not ok or not data:
        print("ERROR: Could not fetch leagues.json")
        sys.exit(1)

    # Filter to German floorball federations only
    leagues = [l for l in data if l.get("operation_id") in ALL_OPERATION_IDS]
    print(f"  Found {len(leagues)} leagues across {len(ALL_OPERATION_IDS)} federations")
    return leagues


async def fetch_scorer_list(session: aiohttp.ClientSession, league_id: int, semaphore: asyncio.Semaphore):
    """Fetch scorer list for a single league. Gibt (ok, liste) zurueck, siehe fetch_json."""
    url = f"{API_BASE}/leagues/{league_id}/scorer.json"
    return await fetch_json(session, url, semaphore)


async def build_index():
    """Main index building logic."""
    start_time = time.time()

    semaphore = asyncio.Semaphore(MAX_CONCURRENT)

    # Fail-Fast: ohne Key laeuft der naechtliche Build sonst 1.943 Ligen lang
    # ins Leere, bevor der Fehler ueberhaupt sichtbar wird
    if not API_KEY:
        print("ERROR: SAISONMANAGER_API_KEY nicht gesetzt")
        sys.exit(1)

    # Key als Header an die zentrale Session haengen — gilt automatisch fuer
    # alle Requests, die ueber diese Session laufen (fetch_json et al.)
    headers = {"X-Api-Key": API_KEY}
    async with aiohttp.ClientSession(headers=headers) as session:
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
        failed_league_ids = []  # Ligen mit ECHTEM Fehlschlag nach allen Retries

        for i in range(0, total, batch_size):
            batch = league_ids[i:i + batch_size]
            tasks = [fetch_scorer_list(session, lid, semaphore) for lid in batch]
            results = await asyncio.gather(*tasks)

            for lid, (ok, result) in zip(batch, results):
                if ok:
                    # result ist entweder eine (evtl. leere) Liste oder None
                    # bei HTTP 404 - beides ein legitimes Ergebnis, keine
                    # Liga faellt deswegen faelschlich aus dem Index (siehe
                    # fetch_json-Dokumentation).
                    all_scorers[lid] = result if result is not None else []
                else:
                    failed_league_ids.append(lid)

            done = min(i + batch_size, total)
            print(f"  Progress: {done}/{total} ({done*100//total}%, {len(failed_league_ids)} echte Fehlschlaege bisher)")
            await asyncio.sleep(REQUEST_DELAY)

        print(f"\n  Erfolgreich: {len(all_scorers)}/{total} Ligen ({len(failed_league_ids)} echte Fehlschlaege nach allen Retries)")

        # Fail-loud statt fail-silent: der bisherige Code baute bei
        # Fehlschlaegen einfach einen loechrigen Index und veroeffentlichte
        # ihn kommentarlos. Ein echter Fehlschlag nach MAX_ERROR_ATTEMPTS
        # Versuchen UND MAX_RATE_LIMIT_ATTEMPTS Ratelimit-Wartezyklen ist bei
        # korrekt gedrosseltem Durchsatz (siehe MAX_CONCURRENT/REQUEST_DELAY
        # oben) die Ausnahme, nicht die Regel - deshalb harter Abbruch statt
        # einer Toleranzschwelle: der GitHub-Actions-Lauf wird rot, git push
        # unterbleibt, die zuletzt veroeffentlichte, vollstaendige
        # player-index.json bleibt online. Analog zum Fail-Fast-Prinzip im
        # History-Builder (build.php) im iOS-Repo.
        if failed_league_ids:
            print(f"\n  FEHLGESCHLAGENE LIGEN ({len(failed_league_ids)}): {failed_league_ids}")
            print("  Breche ab, OHNE player-index.json zu schreiben - der bisherige, vollstaendige Stand bleibt online.")
            sys.exit(1)

        # Step 3: Aggregate by player_id
        print("\nAggregating player data...")
        players = {}  # player_id -> { fn, ln, entries: [...] }
        # Namenskandidaten pro Spieler: (season_int, league_id, fn, ln).
        # Der Anzeigename wird NICHT mehr beim ersten Treffer festgelegt,
        # sondern erst nach dem Sammeln aufgeloest (Step 3b) - sonst
        # zementiert ein Erfassungsfehler in einer alten Liga den Namen.
        name_candidates = defaultdict(list)

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
                        "fn": "",
                        "ln": "",
                        "entries": []
                    }

                # Nur Zeilen mit mindestens einem nicht-leeren Namensfeld
                # kommen als Kandidat infrage (leere Namen bleiben wie bisher
                # ausgeschlossen). fn/ln bleiben als Paar zusammen.
                if fn or ln:
                    season_int = int(season) if season.isdigit() else -1
                    name_candidates[pid_str].append((season_int, league_id, fn, ln))

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

        # Step 3b: Anzeigenamen aufloesen - Name aus der neuesten Saison
        # gewinnt (Details siehe resolve_display_name). Spieler ohne
        # Namenskandidaten behalten das ("", "") aus der Initialisierung.
        # Ausgabeformat bleibt unveraendert: fn/ln getrennt.
        for pid_str, cands in name_candidates.items():
            players[pid_str]["fn"], players[pid_str]["ln"] = resolve_display_name(cands)

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

        # Atomar schreiben (Temp-Datei + os.replace): ein Absturz/Kill mitten
        # im Schreiben (z.B. durch ein CI-Timeout) darf die zuletzt gueltige
        # player-index.json nicht durch einen halb geschriebenen Stand
        # ersetzen. os.replace ist auf demselben Dateisystem atomar.
        os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
        tmp_path = OUTPUT_PATH + ".tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(output, f, ensure_ascii=False, separators=(",", ":"))
        os.replace(tmp_path, OUTPUT_PATH)

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
