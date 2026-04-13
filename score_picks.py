"""
EdgeIQ MVP — Daily Picks Scorer
Uses only: requests, python-dotenv
"""

import os
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

ODDS_API_KEY = os.environ["ODDS_API_KEY"]
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

SPORTS = [
    "basketball_nba",
    "americanfootball_nfl",
    "baseball_mlb",
    "icehockey_nhl",
]

MAX_PICKS = 3

WEIGHTS = {
    "line_movement": 0.45,
    "fade_public":   0.35,
    "juice_value":   0.20,
}

# ── Supabase helpers ──────────────────────────────────────────────────

def sb_headers():
    return {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "return=minimal",
    }

def sb_delete(table, filters):
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=sb_headers(),
        params=filters
    )
    if r.status_code not in (200, 204):
        print(f"  Warning: delete {r.status_code}: {r.text}")

def sb_insert(table, record):
    r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers=sb_headers(),
        json=record
    )
    if r.status_code not in (200, 201):
        print(f"  Warning: insert {r.status_code}: {r.text}")

# ── scoring helpers ───────────────────────────────────────────────────

def american_to_implied_prob(odds):
    odds = int(odds)
    if odds > 0:
        return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)

def expected_value(confidence, odds, stake=100):
    win_prob  = confidence / 100
    loss_prob = 1 - win_prob
    odds      = int(odds)
    if odds == 0:
        return 0   # no odds data — skip this pick
    profit    = stake * (odds / 100) if odds > 0 else stake * (100 / abs(odds))
    return round((win_prob * profit) - (loss_prob * stake), 2)

def safe_avg(values):
    """Average a list — returns 0 if the list is empty (prevents ZeroDivisionError)."""
    return sum(values) / len(values) if values else 0

def score_game(game):
    picks      = []
    bookmakers = game.get("bookmakers", [])
    if not bookmakers:
        return picks

    spreads = {}
    juices  = {}

    for book in bookmakers:
        for market in book.get("markets", []):
            if market["key"] not in ("spreads", "h2h"):
                continue
            for outcome in market["outcomes"]:
                name  = outcome["name"]
                price = outcome.get("price", -110)
                point = outcome.get("point", 0)
                spreads.setdefault(name, []).append(point)
                juices.setdefault(name,  []).append(price)

    if not juices:
        return picks

    teams = list(juices.keys())
    if len(teams) < 2:
        return picks

    for team in teams:
        # use safe_avg everywhere — no more ZeroDivisionError
        avg_juice   = safe_avg(juices.get(team, []))
        avg_spread  = safe_avg(spreads.get(team, []))
        other_team  = [t for t in teams if t != team][0]
        other_juice = safe_avg(juices.get(other_team, []))

        if avg_juice == 0 or other_juice == 0:
            continue   # skip this team if we have no usable odds

        implied          = american_to_implied_prob(avg_juice)
        line_move_signal = min(max((implied - 0.48) * 5, 0), 1)

        fade_signal = 0.0
        if other_juice < -150:
            fade_signal = min((abs(other_juice) - 150) / 200, 1.0)

        juice_signal = max(0, 1 - (abs(avg_juice) - 100) / 100)

        raw = (
            line_move_signal * WEIGHTS["line_movement"] +
            fade_signal      * WEIGHTS["fade_public"]   +
            juice_signal     * WEIGHTS["juice_value"]
        )
        confidence = round(40 + raw * 45, 1)
        ev         = expected_value(confidence, avg_juice)

        if ev <= 0:
            continue

        picks.append({
            "sport":      game["sport_key"],
            "home_team":  game["home_team"],
            "away_team":  game["away_team"],
            "pick_team":  team,
            "pick_line":  f"{team} {avg_spread:+.1f}" if abs(avg_spread) > 0.1 else f"{team} ML",
            "odds":       int(avg_juice),
            "confidence": confidence,
            "ev":         ev,
            "game_time":  game["commence_time"],
            "signals": {
                "line_movement": round(line_move_signal, 3),
                "fade_public":   round(fade_signal, 3),
                "juice_value":   round(juice_signal, 3),
            },
            "num_books":  len(bookmakers),
        })

    return picks

# ── main ──────────────────────────────────────────────────────────────

def fetch_and_score():
    all_picks = []

    for sport in SPORTS:
        print(f"Fetching {sport}...")
        try:
            resp = requests.get(
                f"https://api.the-odds-api.com/v4/sports/{sport}/odds/",
                params={
                    "apiKey":     ODDS_API_KEY,
                    "regions":    "us",
                    "markets":    "spreads,h2h",
                    "oddsFormat": "american",
                },
                timeout=15,
            )
        except requests.exceptions.RequestException as e:
            print(f"  Network error for {sport}: {e}")
            continue

        if resp.status_code == 401:
            print("  ERROR: Invalid ODDS_API_KEY — check your .env file.")
            return
        if resp.status_code == 422:
            print(f"  {sport} is out of season, skipping.")
            continue
        if resp.status_code != 200:
            print(f"  Unexpected error {resp.status_code}: {resp.text}")
            continue

        games = resp.json()
        print(f"  {len(games)} games found")
        for game in games:
            all_picks.extend(score_game(game))

    if not all_picks:
        print("\nNo picks with positive EV today — nothing written.")
        return

    all_picks.sort(key=lambda p: p["ev"], reverse=True)
    top_picks = all_picks[:MAX_PICKS]

    today = datetime.now(timezone.utc).date().isoformat()
    print(f"\nWriting top {len(top_picks)} picks to Supabase for {today}...")

    sb_delete("picks", {"pick_date": f"eq.{today}"})

    for rank, pick in enumerate(top_picks, 1):
        record = {**pick, "pick_date": today, "rank": rank, "result": None}
        sb_insert("picks", record)
        print(f"  #{rank}  {pick['pick_line']:<35} conf {pick['confidence']}%   EV {pick['ev']:+.1f}")

    print(f"\n✅ Done! Check Supabase table editor to confirm the rows.")

if __name__ == "__main__":
    fetch_and_score()
