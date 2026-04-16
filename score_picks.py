"""
EdgeIQ MVP — Daily Picks Scorer
Signals: line movement, fade public, juice value, weather, player props
Uses only: requests, python-dotenv
"""

import os
import json
import requests
from datetime import datetime, timezone
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()

ODDS_API_KEY   = os.environ["ODDS_API_KEY"]

# ── request cache ─────────────────────────────────────────────────────
# Saves API responses to disk so re-running the script in the same hour
# uses cached data instead of burning API quota.
CACHE_DIR = Path(".cache")
CACHE_DIR.mkdir(exist_ok=True)

def cache_get(key):
    """Return cached data if it exists and is less than 55 minutes old."""
    path = CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None
    age = datetime.now().timestamp() - path.stat().st_mtime
    if age > 55 * 60:  # 55 minutes
        return None
    with open(path, "r") as f:
        return json.load(f)

def cache_set(key, data):
    """Save data to cache."""
    path = CACHE_DIR / f"{key}.json"
    with open(path, "w") as f:
        json.dump(data, f)

def cached_get(url, params, cache_key, timeout=15):
    """Make a GET request, returning cached result if available."""
    cached = cache_get(cache_key)
    if cached is not None:
        print(f"  (cached) {cache_key}")
        return cached
    resp = requests.get(url, params=params, timeout=timeout)
    if resp.status_code == 200:
        data = resp.json()
        cache_set(cache_key, data)
        return data
    return None
SUPABASE_URL   = os.environ["SUPABASE_URL"]
SUPABASE_KEY   = os.environ["SUPABASE_KEY"]
WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY", "")  # free at openweathermap.org

SPORTS = [
    "basketball_nba",
    "americanfootball_nfl",
    "baseball_mlb",
    "icehockey_nhl",
]

# sports played outdoors where weather matters
OUTDOOR_SPORTS = {"americanfootball_nfl", "baseball_mlb"}

# sports where run line / puck line corrupts spread data
RUN_LINE_SPORTS = {"baseball_mlb", "icehockey_nhl"}

MAX_PICKS = 6  # 3 sides + 3 totals

WEIGHTS = {
    "line_movement":  0.16,
    "no_vig_edge":    0.13,
    "reverse_line":   0.11,
    "team_form":      0.13,  # new — team quality filter
    "steam_move":     0.08,
    "clv":            0.08,
    "injury":         0.07,
    "book_disagree":  0.06,
    "rest_days":      0.05,
    "weather":        0.05,
    "prop_signal":    0.04,
    "juice_value":    0.02,
    "fade_public":    0.02,
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

def sb_select(table, params):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={**sb_headers(), "Prefer": "return=representation"},
        params=params
    )
    if r.status_code == 200:
        return r.json()
    return []

# ── weather signal ────────────────────────────────────────────────────

# map team names to their city for weather lookup
TEAM_CITIES = {
    # NFL
    "Buffalo Bills": "Buffalo",       "Miami Dolphins": "Miami",
    "New England Patriots": "Boston", "New York Jets": "New York",
    "New York Giants": "New York",    "Philadelphia Eagles": "Philadelphia",
    "Dallas Cowboys": "Dallas",       "Washington Commanders": "Washington",
    "Chicago Bears": "Chicago",       "Green Bay Packers": "Green Bay",
    "Minnesota Vikings": "Minneapolis","Detroit Lions": "Detroit",
    "Atlanta Falcons": "Atlanta",     "Carolina Panthers": "Charlotte",
    "New Orleans Saints": "New Orleans","Tampa Bay Buccaneers": "Tampa",
    "Los Angeles Rams": "Los Angeles","Seattle Seahawks": "Seattle",
    "San Francisco 49ers": "San Francisco","Arizona Cardinals": "Phoenix",
    "Denver Broncos": "Denver",       "Las Vegas Raiders": "Las Vegas",
    "Kansas City Chiefs": "Kansas City","Los Angeles Chargers": "Los Angeles",
    "Baltimore Ravens": "Baltimore",  "Cleveland Browns": "Cleveland",
    "Pittsburgh Steelers": "Pittsburgh","Cincinnati Bengals": "Cincinnati",
    "Jacksonville Jaguars": "Jacksonville","Tennessee Titans": "Nashville",
    "Houston Texans": "Houston",      "Indianapolis Colts": "Indianapolis",
    # MLB
    "New York Yankees": "New York",   "Boston Red Sox": "Boston",
    "Toronto Blue Jays": "Toronto",   "Baltimore Orioles": "Baltimore",
    "Tampa Bay Rays": "Tampa",        "Chicago White Sox": "Chicago",
    "Cleveland Guardians": "Cleveland","Detroit Tigers": "Detroit",
    "Kansas City Royals": "Kansas City","Minnesota Twins": "Minneapolis",
    "Houston Astros": "Houston",      "Los Angeles Angels": "Los Angeles",
    "Oakland Athletics": "Oakland",   "Seattle Mariners": "Seattle",
    "Texas Rangers": "Dallas",        "Atlanta Braves": "Atlanta",
    "Miami Marlins": "Miami",         "New York Mets": "New York",
    "Philadelphia Phillies": "Philadelphia","Washington Nationals": "Washington",
    "Chicago Cubs": "Chicago",        "Cincinnati Reds": "Cincinnati",
    "Milwaukee Brewers": "Milwaukee", "Pittsburgh Pirates": "Pittsburgh",
    "St. Louis Cardinals": "St. Louis","Arizona Diamondbacks": "Phoenix",
    "Colorado Rockies": "Denver",     "Los Angeles Dodgers": "Los Angeles",
    "San Diego Padres": "San Diego",  "San Francisco Giants": "San Francisco",
}

# stadiums that are domed — weather irrelevant
DOME_TEAMS = {
    "Atlanta Falcons", "New Orleans Saints", "Las Vegas Raiders",
    "Indianapolis Colts", "Minnesota Vikings", "Detroit Lions",
    "Houston Texans", "Arizona Cardinals", "Los Angeles Rams",
    "Los Angeles Chargers", "Tampa Bay Rays", "Milwaukee Brewers",
    "Toronto Blue Jays", "Miami Marlins", "Seattle Mariners",
}

def get_weather_signal(home_team, sport):
    """
    Returns a weather signal (0-1) for outdoor games.
    High wind or rain/snow boosts UNDER picks specifically.
    Returns 0.5 (neutral) if no weather key or indoor game.
    """
    if sport not in OUTDOOR_SPORTS:
        return 0.5  # neutral for indoor sports

    if home_team in DOME_TEAMS:
        return 0.5  # neutral for dome stadiums

    if not WEATHER_API_KEY:
        return 0.5  # no key configured

    city = TEAM_CITIES.get(home_team, "")
    if not city:
        return 0.5

    try:
        resp = requests.get(
            "https://api.openweathermap.org/data/2.5/weather",
            params={
                "q":     city + ",US",
                "appid": WEATHER_API_KEY,
                "units": "imperial",
            },
            timeout=8,
        )
        if resp.status_code != 200:
            return 0.5

        data    = resp.json()
        wind    = data.get("wind", {}).get("speed", 0)   # mph
        weather = data.get("weather", [{}])[0].get("main", "")
        temp    = data.get("main", {}).get("temp", 70)   # fahrenheit

        signal = 0.5  # start neutral

        # high wind kills scoring — especially in NFL and MLB
        if wind > 20:
            signal += 0.3
        elif wind > 15:
            signal += 0.2
        elif wind > 10:
            signal += 0.1

        # rain or snow reduces scoring
        if weather in ("Rain", "Snow", "Drizzle", "Thunderstorm"):
            signal += 0.2

        # extreme cold reduces scoring
        if temp < 32:
            signal += 0.15
        elif temp < 40:
            signal += 0.1

        signal = min(signal, 1.0)
        print(f"  Weather {city}: {wind}mph wind, {weather}, {temp}F → signal {signal:.2f}")
        return signal

    except Exception as e:
        print(f"  Weather error for {city}: {e}")
        return 0.5

# ── player props signal ───────────────────────────────────────────────

def get_props_signal(sport, game_id):
    """
    Fetches player props for a game and returns a signal (0-1)
    based on whether prop lines suggest a high or low scoring game.
    Higher signal = more scoring expected = favor overs/favorites.
    Returns 0.5 (neutral) if no props available.
    """
    # only fetch props for sports where they're meaningful
    if sport not in ("basketball_nba", "americanfootball_nfl", "baseball_mlb"):
        return 0.5

    prop_markets = {
        "basketball_nba":      "player_points",
        "americanfootball_nfl":"player_pass_tds,player_rush_yards",
        "baseball_mlb":        "pitcher_strikeouts",  # strikeouts more predictive than HRs
    }
    markets = prop_markets.get(sport, "")
    if not markets:
        return 0.5

    try:
        data = cached_get(
            f"https://api.the-odds-api.com/v4/sports/{sport}/events/{game_id}/odds",
            params={
                "apiKey":     ODDS_API_KEY,
                "regions":    "us",
                "markets":    markets,
                "oddsFormat": "american",
            },
            cache_key=f"props_{sport}_{game_id}",
        )
        if data is None:
            return 0.5
        bookmakers = data.get("bookmakers", [])
        if not bookmakers:
            return 0.5

        # collect all prop lines
        lines = []
        for book in bookmakers[:3]:  # limit to 3 books
            for market in book.get("markets", []):
                for outcome in market.get("outcomes", []):
                    point = outcome.get("point", 0)
                    if point and point > 0:
                        lines.append(point)

        if not lines:
            return 0.5

        avg_line = sum(lines) / len(lines)

        # normalize — scale each sport to its realistic range
        # Neutral = 0.5, above average scoring = > 0.5
        # NBA player points: 15 = low, 20 = avg, 28 = high
        # NFL pass TDs: 1.5 = avg, rush yards: 65 = avg
        # MLB strikeouts: 5 = low, 7 = avg, 9 = high
        if sport == "basketball_nba":
            # center around 20 pts average
            signal = 0.5 + (avg_line - 20.0) / 40.0
        elif sport == "americanfootball_nfl":
            # center around 65 rush yards or 1.5 TDs average
            if avg_line > 10:  # rush yards
                signal = 0.5 + (avg_line - 65.0) / 130.0
            else:              # TDs
                signal = 0.5 + (avg_line - 1.5) / 3.0
        else:
            # MLB strikeouts: center around 7
            signal = 0.5 + (avg_line - 7.0) / 14.0

        # clamp to 0.1 - 0.9 range (never fully 0 or 1)
        signal = max(0.1, min(0.9, signal))

        print(f"  Props signal for {sport}: avg line {avg_line:.1f} → signal {signal:.2f}")
        return signal

    except Exception as e:
        print(f"  Props error: {e}")
        return 0.5

# ── ESPN injury signal ───────────────────────────────────────────────

# map sport keys to ESPN sport paths
ESPN_SPORT_MAP = {
    "basketball_nba":       "basketball/nba",
    "americanfootball_nfl": "football/nfl",
    "baseball_mlb":         "baseball/mlb",
    "icehockey_nhl":        "hockey/nhl",
}

# cache injuries for the session so we only fetch once per sport
_injury_cache = {}

def get_espn_injuries(sport):
    """
    Fetch injured/questionable players from ESPN for a sport.
    Returns a dict: team_name -> list of injured player names
    """
    if sport in _injury_cache:
        return _injury_cache[sport]

    sport_path = ESPN_SPORT_MAP.get(sport, "")
    if not sport_path:
        return {}

    try:
        url  = f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/injuries"
        data = cached_get(url, {}, cache_key=f"espn_injuries_{sport}")
        if data is None:
            print(f"  ESPN injuries {sport}: fetch failed")
            _injury_cache[sport] = {}
            return {}
        injuries = {}

        for team_entry in data.get("injuries", []):
            team_name = team_entry.get("team", {}).get("displayName", "")
            if not team_name:
                continue
            injured_players = []
            for inj in team_entry.get("injuries", []):
                status   = inj.get("status", "").lower()
                player   = inj.get("athlete", {}).get("displayName", "")
                position = inj.get("athlete", {}).get("position", {}).get("abbreviation", "")
                # only flag significant injuries — out or doubtful
                if status in ("out", "doubtful") and player:
                    injured_players.append({
                        "name":     player,
                        "status":   status,
                        "position": position,
                    })
            if injured_players:
                injuries[team_name] = injured_players

        _injury_cache[sport] = injuries
        print(f"  ESPN injuries {sport}: {len(injuries)} teams with injuries")
        return injuries

    except Exception as e:
        print(f"  ESPN injury fetch error for {sport}: {e}")
        _injury_cache[sport] = {}
        return {}

def get_injury_signal(team, sport, injuries):
    """
    Returns an injury impact signal (0-1) for a team.
    0.5 = neutral (no injuries)
    Lower = team has key players out (bad for that team)
    Higher = opponent has key players out (good for this team)

    Key positions by sport:
    - NBA: G (guard), F (forward), C (center) — all important
    - NFL: QB is critical, WR/RB matter
    - MLB: SP (starting pitcher) is most critical
    - NHL: G (goalie) is most critical
    """
    if not injuries:
        return 0.5

    team_injuries = injuries.get(team, [])
    if not team_injuries:
        return 0.5  # no injuries = slight edge

    # weight injuries by position importance
    POSITION_WEIGHTS = {
        # NBA
        "PG": 0.8, "SG": 0.7, "SF": 0.7, "PF": 0.6, "C": 0.6,
        "G":  0.7, "F":  0.6,
        # NFL
        "QB": 1.0, "WR": 0.6, "RB": 0.5, "TE": 0.5,
        "CB": 0.5, "DE": 0.5, "LB": 0.4,
        # MLB
        "SP": 0.9, "RP": 0.4, "C":  0.5,
        "1B": 0.4, "2B": 0.4, "SS": 0.5, "3B": 0.4,
        "LF": 0.4, "CF": 0.5, "RF": 0.4, "DH": 0.4,
        # NHL
        "G":  0.9, "D":  0.5, "LW": 0.5, "RW": 0.5, "C": 0.6,
    }

    impact = 0.0
    for inj in team_injuries:
        pos    = inj.get("position", "")
        status = inj.get("status", "")
        weight = POSITION_WEIGHTS.get(pos, 0.4)
        if status == "out":
            impact += weight
        elif status == "doubtful":
            impact += weight * 0.6

    # convert impact to a 0-1 signal
    # higher impact = worse for this team = lower signal for this team
    signal = max(0.0, 0.5 - min(impact * 0.3, 0.4))

    if impact > 0:
        names = [i["name"] for i in team_injuries[:2]]
        print(f"  Injury signal {team}: {', '.join(names)} → signal {signal:.2f}")

    return round(signal, 3)


# ── No-vig edge signal ───────────────────────────────────────────────
# Remove the vig from each book's odds to get the "true" probability.
# If the best available odds imply a better probability than the no-vig
# market price, there is genuine positive expected value.

def get_no_vig_edge(juice_list, best_juice):
    """
    Compare best available price to the no-vig consensus price.
    Returns 0-1 signal — higher = bigger edge vs true probability.

    No-vig calculation: average implied probs from both sides,
    normalize so they sum to 1, that's the true probability.
    We compare best_juice implied prob to that true probability.
    """
    if len(juice_list) < 3 or best_juice == 0:
        return 0.5

    def implied(odds):
        odds = int(odds)
        if odds == 0: return 0.5
        if odds > 0:  return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)

    # average implied probability across all books (includes vig)
    avg_implied = sum(implied(j) for j in juice_list) / len(juice_list)

    # best available odds implied probability (no-vig adjusted)
    best_implied = implied(best_juice)

    # edge = how much better is our best price vs the vigged consensus
    # positive = we are getting better than fair value
    edge = best_implied - avg_implied

    # scale to 0-1 signal
    # edge of 0 = neutral, edge of +0.05 = strong, edge of -0.05 = bad
    signal = 0.5 + (edge * 8)  # wider scale — small edges matter more
    return max(0.1, min(0.9, round(signal, 3)))


# ── Reverse line movement signal ─────────────────────────────────────
# RLM happens when public money is on one side but the line moves
# the other way. Sharp money overcoming the public = very strong signal.
# We detect it by comparing which side has more books at the current
# price vs the opening price direction.

def get_reverse_line_signal(game_id, team, other_team,
                             avg_juice, other_juice, opening_lines):
    """
    Detect reverse line movement.
    Public loves favorites and home teams.
    If the line moves AWAY from the perceived public side,
    sharp money is countering — that's the signal.
    Returns 0-1.
    """
    # need opening lines to detect movement
    team_key  = (game_id, team)
    other_key = (game_id, other_team)

    if team_key not in opening_lines or other_key not in opening_lines:
        return 0.5

    def implied(odds):
        odds = int(odds)
        if odds == 0: return 0.5
        if odds > 0:  return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)

    opening_team  = implied(opening_lines[team_key])
    opening_other = implied(opening_lines[other_key])
    current_team  = implied(avg_juice)
    current_other = implied(other_juice)

    # which team was the opening favorite?
    opening_fav_is_other = opening_other > opening_team

    # how did the line move?
    team_movement  = current_team  - opening_team   # positive = moved toward team
    other_movement = current_other - opening_other  # positive = moved toward other

    # RLM: line moved toward THIS team despite other team being the public fav
    if opening_fav_is_other and team_movement > 0.01:
        # sharp money moved the line our way against public side
        signal = 0.6 + min(team_movement * 8, 0.3)
        return min(0.9, round(signal, 3))

    # line moved away from our team despite them being the fav (bad sign)
    if not opening_fav_is_other and team_movement < -0.01:
        signal = 0.4 - min(abs(team_movement) * 8, 0.3)
        return max(0.1, round(signal, 3))

    return 0.5  # no clear RLM


# ── Steam move signal ─────────────────────────────────────────────────
# A steam move is when sharp groups hit multiple books simultaneously,
# causing rapid coordinated line movement.
# Detected by checking if multiple consecutive snapshots all moved
# in the same direction — a pattern that doesn't happen by accident.

def get_steam_signal(game_id, team, today):
    """
    Check for steam move: 3+ consecutive snapshots all moving
    in the same direction within a short window.
    Uses your existing odds_snapshots Supabase table.
    Returns 0-1 signal.
    """
    try:
        rows = sb_select("odds_snapshots", {
            "game_id":   f"eq.{game_id}",
            "team":      f"eq.{team}",
            "snap_date": f"eq.{today}",
            "order":     "snapshot_time.asc",
            "select":    "avg_juice,snapshot_time",
        })

        if len(rows) < 3:
            return 0.5  # not enough snapshots yet — need 3+ hourly snapshots

        def implied(odds):
            odds = int(odds) if odds else 0
            if odds == 0: return 0.5
            if odds > 0:  return 100 / (odds + 100)
            return abs(odds) / (abs(odds) + 100)

        # get implied probs in order
        probs = [implied(r["avg_juice"]) for r in rows if r.get("avg_juice")]
        if len(probs) < 3:
            return 0.5

        # check last 3 movements — all same direction?
        movements = [probs[i+1] - probs[i] for i in range(len(probs)-1)]
        last_3    = movements[-3:]

        all_up   = all(m > 0.005 for m in last_3)
        all_down = all(m < -0.005 for m in last_3)

        if all_up:
            # steam toward this team — total movement magnitude
            total_move = sum(last_3)
            signal = 0.6 + min(total_move * 5, 0.3)
            return min(0.9, round(signal, 3))

        if all_down:
            # steam away from this team
            total_move = sum(abs(m) for m in last_3)
            signal = 0.4 - min(total_move * 5, 0.3)
            return max(0.1, round(signal, 3))

        return 0.5  # no steam detected

    except Exception as e:
        return 0.5


# ── Team form / win rate signal ──────────────────────────────────────
# Pulls current season win% from ESPN scoreboard.
# Bad teams (sub .400) get penalized, good teams (.550+) get a boost.
# This prevents the model from blindly picking terrible teams at good odds.

_team_form_cache = {}

def get_team_form(sport):
    """
    Fetch current season records for all teams from ESPN scoreboard.
    Returns dict: team_name -> win_pct (0.0 - 1.0)
    """
    if sport in _team_form_cache:
        return _team_form_cache[sport]

    ESPN_SPORT_MAP = {
        "basketball_nba":       "basketball/nba",
        "americanfootball_nfl": "football/nfl",
        "baseball_mlb":         "baseball/mlb",
        "icehockey_nhl":        "hockey/nhl",
    }
    sport_path = ESPN_SPORT_MAP.get(sport, "")
    if not sport_path:
        return {}

    cache_key = f"team_form_{sport}"
    cached    = cache_get(cache_key)
    if cached is not None:
        _team_form_cache[sport] = cached
        return cached

    try:
        resp = requests.get(
            f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/teams",
            timeout=10,
        )
        if resp.status_code != 200:
            return {}

        data  = resp.json()
        teams = data.get("sports", [{}])[0].get("leagues", [{}])[0].get("teams", [])
        form  = {}

        for entry in teams:
            team = entry.get("team", {})
            name = team.get("displayName", "")
            record = team.get("record", {}).get("items", [])
            if not record:
                continue
            # first record item is overall season record
            stats = record[0].get("stats", [])
            wins   = next((s["value"] for s in stats if s["name"] == "wins"),   None)
            losses = next((s["value"] for s in stats if s["name"] == "losses"), None)
            if wins is not None and losses is not None:
                total = wins + losses
                if total > 0:
                    form[name] = round(wins / total, 3)

        cache_set(cache_key, form)
        print(f"  Team form {sport}: {len(form)} teams loaded")
        _team_form_cache[sport] = form
        return form

    except Exception as e:
        print(f"  Team form error {sport}: {e}")
        _team_form_cache[sport] = {}
        return {}

def get_form_signal(team, sport, team_form):
    """
    Convert win% to a 0-1 signal.
    .550+ = strong team = 0.7-0.85
    .500  = average     = 0.5
    .400  = weak team   = 0.3
    .300- = terrible    = 0.15

    Fuzzy-matches team name by nickname.
    """
    if not team_form:
        return 0.5

    team_nickname = team.lower().split()[-1]
    win_pct       = None

    for form_name, pct in team_form.items():
        if team_nickname in form_name.lower():
            win_pct = pct
            break

    if win_pct is None:
        return 0.5  # unknown team = neutral

    # scale win% to signal
    # centered at .500 = 0.5 signal
    if win_pct >= 0.600:   signal = 0.85
    elif win_pct >= 0.550: signal = 0.72
    elif win_pct >= 0.500: signal = 0.58
    elif win_pct >= 0.450: signal = 0.45
    elif win_pct >= 0.400: signal = 0.32
    else:                  signal = 0.18  # bad team — strong penalty

    return signal


# ── Book disagreement signal ─────────────────────────────────────────
# When books disagree on a line, the market is inefficient — there is
# value to be found. High disagreement = higher signal for the underdog
# (books protecting themselves from liabilities they disagree on).

def get_book_disagree_signal(juice_list):
    """
    Measure standard deviation of odds across books.
    High std dev = books disagree = market inefficiency.
    Returns 0-1 signal. 0.5 = neutral (books agree).
    """
    if len(juice_list) < 3:
        return 0.5  # not enough books to measure disagreement

    # convert to implied probability first — avoids American odds scale distortion
    def imp(odds):
        odds = int(odds)
        if odds > 0: return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)

    probs = [imp(j) for j in juice_list]
    mean  = sum(probs) / len(probs)
    variance = sum((p - mean) ** 2 for p in probs) / len(probs)
    std_dev = variance ** 0.5

    # std dev on implied probs: 0.01 = tight, 0.03 = notable, 0.05+ = big disagreement
    if std_dev >= 0.05:   return 0.85
    elif std_dev >= 0.03: return 0.70
    elif std_dev >= 0.02: return 0.58
    elif std_dev >= 0.01: return 0.52
    else:                 return 0.45  # books agree


# ── Rest days signal ──────────────────────────────────────────────────
# Teams on short rest perform worse against the spread.
# NBA back-to-backs are particularly strong fade signals.
# Uses ESPN scoreboard to find last game date.

_rest_cache = {}

def get_rest_days(team, sport):
    """
    Fetch days of rest for a team since their last game.
    Walks back day by day (up to 7 days) until it finds the team.
    Each day's scoreboard is cached separately.
    """
    cache_key = f"rest_{sport}_{team}"
    cached = cache_get(cache_key)
    if cached is not None:
        return cached

    ESPN_SPORT_MAP = {
        "basketball_nba":       "basketball/nba",
        "americanfootball_nfl": "football/nfl",
        "baseball_mlb":         "baseball/mlb",
        "icehockey_nhl":        "hockey/nhl",
    }
    sport_path = ESPN_SPORT_MAP.get(sport, "")
    if not sport_path:
        return None

    team_lower    = team.lower()
    team_nickname = team_lower.split()[-1]  # "angels", "dodgers", etc.
    today_dt      = datetime.now(timezone.utc).date()

    try:
        from datetime import timedelta, date as date_type

        for days_back in range(1, 8):
            check_date    = today_dt - timedelta(days=days_back)
            check_str     = check_date.strftime("%Y%m%d")
            day_cache_key = f"espn_sb_{sport}_{check_str}"

            day_events = cache_get(day_cache_key)
            if day_events is None:
                resp = requests.get(
                    f"https://site.api.espn.com/apis/site/v2/sports/{sport_path}/scoreboard",
                    params={"dates": check_str},
                    timeout=8,
                )
                if resp.status_code != 200:
                    continue
                day_events = resp.json().get("events", [])
                cache_set(day_cache_key, day_events)

            # look for completed games with this team
            for event in day_events:
                status = event.get("status", {}).get("type", {}).get("name", "")
                if status not in ("STATUS_FINAL", "STATUS_FINAL_OT"):
                    continue
                comps = event.get("competitions", [{}])
                if not comps:
                    continue
                for competitor in comps[0].get("competitors", []):
                    name = competitor.get("team", {}).get("displayName", "").lower()
                    if team_nickname in name and len(team_nickname) > 3:
                        rest_days = days_back
                        print(f"  Rest {team}: last game {check_date} ({rest_days}d ago)")
                        cache_set(cache_key, rest_days)
                        return rest_days

        # no game found in last 7 days
        cache_set(cache_key, None)
        return None

    except Exception as e:
        return None

def get_rest_signal(team, sport):
    """
    Convert rest days to a 0-1 signal.
    Short rest (0-1 days) = bad for team = low signal
    Good rest (3-6 days)  = good for team = high signal
    Long layoff (7+ days) = neutral (rust factor)
    """
    rest = get_rest_days(team, sport)
    if rest is None:
        return 0.5  # unknown = neutral

    if rest == 0:    return 0.1   # back-to-back — strong fade
    elif rest == 1:  return 0.3   # short rest
    elif rest == 2:  return 0.5   # neutral
    elif rest <= 5:  return 0.75  # well rested
    elif rest <= 7:  return 0.65  # good rest
    else:            return 0.5   # long layoff — rust possible


# ── CLV proxy signal ──────────────────────────────────────────────────
# Closing Line Value (CLV) measures if your line has moved in your favor
# since opening. We approximate it by comparing the first snapshot of the
# day to the most recent snapshot — no new API needed.
# Positive CLV = the market has validated our pick.

def get_clv_signal(game_id, team, current_juice, opening_lines):
    """
    Compare opening line to current line.
    If the line has moved IN YOUR FAVOR since opening, that's CLV.
    Different from line_movement signal which rewards any movement —
    CLV specifically measures if the movement validates the pick direction.
    Returns 0-1 signal.
    """
    key = (game_id, team)
    if key not in opening_lines:
        return 0.5  # no history yet

    def implied(odds):
        odds = int(odds)
        if odds == 0: return 0.5
        if odds > 0:  return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)

    opening_prob = implied(opening_lines[key])
    current_prob = implied(current_juice)

    # movement toward this team = positive CLV
    clv = current_prob - opening_prob

    # normalize: >3% movement is strong CLV
    if clv >= 0.06:    return 0.95
    elif clv >= 0.04:  return 0.82
    elif clv >= 0.02:  return 0.68
    elif clv >= 0.00:  return 0.50  # no movement = truly neutral
    elif clv >= -0.02: return 0.42
    elif clv >= -0.04: return 0.28
    else:              return 0.15  # strong movement against us


# ── Totals (over/under) scoring ──────────────────────────────────────

def score_totals(game, opening_lines, weather_cache):
    """
    Score over/under picks for a game.
    Returns a list of pick candidates (over and under).
    Signals most relevant: weather, CLV, book disagreement.
    """
    picks      = []
    bookmakers = game.get("bookmakers", [])
    if not bookmakers:
        return picks

    sport   = game["sport_key"]
    game_id = game["id"]

    overs  = []   # list of (total_line, juice) for over
    unders = []   # list of (total_line, juice) for under

    for book in bookmakers:
        for market in book.get("markets", []):
            if market["key"] != "totals":
                continue
            for outcome in market["outcomes"]:
                name  = outcome["name"]
                price = int(outcome.get("price", -110))
                point = outcome.get("point", 0)
                if -600 <= price <= 600 and price != 0 and point > 0:
                    if name.lower() == "over":
                        overs.append((point, price))
                    elif name.lower() == "under":
                        unders.append((point, price))

    if not overs or not unders:
        return picks

    avg_over_line  = safe_avg([o[0] for o in overs])
    avg_over_juice = safe_avg([o[1] for o in overs])
    avg_under_juice = safe_avg([u[1] for u in unders])

    if avg_over_juice == 0 or avg_under_juice == 0:
        return picks
    if -99 < avg_over_juice < 99 or -99 < avg_under_juice < 99:
        return picks

    # get weather signal for this game
    weather_key = game_id
    if weather_key not in weather_cache:
        weather_cache[weather_key] = get_weather_signal(game["home_team"], sport)
    weather_sig = weather_cache[weather_key]

    for side in ["over", "under"]:
        avg_juice = avg_over_juice if side == "over" else avg_under_juice

        # weather hurts scoring — boosts under, hurts over
        if side == "under":
            weather_signal = weather_sig           # bad weather = good for under
        else:
            weather_signal = 1.0 - weather_sig    # bad weather = bad for over

        # juice value signal
        # juice value = vig efficiency, symmetric for fav and dog
        # scaled tightly so it acts as a tiebreaker not a driver
        # -110 = 0.65, -130 = 0.55, -150 = 0.50, -180 = 0.42, +170 = 0.52
        abs_odds = abs(avg_juice) if avg_juice != 0 else 110
        juice_signal = max(0.25, min(0.75, 0.75 - (abs_odds - 100) / 600))

        # book disagreement on the total line
        all_juices = [o[1] for o in overs] if side == "over" else [u[1] for u in unders]
        book_disagree_signal = get_book_disagree_signal(all_juices)

        # CLV for totals — compare opening total line to current
        opening_key = (game_id, side)
        clv_signal  = 0.5  # neutral default for totals

        # fade public on totals — public loves overs
        # if over juice is heavy (public pounding it), favor under
        fade_signal = 0.0
        if side == "under" and avg_over_juice < -120:
            fade_signal = min((abs(avg_over_juice) - 120) / 150, 1.0)
        elif side == "over" and avg_under_juice < -120:
            fade_signal = min((abs(avg_under_juice) - 120) / 150, 1.0)

        raw = (
            weather_signal       * 0.30 +
            juice_signal         * 0.20 +
            book_disagree_signal * 0.20 +
            fade_signal          * 0.20 +
            clv_signal           * 0.10
        )

        confidence = round(48 + raw * 20, 1)
        ev         = expected_value(confidence, avg_juice)

        if ev <= 0:
            continue

        label = f"{'Over' if side == 'over' else 'Under'} {avg_over_line:.1f}"

        picks.append({
            "sport":      sport,
            "home_team":  game["home_team"],
            "away_team":  game["away_team"],
            "pick_team":  label,
            "pick_line":  label,
            "odds":       int(avg_juice),
            "confidence": confidence,
            "ev":         ev,
            "game_time":  game["commence_time"],
            "pick_type":  "total",
            "signals": {
                "line_movement": 0,
                "fade_public":   round(fade_signal, 3),
                "juice_value":   round(juice_signal, 3),
                "weather":       round(weather_signal, 3),
                "prop_signal":   0,
                "injury":        0,
                "book_disagree": round(book_disagree_signal, 3),
                "rest_days":     0,
                "clv":           round(clv_signal, 3),
            },
            "num_books":  len(bookmakers),
        })

    return picks


# ── line movement from snapshots ─────────────────────────────────────

def get_opening_lines(today):
    rows = sb_select("odds_snapshots", {
        "snap_date": f"eq.{today}",
        "order":     "snapshot_time.asc",
        "select":    "game_id,team,avg_juice,snapshot_time",
    })
    opening = {}
    for row in rows:
        key = (row["game_id"], row["team"])
        if key not in opening:
            opening[key] = row["avg_juice"]
    return opening

def real_line_movement_signal(game_id, team, current_juice, opening_lines):
    key = (game_id, team)
    if key not in opening_lines:
        return 0.5  # neutral when no snapshot history yet

    def implied(odds):
        odds = int(odds)
        if odds == 0: return 0.5
        if odds > 0:  return 100 / (odds + 100)
        return abs(odds) / (abs(odds) + 100)

    movement = implied(current_juice) - implied(opening_lines[key])

    if movement <= 0:    return 0.0
    elif movement < 0.02: return 0.2
    elif movement < 0.05: return 0.5
    elif movement < 0.10: return 0.8
    else:                 return 1.0

# ── scoring helpers ───────────────────────────────────────────────────

def safe_avg(values):
    return sum(values) / len(values) if values else 0

def american_to_implied_prob(odds):
    odds = int(odds)
    if odds == 0: return 0.5
    if odds > 0:  return 100 / (odds + 100)
    return abs(odds) / (abs(odds) + 100)

def expected_value(confidence, odds):
    odds = int(odds)
    if odds == 0: return 0
    implied_prob = american_to_implied_prob(odds) * 100
    return round(confidence - implied_prob, 2)

def score_game(game, opening_lines, weather_cache, props_cache, injuries, rest_cache, today="", team_form=None):
    picks      = []
    bookmakers = game.get("bookmakers", [])
    if not bookmakers:
        return picks

    sport   = game["sport_key"]
    game_id = game["id"]
    markets = "h2h" if sport in RUN_LINE_SPORTS else "spreads,h2h"

    spreads    = {}
    juices     = {}
    best_price = {}   # team -> best (most favorable) odds across all books

    for book in bookmakers:
        for market in book.get("markets", []):
            mkey = market["key"]
            if mkey not in ("spreads", "h2h"):
                continue
            for outcome in market["outcomes"]:
                name  = outcome["name"]
                price = int(outcome.get("price", -110))
                point = outcome.get("point", 0)
                if -600 <= price <= 600 and price != 0:
                    juices.setdefault(name, []).append(price)
                    cur_best = best_price.get(name, -9999)
                    if price > cur_best:
                        best_price[name] = price
                if mkey == "spreads":
                    spreads.setdefault(name, []).append(point)

    if not juices:
        return picks

    teams = list(juices.keys())
    if len(teams) < 2:
        return picks

    # get weather and props once per game
    weather_key = game_id
    if weather_key not in weather_cache:
        weather_cache[weather_key] = get_weather_signal(game["home_team"], sport)
    weather_signal = weather_cache[weather_key]

    props_key = game_id
    if props_key not in props_cache:
        props_cache[props_key] = get_props_signal(sport, game_id)
    prop_signal = props_cache[props_key]

    for team in teams:
        avg_juice   = safe_avg(juices.get(team, []))
        avg_spread  = safe_avg(spreads.get(team, []))
        other_team  = [t for t in teams if t != team][0]
        other_juice = safe_avg(juices.get(other_team, []))

        if avg_juice == 0 or other_juice == 0:
            continue
        if -99 < avg_juice < 99:
            continue

        # signals
        line_move_signal = real_line_movement_signal(game_id, team, avg_juice, opening_lines)

        fade_signal = 0.0
        if other_juice < -200:
            fade_signal = min((abs(other_juice) - 200) / 250, 0.6)  # only very heavy chalk

        # juice value = vig efficiency, symmetric for fav and dog
        # scaled tightly so it acts as a tiebreaker not a driver
        # -110 = 0.65, -130 = 0.55, -150 = 0.50, -180 = 0.42, +170 = 0.52
        abs_odds = abs(avg_juice) if avg_juice != 0 else 110
        juice_signal = max(0.25, min(0.75, 0.75 - (abs_odds - 100) / 600))

        # injury signal — lower if key players out for this team
        injury_signal = get_injury_signal(team, sport, injuries)

        # team form — penalize bad teams, reward good teams
        form_signal = get_form_signal(team, sport, team_form or {})

        # book disagreement — high std dev across books = market inefficiency
        book_disagree_signal = get_book_disagree_signal(juices.get(team, []))

        # rest days — short rest hurts, good rest helps
        rest_cache_key = (team, sport)
        if rest_cache_key not in rest_cache:
            rest_cache[rest_cache_key] = get_rest_signal(team, sport)
        rest_signal = rest_cache[rest_cache_key]

        # CLV proxy — has the market moved to validate our pick?
        clv_signal = get_clv_signal(game["id"], team, avg_juice, opening_lines)

        # no-vig edge — true probability gap
        team_best  = best_price.get(team, avg_juice)
        no_vig_signal = get_no_vig_edge(juices.get(team, []), team_best)

        # reverse line movement — sharp vs public divergence
        reverse_signal = get_reverse_line_signal(
            game_id, team, other_team, avg_juice, other_juice, opening_lines
        )

        # steam move — coordinated sharp action
        steam_signal = get_steam_signal(game_id, team, today)

        # weather boosts underdogs and low-scoring picks
        # if weather is bad (high signal) it helps the underdog (positive odds)
        team_weather = weather_signal if avg_juice > 0 else (1 - weather_signal)

        raw = (
            line_move_signal     * WEIGHTS["line_movement"] +
            fade_signal          * WEIGHTS["fade_public"]   +
            juice_signal         * WEIGHTS["juice_value"]   +
            team_weather         * WEIGHTS["weather"]       +
            prop_signal          * WEIGHTS["prop_signal"]   +
            injury_signal        * WEIGHTS["injury"]        +
            book_disagree_signal * WEIGHTS["book_disagree"] +
            rest_signal          * WEIGHTS["rest_days"]     +
            clv_signal           * WEIGHTS["clv"]           +
            no_vig_signal        * WEIGHTS["no_vig_edge"]   +
            reverse_signal       * WEIGHTS["reverse_line"]  +
            steam_signal         * WEIGHTS["steam_move"]    +
            form_signal          * WEIGHTS["team_form"]
        )
        confidence = round(48 + raw * 20, 1)
        ev         = expected_value(confidence, avg_juice)

        # scaled minimum EV threshold — bigger underdogs need more edge
        # this prevents the model from recommending +300 dogs just because vig is low
        # favorites need less edge since the implied probability is already high
        abs_odds = abs(avg_juice)
        if avg_juice >= 0:
            # underdog — require more EV as odds get longer
            # +100 needs EV > 1, +200 needs EV > 3, +300 needs EV > 5
            min_ev = 1.0 + (avg_juice / 100.0)
        else:
            # favorite — standard positive EV is enough
            min_ev = 0.5

        if ev < min_ev:
            continue

        picks.append({
            "sport":      sport,
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
                "weather":       round(team_weather, 3),
                "prop_signal":   round(prop_signal, 3),
                "injury":        round(injury_signal, 3),
                "book_disagree": round(book_disagree_signal, 3),
                "rest_days":     round(rest_signal, 3),
                "clv":           round(clv_signal, 3),
                "no_vig_edge":   round(no_vig_signal, 3),
                "reverse_line":  round(reverse_signal, 3),
                "steam_move":    round(steam_signal, 3),
                "team_form":     round(form_signal, 3),
            },
            "num_books":  len(bookmakers),
        })

    return picks

# ── main ──────────────────────────────────────────────────────────────

def fetch_and_score():
    today         = datetime.now(timezone.utc).date().isoformat()
    weather_cache = {}
    props_cache   = {}
    injury_cache  = {}   # filled per-sport below
    rest_cache    = {}   # (team, sport) -> rest signal

    # ── determine run mode based on ET time ──────────────────
    # ET = UTC - 4 (EDT) / UTC - 5 (EST). Use UTC-4 as safe default.
    from datetime import timedelta as _tdt
    et_hour = (datetime.now(timezone.utc) - _tdt(hours=4)).hour

    if et_hour < 12:        # before noon ET → 8 AM run
        run_mode     = "morning"
        window_start = datetime.now(timezone.utc)
        window_end   = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)
        window_label = "today's upcoming games only"
    elif et_hour < 20:      # noon–8 PM ET → 3 PM run
        run_mode     = "afternoon"
        window_start = datetime.now(timezone.utc)
        window_end   = datetime.now(timezone.utc).replace(hour=23, minute=59, second=59)
        window_label = "today's remaining games"
    else:                   # after 8 PM ET → 8 PM run
        run_mode     = "evening"
        # use ET date as base — at midnight UTC it's still "yesterday" in ET
        et_now       = datetime.now(timezone.utc) - _tdt(hours=4)
        tomorrow     = (et_now + _tdt(days=1)).date()
        window_start = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 0, 0, 0, tzinfo=timezone.utc)
        window_end   = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 23, 59, 59, tzinfo=timezone.utc)
        window_label = "tomorrow's full slate"

    print(f"Run mode: {run_mode} | Window: {window_label}")

    # clear odds cache so game IDs match snapshot table
    # props, ESPN, rest caches are kept since they don't change intraday
    import glob
    cleared = 0
    for pattern in [f"odds_*_{today}.json", f"totals_*_{today}.json"]:
        for f in glob.glob(str(CACHE_DIR / pattern)):
            try:
                Path(f).unlink()
                cleared += 1
            except Exception:
                pass
    print(f"  Cleared {cleared} odds cache files for fresh game IDs")

    print("Loading opening lines from snapshots...")
    opening_lines = get_opening_lines(today)
    print(f"  {len(opening_lines)} opening line records found")
    if not opening_lines:
        print("  (No snapshots yet — line movement will use neutral baseline)\n")

    all_picks = []

    for sport in SPORTS:
        print(f"\nFetching {sport}...")
        markets = "h2h" if sport in RUN_LINE_SPORTS else "spreads,h2h"
        try:
            cache_key = f"odds_{sport}_{today}"
            games = cached_get(
                f"https://api.the-odds-api.com/v4/sports/{sport}/odds/",
                params={
                    "apiKey":     ODDS_API_KEY,
                    "regions":    "us",
                    "markets":    markets,
                    "oddsFormat": "american",
                },
                cache_key=cache_key,
            )
            if games is None:
                print(f"  Error fetching odds for {sport}")
                continue

            # also fetch totals market separately
            totals_cache_key = f"totals_{sport}_{today}"
            totals_games = cached_get(
                f"https://api.the-odds-api.com/v4/sports/{sport}/odds/",
                params={
                    "apiKey":     ODDS_API_KEY,
                    "regions":    "us",
                    "markets":    "totals",
                    "oddsFormat": "american",
                },
                cache_key=totals_cache_key,
            )
        except requests.exceptions.RequestException as e:
            print(f"  Network error: {e}")
            continue
        print(f"  {len(games)} games found")

        # fetch ESPN injuries and team form for this sport
        injuries  = get_espn_injuries(sport)
        team_form = get_team_form(sport)

        # ── three-run schedule (all times ET = UTC-4) ──────────────
        # RUN_MODE is set once per session at top of fetch_and_score()
        # 8 AM run:  today's games not yet started
        # 3 PM run:  today's games not yet started (replaces in-progress picks)
        # 8 PM run:  tomorrow's full slate only

        from datetime import timedelta as _td

        def parse_game_time(game):
            ct = game.get("commence_time", "")
            if not ct: return None
            try:
                return datetime.fromisoformat(ct.replace("Z", "+00:00"))
            except Exception:
                return None

        def in_window(game):
            gt = parse_game_time(game)
            if gt is None: return False
            return window_start <= gt <= window_end

        games_filtered = [g for g in games if in_window(g)]
        print(f"  {len(games_filtered)}/{len(games)} games in window ({window_label})")
        games = games_filtered

        # build totals lookup by game_id
        totals_by_id = {}
        if totals_games:
            for tg in totals_games:
                if in_window(tg):
                    totals_by_id[tg["id"]] = tg

        for game in games:
            all_picks.extend(score_game(game, opening_lines, weather_cache, props_cache, injuries, rest_cache, today, team_form))
            # score totals if available for this game
            if game["id"] in totals_by_id:
                all_picks.extend(score_totals(totals_by_id[game["id"]], opening_lines, weather_cache))

    if not all_picks:
        print("\nNo picks with positive EV today.")
        return

    all_picks.sort(key=lambda p: p["ev"], reverse=True)
    top_picks = all_picks[:MAX_PICKS]

    # pick_date and merge strategy depend on run mode
    if run_mode == "evening":
        from datetime import timedelta as _td2
        # use ET date base so midnight UTC doesn't add an extra day
        et_now_for_date = datetime.now(timezone.utc) - _td2(hours=4)
        pick_date = (et_now_for_date + _td2(days=1)).date().isoformat()
    else:
        pick_date = today

    print(f"\nWriting top {len(top_picks)} picks to Supabase for {pick_date} [{run_mode} run]...")

    # read existing picks — store prev_confidence AND keep graded picks
    existing = sb_select("picks", {
        "pick_date": f"eq.{pick_date}",
        "select":    "id,pick_line,confidence,result,rank,game_time",
    })
    prev_conf = {row["pick_line"]: row["confidence"] for row in existing if row.get("pick_line")}

    if run_mode == "afternoon":
        # 3 PM run: keep graded picks AND in-progress games, replace only future ungraded picks
        now_utc = datetime.now(timezone.utc)

        def game_has_started(row):
            """Return True if this pick's game has already started or finished."""
            gt = row.get("game_time", "")
            if not gt: return False
            try:
                game_dt = datetime.fromisoformat(gt.replace("Z", "+00:00"))
                return game_dt <= now_utc
            except Exception:
                return False

        # keep: graded picks + in-progress games (started but no result yet)
        # delete: ungraded picks whose game hasn't started yet
        keep_rows   = []
        delete_rows = []
        for row in existing:
            if row.get("result") is not None:
                keep_rows.append(row)    # already graded — always keep
            elif game_has_started(row):
                keep_rows.append(row)    # game in progress — keep on board
            else:
                delete_rows.append(row)  # future game, ungraded — replace

        for row in delete_rows:
            sb_delete("picks", {"id": f"eq.{row['id']}"})

        slots_available = MAX_PICKS - len(keep_rows)
        keep_lines      = {r["pick_line"] for r in keep_rows}
        new_top         = [p for p in top_picks if p["pick_line"] not in keep_lines]
        top_picks       = new_top[:max(0, slots_available)]

        in_progress = [r for r in keep_rows if r.get("result") is None]
        graded      = [r for r in keep_rows if r.get("result") is not None]
        print(f"  Kept: {len(graded)} graded, {len(in_progress)} in-progress | Adding {len(top_picks)} new picks")
    else:
        # 8 AM and 8 PM runs: full replace
        sb_delete("picks", {"pick_date": f"eq.{pick_date}"})

    for rank, pick in enumerate(top_picks, 1):
        prev = prev_conf.get(pick["pick_line"])
        record = {
            **pick,
            "pick_date":       pick_date,
            "rank":            rank,
            "result":          None,
            "prev_confidence": prev,  # None on first run, float on rescore
        }
        sb_insert("picks", record)

        sigs   = pick["signals"]
        driver = max(sigs, key=sigs.get)
        labels = {
            "line_movement": "sharp line move",
            "fade_public":   "fading public",
            "juice_value":   "juice value",
            "weather":       "weather angle",
            "prop_signal":   "prop signal",
            "injury":        "injury edge",
            "book_disagree": "book disagreement",
            "rest_days":     "rest advantage",
            "clv":           "closing line value",
            "no_vig_edge":   "no-vig edge",
            "reverse_line":  "reverse line move",
            "steam_move":    "steam move detected",
            "team_form":     "team quality",
        }
        print(f"  #{rank}  {pick['pick_line']:<35} conf {pick['confidence']}%  EV {pick['ev']:+.1f}  [{labels.get(driver, driver)}]")

    print(f"\nDone!")

if __name__ == "__main__":
    fetch_and_score()
