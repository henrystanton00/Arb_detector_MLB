#!/usr/bin/env python3
"""
MARKET GAP — prediction-market discrepancy & arb screener (v0.1)
================================================================
Compares prices for the same game across:
  1. Kalshi        (direct public API, real-time, free)
  2. Polymarket    (direct public Gamma API, real-time, free)
  3. Sportsbooks   (SportsGameOdds aggregator -> de-vigged consensus benchmark
                    from FanDuel, DraftKings, BetMGM, Caesars, ESPN BET, etc.)

Outputs snapshot.json (for the website) and a console report.

USAGE
-----
  python pipeline.py                    # full run (needs internet; SGO optional)
  python pipeline.py --no-sgo           # skip sportsbook benchmark (no key needed)
  python pipeline.py --selftest         # run offline logic tests on fixture data
  python pipeline.py --debug            # also dump raw API samples to ./debug/

  SGO key: set env var SGO_API_KEY (PowerShell:  $env:SGO_API_KEY="yourkey")
  NEVER hardcode the key in this file or commit it to a repo.

DESIGN NOTES
------------
- Stdlib only (urllib, json) so it runs anywhere with Python 3.9+, no pip installs.
- Matching is deterministic: (league, {team A, team B}, game date). No fuzzy
  string matching, so no false arbs from mismatched markets.
- All arb math uses ASK prices (what you actually pay) plus Kalshi's exact
  published taker-fee formula: ceil-to-cent(0.07 * P * (1-P)) per contract.
- Polymarket fees are read from the API's own feesEnabled/feeType fields.
- Everything is a "flag to verify manually", not a trade instruction.
"""

import argparse
import json
import math
import os
import re
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta

# ----------------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------------

KALSHI_BASE = "https://external-api.kalshi.com/trade-api/v2"
GAMMA_BASE = "https://gamma-api.polymarket.com"
SGO_BASE = "https://api.sportsgameodds.com/v2"

# MLB only for now. NFL support was removed 2026-07-20 to keep payloads and
# parsing tight during the build phase; restore it from git history (or ask
# Claude) when the season approaches.
LEAGUES = ["MLB"]

# Only screen games from yesterday(safety margin) through N days ahead.
# Cuts API payloads and SGO object usage; distant games have no liquidity
# anyway.
MAX_DAYS_AHEAD = 3

# Kalshi series ticker prefixes. Kalshi tickers are structured, e.g.
# KXMLBGAME-26JUL20SEACIN. If Kalshi renames a series, update here.
KALSHI_SERIES_PREFIXES = {
    "MLB": ["KXMLBGAME", "KXMLB"],
}

# Kalshi taker fee multiplier (general schedule, July 2026). Some series carry
# different multipliers; keep configurable.
KALSHI_FEE_MULTIPLIER = 0.07

# How many top-volume Polymarket events to scan per page / how many pages.
GAMMA_PAGE_SIZE = 100
GAMMA_MAX_PAGES = 5

# Minimum 24h volume (USD) for a market to be worth showing at all.
MIN_VOLUME_24H = 25.0

# Team nickname -> canonical code, scoped by league (avoids Giants/Giants,
# Cardinals/Cardinals cross-league collisions).
TEAMS = {
    "MLB": {
        "diamondbacks": "ARI", "dbacks": "ARI", "braves": "ATL", "orioles": "BAL",
        "red sox": "BOS", "cubs": "CHC", "white sox": "CWS", "reds": "CIN",
        "guardians": "CLE", "rockies": "COL", "tigers": "DET", "astros": "HOU",
        "royals": "KC", "angels": "LAA", "dodgers": "LAD", "marlins": "MIA",
        "brewers": "MIL", "twins": "MIN", "mets": "NYM", "yankees": "NYY",
        "athletics": "ATH", "a's": "ATH", "phillies": "PHI", "pirates": "PIT",
        "padres": "SD", "giants": "SFG", "mariners": "SEA", "cardinals": "STL",
        "rays": "TB", "rangers": "TEX", "blue jays": "TOR", "nationals": "WSH",
    },
}

# City-name aliases (only cities that are unambiguous WITHIN their league;
# "new york"/"chicago"/"los angeles" stay out on purpose).
# Kalshi disambiguates shared-city teams in yes_sub_title with a trailing
# letter/abbreviation: "Chicago C" (Cubs), "Chicago WS" (White Sox),
# "Los Angeles D" (Dodgers), "Los Angeles A" (Angels), "New York M" (Mets),
# "New York Y" (Yankees). This is Kalshi telling us the exact side, not a
# guess — safe to match exactly, unlike bare "Chicago"/"New York"/"Los
# Angeles" which stay unaliased in CITY_ALIASES on purpose.
KALSHI_AMBIGUOUS_SUBTITLES = {
    "MLB": {
        "chicago c": "CHC", "chicago cubs": "CHC",
        "chicago ws": "CWS", "chicago white sox": "CWS",
        "los angeles d": "LAD", "los angeles dodgers": "LAD",
        "los angeles a": "LAA", "los angeles angels": "LAA",
        "new york m": "NYM", "new york mets": "NYM",
        "new york y": "NYY", "new york yankees": "NYY",
    },
}

CITY_ALIASES = {
    "MLB": {
        "arizona": "ARI", "atlanta": "ATL", "baltimore": "BAL",
        "boston": "BOS", "cincinnati": "CIN", "cleveland": "CLE",
        "colorado": "COL", "detroit": "DET", "houston": "HOU",
        "kansas city": "KC", "milwaukee": "MIL", "minnesota": "MIN",
        "oakland": "ATH", "sacramento": "ATH", "philadelphia": "PHI",
        "pittsburgh": "PIT", "san diego": "SD", "san francisco": "SFG",
        "seattle": "SEA", "st. louis": "STL", "st louis": "STL",
        "tampa bay": "TB", "texas": "TEX", "toronto": "TOR",
        "washington": "WSH", "miami": "MIA",
        # Deliberately NOT aliased: "new york" (Mets/Yankees), "chicago"
        # (Cubs/White Sox), "los angeles" (Dodgers/Angels) — each is two
        # teams, and guessing which one would risk another side-inversion
        # bug. These rely on the ticker parser (which is unambiguous) or a
        # nickname elsewhere in the text.
    },
}
for _lg, _aliases in CITY_ALIASES.items():
    TEAMS[_lg].update(_aliases)

# Team-code aliases for parsing Kalshi event tickers (KXMLBGAME-26JUL20SEACIN)
# and short subtitles. Maps every plausible code spelling -> our canonical code.


def _build_code_aliases():
    out = {}
    extras = {
        "MLB": {"CHW": "CWS", "CHA": "CWS", "CHN": "CHC", "OAK": "ATH",
                "SAC": "ATH", "WAS": "WSH", "WSN": "WSH", "SF": "SFG",
                "SFO": "SFG", "TBR": "TB", "KCR": "KC", "SDP": "SD",
                "ANA": "LAA", "NYA": "NYY", "NYN": "NYM", "SLN": "STL",
                "LAN": "LAD", "AZ": "ARI"},
    }
    for lg in TEAMS:
        base = {code: code for code in set(TEAMS[lg].values())}
        base.update(extras.get(lg, {}))
        out[lg] = base
    return out


CODE_ALIASES = _build_code_aliases()

# Words that mark a question as NOT a plain moneyline (spreads, totals, props,
# series prices, etc.). We only screen moneylines in v1 — resolution rules for
# anything else differ across venues too easily.
ML_BLOCKLIST = (
    "by more", "by over", "by at least", "by fewer", " runs", " points",
    " goals", "over/under", " over ", " under ", "combined", "series",
    "first ", "margin", "spread", " total", " hit ", "home run", "strikeout",
    " score ", "both ", "either ", "inning", "quarter", " half", "shutout",
    "no-hitter", "walk-off", "extra inning", "grand slam", " rbi",
    "touchdown", " yards", "passing", "rushing", " lead ", "postseason",
    "playoffs", "division", "pennant", "world series", "super bowl", "mvp",
)

# Quote sanity: skip near-resolved / live-blowout / illiquid quotes.
PRICE_MIN, PRICE_MAX, MAX_SPREAD = 0.03, 0.97, 0.20

# ALLOWLIST: a Polymarket binary question is a moneyline ONLY if it matches
# one of these shapes. Everything else is ignored, full stop.
ML_QUESTION_PATTERNS = (
    re.compile(r"^will (the )?.+ (beat|defeat) (the )?.+\?$"),
    re.compile(r"^will (the )?.+ win (against|vs\.?|at) (the )?.+\?$"),
    re.compile(r"^will (the )?.+ win (their|its) .+ (game|matchup)( .+)?\?$"),
    re.compile(r"^.+ vs\.? .+: .+ (to win|winner)\??$"),
    # date-anchored / bare winner questions: 'Will the Mariners win?',
    # 'Will the Mariners win on July 21?', '... win today/tonight?'
    re.compile(r"^will (the )?.+ win\?$"),
    re.compile(r"^will (the )?.+ win (today|tonight)\?$"),
    re.compile(r"^will (the )?.+ win on [a-z0-9 ,/-]+\?$"),
)
# Game-event slugs look like 'mlb-sea-cin-2026-07-20'; futures/props don't.
GAME_SLUG_RE = re.compile(r"^(mlb)-[a-z0-9]{2,4}-[a-z0-9]{2,4}-\d{4}-\d{2}-\d{2}")

# Any edge vs. book consensus above this is almost certainly a bad match or a
# stale/live quote, not free money. Quarantined into snapshot['suspect'], never
# shown in the main table. Real MLB moneyline gaps vs consensus top out ~8pts.
MAX_PLAUSIBLE_EDGE = 0.12

# Cross-venue arbs above this profit are treated as data errors (side
# inversion, stale book) rather than free money — real surviving arbs are
# small. They land in snapshot['arbs_suspect'], not the headline list.
ARB_MAX_CREDIBLE = 0.05

# Date-matching window per league. NFL teams play weekly -> folding date±1 is
# safe. MLB teams play each other 3-4 NIGHTS IN A ROW -> folding would merge
# different games of the same series into fake arbs. Exact ET date only.
MATCH_WINDOW = {"MLB": 0}

# US Eastern for game-date normalization (sports schedules live in ET).
try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:  # noqa: BLE001 — Windows without tzdata: fixed EDT offset
    ET = timezone(timedelta(hours=-4))

# SGO uses full team name strings; map common SGO/standard abbreviations to ours
SGO_LEAGUE_IDS = {"MLB": "MLB"}


# ----------------------------------------------------------------------------
# HTTP helper (stdlib only)
# ----------------------------------------------------------------------------

def http_get_json(url, headers=None, timeout=30):
    req = urllib.request.Request(url, headers=headers or {})
    req.add_header("User-Agent", "MarketGapScreener/0.1 (personal project)")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))


# ----------------------------------------------------------------------------
# FEE MODELS
# ----------------------------------------------------------------------------

def kalshi_taker_fee(price_dollars, multiplier=KALSHI_FEE_MULTIPLIER):
    """Kalshi general taker fee per contract, per official fee schedule:
    fees = round_up(M * C * P * (1-P)); we compute per-contract (C=1),
    rounded UP to the next cent — worst case, which is what we want."""
    raw = multiplier * price_dollars * (1.0 - price_dollars)
    return math.ceil(raw * 100.0) / 100.0


def polymarket_fee(price_dollars, fees_enabled, fee_rate=None):
    """Polymarket fees, per market. Sports markets now carry an explicit
    feeSchedule (sports_fees_v2: taker fee = rate * min(P, 1-P) per share,
    e.g. 5% of the cheaper side). Non-fee markets return 0. If fees are
    enabled but no rate is visible, use a conservative 2%-of-price."""
    if not fees_enabled:
        return 0.0
    if fee_rate:
        return round(fee_rate * min(price_dollars, 1.0 - price_dollars), 4)
    return round(price_dollars * 0.02, 4)


def american_to_prob(odds):
    """American odds -> implied probability (with vig still included)."""
    odds = float(odds)
    if odds < 0:
        return -odds / (-odds + 100.0)
    return 100.0 / (odds + 100.0)


def devig_pair(p_home, p_away):
    """Remove vig by normalizing the two implied probabilities to sum to 1."""
    total = p_home + p_away
    if total <= 0:
        return None, None
    return p_home / total, p_away / total


# ----------------------------------------------------------------------------
# TITLE PARSING / MATCH KEYS
# ----------------------------------------------------------------------------

def find_teams_in_text(text, league):
    """Return canonical team codes IN ORDER OF APPEARANCE in the text.
    Order matters: in 'Will the Reds beat the Mariners?' the first team is
    the subject of the market."""
    # Periods are DROPPED (not space-replaced) so 'St. Louis' normalizes to
    # 'st louis' matching our alias key exactly; other punctuation becomes a
    # space, then runs of whitespace collapse to one. Without this, 'St.
    # Louis' -> 'st  louis' (double space) silently failed to match either
    # the 'st. louis' or 'st louis' alias.
    cleaned = text.lower().replace(".", "")
    cleaned = re.sub(r"[^a-z0-9' ]", " ", cleaned)
    text_l = " " + re.sub(r"\s+", " ", cleaned).strip() + " "
    first_pos = {}
    for name, code in TEAMS[league].items():
        name_clean = name.replace(".", "")
        idx = text_l.find(f" {name_clean} ")
        if idx >= 0 and (code not in first_pos or idx < first_pos[code]):
            first_pos[code] = idx
    return [code for code, _ in sorted(first_pos.items(), key=lambda kv: kv[1])]


def find_team_code_token(text, league):
    """Match short team CODES ('SEA', 'CIN') as standalone tokens. Only used
    on short fields like Kalshi's yes_sub_title, where false positives
    ('sea' inside 'season') can't occur."""
    for token in re.findall(r"[A-Za-z']+", text.upper()):
        if token in CODE_ALIASES[league]:
            return CODE_ALIASES[league][token]
    return None


def teams_from_kalshi_ticker(event_ticker, league):
    """Parse the two team codes off a Kalshi game ticker. Real format is
    DATE(YYMMMDD) + TIME(HHMM) + TEAMCODES, e.g.
    KXMLBGAME-26JUL231507TBTOR -> date 26JUL23, time 1507, teams TB+TOR.
    (An earlier version of this regex omitted the time segment, which
    silently broke ticker parsing on every market — home team and a chunk
    of team resolution rode on this.) Deterministic: tries every split of
    the trailing code blob against known codes."""
    m = re.search(r"-(?:\d{2}[A-Z]{3}\d{2})(?:\d{4})([A-Z]{4,8})$",
                  event_ticker or "")
    if not m:
        # fallback: older/other format without the time segment
        m = re.search(r"-(?:\d{2}[A-Z]{3}\d{2})([A-Z]{4,8})$", event_ticker or "")
    if not m:
        return []
    blob = m.group(1)
    aliases = CODE_ALIASES[league]
    for i in range(2, len(blob) - 1):
        a, b = blob[:i], blob[i:]
        if a in aliases and b in aliases and aliases[a] != aliases[b]:
            return [aliases[a], aliases[b]]
    return []


def game_key(league, team_a, team_b, date_iso):
    """Deterministic match key: league + unordered team pair + date."""
    pair = "|".join(sorted([team_a, team_b]))
    return f"{league}:{pair}:{date_iso}"


def date_only(dt_string):
    """Datetime string -> the GAME's date in US Eastern (sports schedule
    time). Rule: timestamps before 6am ET belong to the previous night's
    game (west-coast games end ~1:30am ET)."""
    dt = parse_dt(dt_string)
    if not dt:
        return None
    local = dt.astimezone(ET)
    if local.hour < 6:
        local -= timedelta(days=1)
    return local.strftime("%Y-%m-%d")


def parse_dt(dt_string):
    """Tolerant ISO-ish parser: handles 'Z', space separators, '+00'."""
    if not dt_string:
        return None
    s = str(dt_string).strip().replace("Z", "+00:00").replace(" ", "T", 1)
    if re.search(r"[+-]\d{2}$", s):
        s += ":00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError:
        m = re.match(r"(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2})", str(dt_string))
        if not m:
            return None
        dt = datetime.fromisoformat(f"{m.group(1)}T{m.group(2)}:{m.group(3)}:00+00:00")
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def has_started(dt_string):
    dt = parse_dt(dt_string)
    return bool(dt) and dt <= datetime.now(timezone.utc)


def within_horizon(date_iso):
    """True if the game date falls in [today-1, today+MAX_DAYS_AHEAD] (ET)."""
    if not date_iso:
        return False
    lo = (datetime.now(ET) - timedelta(days=1)).strftime("%Y-%m-%d")
    hi = (datetime.now(ET) + timedelta(days=MAX_DAYS_AHEAD)).strftime("%Y-%m-%d")
    return lo <= date_iso <= hi


def adjacent_dates(date_iso, window=1):
    d = datetime.strptime(date_iso, "%Y-%m-%d")
    return [
        (d + timedelta(days=off)).strftime("%Y-%m-%d")
        for off in range(-window, window + 1)
    ]


# ----------------------------------------------------------------------------
# ADAPTER 1: KALSHI
# ----------------------------------------------------------------------------

def fetch_kalshi(debug=False):
    """Yield normalized records:
    {source, league, team (whose YES side), opponent, date, yes_bid, yes_ask,
     fee_at_ask, volume_24h, title, url}"""
    records, raw_sample = [], None
    KALSHI_REJECTS.clear()
    KALSHI_REJECT_SAMPLES.clear()
    for league, prefixes in KALSHI_SERIES_PREFIXES.items():
        if league not in LEAGUES:
            continue
        for prefix in prefixes:
            cursor = ""
            # ask Kalshi only for markets closing within the horizon
            max_close = int((datetime.now(timezone.utc)
                             + timedelta(days=MAX_DAYS_AHEAD + 2)).timestamp())
            while True:
                params = {"series_ticker": prefix, "status": "open",
                          "limit": 200, "max_close_ts": max_close}
                if cursor:
                    params["cursor"] = cursor
                url = f"{KALSHI_BASE}/markets?{urllib.parse.urlencode(params)}"
                try:
                    data = http_get_json(url)
                except Exception as e:  # noqa: BLE001 — log & continue
                    print(f"  [kalshi] {prefix}: {e}")
                    break
                markets = data.get("markets", [])
                if raw_sample is None and markets:
                    raw_sample = markets[0]
                for m in markets:
                    rec = _kalshi_market_to_record(m, league)
                    if rec and within_horizon(rec["date"]):
                        records.append(rec)
                    elif rec:
                        _kbump("outside_horizon")
                cursor = data.get("cursor") or ""
                if not cursor or not markets:
                    break
    if debug and raw_sample:
        _dump_debug("kalshi_sample.json", raw_sample)
    records, dropped, pair_samples = _kalshi_pair_sanity(records)
    if debug:
        if dropped:
            print(f"  [kalshi] pair-sanity dropped {dropped} records "
                  f"(ambiguous sides or prices not summing to ~$1)")
        print("  [kalshi] parse tally: " + json.dumps(
            dict(sorted(KALSHI_REJECTS.items(), key=lambda kv: -kv[1]))))
        if KALSHI_REJECT_SAMPLES:
            _dump_debug("kalshi_rejected.json", KALSHI_REJECT_SAMPLES)
        if pair_samples:
            _dump_debug("kalshi_pair_dropped.json", pair_samples)
        _dump_debug("kalshi_records.json", records)
    return records


def _kalshi_pair_sanity(records):
    """Cross-validate markets within the same Kalshi event. If an event has
    two winner markets, they must (a) be attributed to DIFFERENT teams and
    (b) have midpoints summing to roughly $1.00. Violations mean our side
    attribution can't be trusted -> drop the whole event."""
    by_event, clean, dropped, samples = {}, [], 0, []
    for r in records:
        by_event.setdefault(r.get("event_ticker") or r["url"], []).append(r)
    for evt, rs in by_event.items():
        if len(rs) == 1:
            clean.extend(rs)
            continue
        sides = {r["team"] for r in rs}
        if len(sides) != len(rs):
            dropped += len(rs)   # two markets mapped to the same team: bug
            samples.append({"event": evt, "reason": "duplicate_side",
                            "teams": [r["team"] for r in rs]})
            continue
        if len(rs) == 2:
            mid_sum = sum((r["yes_bid"] + r["yes_ask"]) / 2.0 for r in rs)
            if not (0.85 <= mid_sum <= 1.15):
                dropped += 2     # crossed/stale or misattributed pair
                samples.append({"event": evt, "reason": "mid_sum_off",
                                "mid_sum": round(mid_sum, 3),
                                "teams": [r["team"] for r in rs]})
                continue
        clean.extend(rs)
    return clean, dropped, samples[:20]


KALSHI_REJECTS = {}
KALSHI_REJECT_SAMPLES = []


def _kbump(reason, sample=None):
    KALSHI_REJECTS[reason] = KALSHI_REJECTS.get(reason, 0) + 1
    if sample and len(KALSHI_REJECT_SAMPLES) < 25:
        KALSHI_REJECT_SAMPLES.append({"reason": reason, "sample": sample})


def _kalshi_market_to_record(m, league):
    # Winner markets only: spread/total markets carry strike fields.
    if m.get("floor_strike") is not None or m.get("cap_strike") is not None:
        _kbump("has_strike_field")
        return None
    if m.get("market_type") not in (None, "binary"):
        _kbump("non_binary_market_type", m.get("market_type"))
        return None
    # Teams: event ticker first (deterministic), title text as fallback
    ticker = m.get("event_ticker", "")
    teams = teams_from_kalshi_ticker(ticker, league)
    if len(teams) < 2:
        teams = find_teams_in_text(
            m.get("title", "") + " " + m.get("subtitle", ""), league)
    if len(teams) < 2:
        _kbump("teams_unresolved", ticker or m.get("title"))
        return None
    # Which team does YES refer to? Must resolve UNAMBIGUOUSLY from
    # yes_sub_title (nickname, city, or bare code). If it names zero teams or
    # both teams, we REJECT the market — guessing the side from title order
    # once inverted a Blue Jays/Rays price, and wrong-side data is worse than
    # no data.
    sub = m.get("yes_sub_title", "") or ""
    sub_norm = re.sub(r"\s+", " ", sub.lower().replace(".", "")).strip()
    team = KALSHI_AMBIGUOUS_SUBTITLES.get(league, {}).get(sub_norm)
    if team is None:
        hits = find_teams_in_text(sub, league)
        if len(hits) == 1:
            team = hits[0]
        elif not hits:
            team = find_team_code_token(sub, league)
    if team is None:
        sub2 = m.get("subtitle", "") or ""
        hits2 = find_teams_in_text(sub2, league)
        if len(hits2) == 1:
            team = hits2[0]
    if team is None or team not in teams:
        _kbump("yes_side_ambiguous", f"{ticker} | sub_title={sub!r}")
        return None
    opponent = next((t for t in teams if t != team), None)
    if not opponent:
        _kbump("no_opponent")
        return None
    # home guess: Kalshi tickers run AWAY,HOME (…SEACIN = SEA at CIN) and
    # titles read 'Seattle at Cincinnati'. Display-labeling only — price
    # orientation never depends on this.
    home = None
    tick_teams = teams_from_kalshi_ticker(ticker, league)
    if len(tick_teams) == 2:
        home = tick_teams[1]
    elif " at " in m.get("title", "").lower():
        tt = find_teams_in_text(m.get("title", ""), league)
        if len(tt) >= 2:
            home = tt[1]
    try:
        yes_bid = float(m.get("yes_bid_dollars") or 0)
        yes_ask = float(m.get("yes_ask_dollars") or 0)
    except (TypeError, ValueError):
        _kbump("bad_price_field")
        return None
    if not (PRICE_MIN < yes_ask < PRICE_MAX):
        _kbump("price_out_of_range", yes_ask)
        return None
    if yes_bid > 0 and (yes_ask - yes_bid) > MAX_SPREAD:
        _kbump("spread_too_wide", yes_ask - yes_bid)
        return None
    date_iso = date_only(m.get("expected_expiration_time")
                         or m.get("close_time") or "")
    if not date_iso:
        _kbump("no_date")
        return None
    _kbump("ACCEPTED")
    return {
        "source": "kalshi",
        "league": league,
        "team": team,
        "opponent": opponent,
        "date": date_iso,
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "fee_at_ask": kalshi_taker_fee(yes_ask),
        "volume_24h": float(m.get("volume_24h_fp") or 0),
        "title": m.get("title", ""),
        "event_ticker": m.get("event_ticker", ""),
        "yes_sub_title": m.get("yes_sub_title", ""),
        "home": home,
        "url": f"https://kalshi.com/markets/{m.get('event_ticker', '')}",
    }


# ----------------------------------------------------------------------------
# ADAPTER 2: POLYMARKET (Gamma)
# ----------------------------------------------------------------------------

def fetch_polymarket(debug=False):
    """Prefer targeted tag queries (tag_slug per league) so weekday MLB games
    aren't buried under World Cup / politics volume; fall back to a top-volume
    scan if the tag lookup fails."""
    records, raw_sample = [], None
    POLY_REJECTS.clear()
    POLY_REJECT_SAMPLES.clear()

    def _pull(params, pages):
        nonlocal raw_sample
        got = 0
        for page in range(pages):
            p = dict(params)
            p["offset"] = page * GAMMA_PAGE_SIZE
            p["limit"] = GAMMA_PAGE_SIZE
            url = f"{GAMMA_BASE}/events?{urllib.parse.urlencode(p)}"
            try:
                events = http_get_json(url)
            except Exception as e:  # noqa: BLE001
                print(f"  [polymarket] {e}")
                return got
            if not isinstance(events, list) or not events:
                return got
            if raw_sample is None:
                raw_sample = events[0]
            for ev in events:
                for r in _gamma_event_to_records(ev):
                    if within_horizon(r["date"]):
                        records.append(r)
                        got += 1
            if len(events) < GAMMA_PAGE_SIZE:
                return got
        return got

    tagged = 0
    for league in LEAGUES:
        tag_slug = league.lower()
        try:
            tag = http_get_json(f"{GAMMA_BASE}/tags/slug/{tag_slug}")
            tag_id = tag.get("id")
        except Exception as e:  # noqa: BLE001
            print(f"  [polymarket] tag lookup '{tag_slug}': {e}")
            tag_id = None
        if tag_id:
            tagged += _pull({"tag_id": tag_id, "closed": "false",
                             "active": "true"}, pages=3)
    if tagged == 0:
        # fallback: top-volume scan (old behavior)
        end_max = (datetime.now(timezone.utc)
                   + timedelta(days=MAX_DAYS_AHEAD + 2)
                   ).strftime("%Y-%m-%dT%H:%M:%SZ")
        _pull({"closed": "false", "active": "true", "order": "volume24hr",
               "ascending": "false", "end_date_max": end_max},
              pages=GAMMA_MAX_PAGES)

    if debug and raw_sample:
        _dump_debug("polymarket_sample.json", raw_sample)
    if debug:
        print("  [polymarket] parse tally: " + json.dumps(
            dict(sorted(POLY_REJECTS.items(), key=lambda kv: -kv[1]))))
        if POLY_REJECT_SAMPLES:
            _dump_debug("polymarket_rejected.json", POLY_REJECT_SAMPLES)
    return records


def _teams_from_gamma_slug(slug, league):
    """mlb-sea-cin-2026-07-21 -> ['SEA', 'CIN'] via code aliases."""
    m = re.match(r"^[a-z]+-([a-z0-9]{2,4})-([a-z0-9]{2,4})-\d{4}-\d{2}-\d{2}",
                 slug or "")
    if not m:
        return []
    a = CODE_ALIASES[league].get(m.group(1).upper())
    b = CODE_ALIASES[league].get(m.group(2).upper())
    return [a, b] if a and b and a != b else []


def _detect_league(text):
    for league in LEAGUES:
        if len(find_teams_in_text(text, league)) >= 2:
            return league
    # explicit league tag in title/slug beats team detection ambiguity
    tl = text.lower()
    for league in LEAGUES:
        if league.lower() in tl:
            return league
    return None


POLY_REJECTS = {}
POLY_REJECT_SAMPLES = []


def _bump(reason, sample=None):
    POLY_REJECTS[reason] = POLY_REJECTS.get(reason, 0) + 1
    if sample and len(POLY_REJECT_SAMPLES) < 25:
        POLY_REJECT_SAMPLES.append({"reason": reason, "text": sample})


def _gamma_event_to_records(ev):
    out = []
    title = ev.get("title", "")
    slug = ev.get("slug", "")
    league = _detect_league(f"{title} {slug} {ev.get('seriesSlug', '')}")
    if not league:
        _bump("event_not_our_league")
        return out
    ev_teams = find_teams_in_text(title, league)
    date_iso = date_only(ev.get("endDate") or ev.get("startDate") or "")
    for m in ev.get("markets", []) or []:
        try:
            outcomes = json.loads(m.get("outcomes") or "[]")
            prices = json.loads(m.get("outcomePrices") or "[]")
        except json.JSONDecodeError:
            _bump("bad_json")
            continue
        vol24 = float(m.get("volume24hr") or ev.get("volume24hr") or 0)
        best_bid = float(m.get("bestBid") or 0)
        best_ask = float(m.get("bestAsk") or 0)
        fees_on = bool(m.get("feesEnabled"))
        question = m.get("question", "")
        question_l = " " + question.lower() + " "
        # game start time is the authoritative date; also our live-game filter
        gst = m.get("gameStartTime") or ev.get("gameStartTime")
        if gst:
            if has_started(gst):
                _bump("live_or_started")
                continue  # live/in-progress: not comparable to pregame
            m_date = date_only(gst)
        else:
            m_date = date_only(m.get("endDate") or "") or date_iso
        # --- STRUCTURED SPORTS PATH (primary): Polymarket labels sports
        # markets explicitly. sportsMarketType tells us it's a moneyline;
        # marketMetadata tells us the exact team AND home/away. No guessing.
        smt = m.get("sportsMarketType")
        if smt is not None:
            if smt != "moneyline":
                _bump("non_moneyline_sports")
                continue
            if m.get("closed") or m.get("acceptingOrders") is False:
                _bump("market_closed")
                continue
            meta = m.get("marketMetadata") or {}
            sel_line = (meta.get("opticOddsSelectionLine") or "").lower()
            if sel_line == "draw":
                _bump("draw_market")
                continue
            sel = meta.get("opticOddsSelection") or ""
            hits = find_teams_in_text(sel, league)
            team = hits[0] if len(hits) == 1 else find_team_code_token(sel, league)
            if team is None:
                q_hits = find_teams_in_text(question, league)
                team = q_hits[0] if q_hits else None
            if team is None:
                _bump("selection_unresolved", sel or question)
                continue
            pool = ev_teams[:] or _teams_from_gamma_slug(slug, league)
            opponent = next((t for t in pool if t != team), None)
            if not opponent or not m_date:
                _bump("no_opponent_or_date", f"{question} | {title} | {slug}")
                continue
            if not (PRICE_MIN < best_ask < PRICE_MAX):
                _bump("price_out_of_range")
                continue
            if best_bid > 0 and (best_ask - best_bid) > MAX_SPREAD:
                _bump("spread_too_wide")
                continue
            fee_rate = (m.get("feeSchedule") or {}).get("rate")
            rec = _poly_record(league, team, opponent, m_date, best_bid,
                               best_ask, fees_on, vol24, question, slug,
                               fee_rate=fee_rate)
            # authoritative home/away straight from the API
            if sel_line == "home":
                rec["home"] = team
            elif sel_line == "away":
                rec["home"] = opponent
            _bump("ACCEPTED_sports_moneyline")
            out.append(rec)
            continue
        if outcomes == ["Yes", "No"]:
            # ALLOWLIST: must look exactly like a winner market. The blocklist
            # still runs afterward as a safety net, but the default is reject.
            ql = question_l.strip()
            if not any(p.match(ql) for p in ML_QUESTION_PATTERNS):
                _bump("question_not_winner_shape", question)
                continue
            if any(word in question_l for word in ML_BLOCKLIST):
                _bump("blocklist_word", question)
                continue
            q_teams = find_teams_in_text(question, league)
            if not q_teams:
                _bump("no_team_in_question", question)
                continue
            # subject = FIRST team mentioned ("Will the Reds beat the
            # Mariners?" -> CIN). If only one team is in the question
            # ("Will the Brewers win?"), opponent comes from the event title.
            team = q_teams[0]
            pool = q_teams if len(q_teams) >= 2 else ev_teams
            opponent = next((t for t in pool if t != team), None)
            if not opponent or not m_date:
                _bump("no_opponent_or_date", f"{question} | title: {title}")
                continue
            if not (PRICE_MIN < best_ask < PRICE_MAX):
                _bump("price_out_of_range")
                continue
            if best_bid > 0 and (best_ask - best_bid) > MAX_SPREAD:
                _bump("spread_too_wide")
                continue
            _bump("ACCEPTED_binary")
            out.append(_poly_record(league, team, opponent, m_date, best_bid,
                                    best_ask, fees_on, vol24, question, slug))
        elif len(outcomes) == 2:
            # team-name outcomes: ["Mariners", "Reds"]. Only accept if the
            # event slug is a game slug (mlb-sea-cin-2026-07-20 style) — this
            # is how Polymarket structures single-game winner markets.
            if not GAME_SLUG_RE.match(slug):
                _bump("not_game_slug", slug)
                continue
            if any(word in (" " + title.lower() + " ") for word in ML_BLOCKLIST):
                _bump("blocklist_word_title", title)
                continue
            # teams can live in the outcomes even when the title uses
            # abbreviations the alias map misses
            pool_teams = ev_teams[:]
            for outcome_name in outcomes:
                for t in find_teams_in_text(outcome_name, league):
                    if t not in pool_teams:
                        pool_teams.append(t)
            if len(pool_teams) < 2:
                _bump("teams_unresolved", f"{title} | {outcomes}")
                continue
            for i, outcome_name in enumerate(outcomes):
                t = find_teams_in_text(outcome_name, league)
                if not t:
                    _bump("outcome_team_unresolved", str(outcome_name))
                    continue
                team = t[0]
                opponent = next((x for x in pool_teams if x != team), None)
                try:
                    p = float(prices[i])
                except (ValueError, IndexError, TypeError):
                    _bump("bad_price")
                    continue
                if not opponent or not m_date:
                    _bump("no_opponent_or_date")
                    continue
                if not (PRICE_MIN < p < PRICE_MAX):
                    _bump("price_out_of_range")
                    continue
                _bump("ACCEPTED_team_outcome")
                # no per-outcome bid/ask in this shape; use price as both,
                # flagged as midpoint-based (wider uncertainty)
                out.append(_poly_record(league, team, opponent, m_date, p, p,
                                        fees_on, vol24, title, slug,
                                        midpoint_only=True))
        else:
            _bump("other_market_shape")
    return out


def _poly_record(league, team, opponent, date_iso, bid, ask, fees_on, vol24,
                 title, slug, midpoint_only=False, fee_rate=None):
    return {
        "source": "polymarket",
        "league": league,
        "team": team,
        "opponent": opponent,
        "date": date_iso,
        "yes_bid": bid,
        "yes_ask": ask,
        "fee_at_ask": polymarket_fee(ask, fees_on, fee_rate),
        "fees_enabled": fees_on,
        "fee_rate": fee_rate,
        "volume_24h": vol24,
        "title": title,
        "home": None,
        "url": f"https://polymarket.com/event/{slug}",
        "midpoint_only": midpoint_only,
    }


# ----------------------------------------------------------------------------
# ADAPTER 3: SPORTSBOOK CONSENSUS (SportsGameOdds)
# ----------------------------------------------------------------------------

def fetch_sgo_consensus(api_key, debug=False):
    """Return {game_key: {team_code: devigged_consensus_prob}} built from
    every bookmaker's moneyline, averaged after de-vigging per book."""
    consensus, raw_sample = {}, None
    leagues_param = ",".join(SGO_LEAGUE_IDS[lg] for lg in LEAGUES)
    starts_before = (datetime.now(timezone.utc)
                     + timedelta(days=MAX_DAYS_AHEAD + 1)).strftime("%Y-%m-%d")
    use_window = True
    cursor = None
    for _ in range(5):  # pages; free tier object budget is precious
        params = {"leagueID": leagues_param, "oddsAvailable": "true",
                  "limit": 25}
        if use_window:
            params["startsBefore"] = starts_before
        if cursor:
            params["cursor"] = cursor
        url = f"{SGO_BASE}/events?{urllib.parse.urlencode(params)}"
        try:
            data = http_get_json(url, headers={"x-api-key": api_key})
        except Exception as e:  # noqa: BLE001
            if use_window:
                # param may be unsupported — retry once without the window
                use_window = False
                continue
            print(f"  [sgo] {e}")
            break
        if not data.get("success"):
            if use_window:
                use_window = False
                continue
            print(f"  [sgo] API error: {data.get('error')}")
            break
        events = data.get("data", [])
        if raw_sample is None and events:
            raw_sample = events[0]
        for ev in events:
            parsed = _sgo_event_to_consensus(ev)
            if parsed:
                key, probs = parsed
                if within_horizon(probs.get("_date")):
                    consensus[key] = probs
        cursor = data.get("nextCursor")
        if not cursor or not events:
            break
    if debug and raw_sample:
        _dump_debug("sgo_sample.json", raw_sample)
    return consensus


def _sgo_event_to_consensus(ev):
    league = str(ev.get("leagueID", "")).upper()
    if league not in LEAGUES:
        return None
    # team names: try common paths defensively (schema verified on first run
    # via --debug dump; adjust here if their explorer shows different paths)
    names = json.dumps(ev.get("teams", {}))
    teams_found = find_teams_in_text(names, league)
    if len(teams_found) < 2:
        return None
    # figure out which found team is home vs away
    home_blob = json.dumps((ev.get("teams") or {}).get("home", {}))
    home_hits = find_teams_in_text(home_blob, league)
    if not home_hits:
        return None
    home = home_hits[0]
    away = next((t for t in teams_found if t != home), None)
    if not away:
        return None
    start = (ev.get("status") or {}).get("startsAt") or ev.get("startsAt") or ""
    if has_started(str(start)):
        return None  # in-progress or finished: not a pregame benchmark
    date_iso = date_only(str(start))
    if not date_iso:
        return None
    odds = ev.get("odds") or {}
    ml_home = odds.get("points-home-game-ml-home") or {}
    ml_away = odds.get("points-away-game-ml-away") or {}
    pairs = []
    books_home = ml_home.get("byBookmaker") or {}
    books_away = ml_away.get("byBookmaker") or {}
    for book_id, hdata in books_home.items():
        adata = books_away.get(book_id)
        if not adata:
            continue
        h_odds = hdata.get("odds") or hdata.get("bookOdds")
        a_odds = adata.get("odds") or adata.get("bookOdds")
        if h_odds is None or a_odds is None:
            continue
        try:
            ph, pa = devig_pair(american_to_prob(h_odds),
                                american_to_prob(a_odds))
        except (ValueError, TypeError):
            continue
        if ph:
            pairs.append((ph, pa))
    if not pairs:
        return None
    avg_home = sum(p[0] for p in pairs) / len(pairs)
    avg_away = sum(p[1] for p in pairs) / len(pairs)
    key = game_key(league, home, away, date_iso)
    return key, {home: round(avg_home, 4), away: round(avg_away, 4),
                 "_books": len(pairs), "_date": date_iso, "_league": league,
                 "_home": home}


# ----------------------------------------------------------------------------
# MATCH + COMPUTE
# ----------------------------------------------------------------------------

def _flip_quote(rec):
    """Re-express a YES quote for one team as the equivalent YES quote for the
    opponent, using the same order book: bid' = 1-ask, ask' = 1-bid. Fees are
    recomputed at the new ask. This is pure arithmetic — no text parsing, so
    it cannot mis-attribute a side."""
    new = dict(rec)
    new["team"], new["opponent"] = rec["opponent"], rec["team"]
    new["yes_bid"] = round(1.0 - rec["yes_ask"], 4)
    new["yes_ask"] = round(1.0 - rec["yes_bid"], 4)
    if rec["source"] == "kalshi":
        new["fee_at_ask"] = kalshi_taker_fee(new["yes_ask"])
    else:
        new["fee_at_ask"] = polymarket_fee(new["yes_ask"],
                                           rec.get("fees_enabled", False),
                                           rec.get("fee_rate"))
    return new


def _orient(rec, ref_team):
    """Return the quote expressed as YES on ref_team, flipping if needed."""
    return rec if rec["team"] == ref_team else _flip_quote(rec)


def _quote_quality(rec):
    """Prefer tighter spreads, then higher volume, when one source has two
    markets (one per team) for the same game."""
    return (-(rec["yes_ask"] - rec["yes_bid"]), rec["volume_24h"])


def build_snapshot(kalshi_recs, poly_recs, sgo_consensus):
    """Group per game (unordered team pair + date). EVERY quote is converted
    to one canonical reference side (alphabetically-first team code) before
    storage, so quotes from different markets/venues can never be attached to
    the wrong side. Display orientation is then flipped to the HOME team."""
    by_key = {}
    below_volume = 0
    for rec in kalshi_recs + poly_recs:
        if rec["volume_24h"] < MIN_VOLUME_24H:
            below_volume += 1
            continue
        pair = sorted([rec["team"], rec["opponent"]])
        ref = pair[0]
        key = game_key(rec["league"], pair[0], pair[1], rec["date"])
        g = by_key.setdefault(key, {"league": rec["league"],
                                    "date": rec["date"],
                                    "team": pair[0], "opponent": pair[1],
                                    "quotes": {}, "home": None})
        if rec.get("home") in pair:
            g["home"] = rec["home"]
        oriented = _orient(rec, ref)
        prev = g["quotes"].get(rec["source"])
        if prev is None or _quote_quality(oriented) > _quote_quality(prev):
            g["quotes"][rec["source"]] = oriented

    folded = _fold_adjacent_dates(by_key)

    rows, arbs = [], []
    for key, game in folded.items():
        cons = _lookup_consensus(sgo_consensus, game)
        # Home team: SGO's explicit home/away field is authoritative;
        # Kalshi's ticker convention is the fallback; else show the ref side.
        home = None
        if cons and cons.get("_home") in (game["team"], game["opponent"]):
            home = cons["_home"]
        elif game.get("home"):
            home = game["home"]
        disp = home or game["team"]
        opp = game["opponent"] if disp == game["team"] else game["team"]

        kq = game["quotes"].get("kalshi")
        pq = game["quotes"].get("polymarket")
        kq = _orient(kq, disp) if kq else None
        pq = _orient(pq, disp) if pq else None
        consensus = cons.get(disp) if cons else None

        row = {
            "game_key": key,
            "league": game["league"],
            "date": game["date"],
            "team": disp,
            "opponent": opp,
            "home_team": home,
            "orientation": ("home" if home else "ref"),
            "kalshi": _quote_public(kq),
            "polymarket": _quote_public(pq),
            "book_consensus": consensus,
            "book_count": cons.get("_books") if cons else None,
        }
        # discrepancy vs consensus (uses midpoint of each venue's bid/ask)
        for src, quote in (("kalshi", kq), ("polymarket", pq)):
            if quote and consensus is not None:
                mid = (quote["yes_bid"] + quote["yes_ask"]) / 2.0
                row[f"{src}_edge_vs_books"] = round(mid - consensus, 4)
        # which side does the gap favor?
        best_src, best_edge = None, 0.0
        for src in ("kalshi", "polymarket"):
            e = row.get(f"{src}_edge_vs_books")
            if e is not None and abs(e) > abs(best_edge):
                best_src, best_edge = src, e
        if best_src:
            if best_edge < 0:
                row["signal"] = (
                    f"{disp} priced {abs(best_edge)*100:.1f} pts BELOW "
                    f"book consensus on {best_src} -> value side: "
                    f"'{disp} wins' (YES) on {best_src}")
            else:
                row["signal"] = (
                    f"{disp} priced {best_edge*100:.1f} pts ABOVE "
                    f"book consensus on {best_src} -> value side: "
                    f"'{opp} wins' (i.e. NO {disp}) on {best_src}")
        # cross-venue arb (both quotes are same-side now by construction)
        if kq and pq:
            arb = _check_arb(kq, pq)
            if arb:
                arb["suspect"] = arb["profit_per_dollar"] > ARB_MAX_CREDIBLE
                if arb["suspect"]:
                    arb["note"] = ("profit this large almost always means a "
                                   "data error (side inversion / stale quote)"
                                   " — treat as broken, not free money")
                row["arb"] = arb
                arbs.append({**arb, "game_key": key, "team": disp,
                             "opponent": opp})
        rows.append(row)

    rows.sort(key=lambda r: max(abs(r.get("kalshi_edge_vs_books") or 0),
                                abs(r.get("polymarket_edge_vs_books") or 0)),
              reverse=True)
    # Quarantine implausible edges: almost always a bad match or stale quote.
    clean, suspect = [], []
    for r in rows:
        worst = max(abs(r.get("kalshi_edge_vs_books") or 0),
                    abs(r.get("polymarket_edge_vs_books") or 0))
        (suspect if worst > MAX_PLAUSIBLE_EDGE else clean).append(r)
    credible = [a for a in arbs if not a["suspect"]]
    broken = [a for a in arbs if a["suspect"]]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "leagues": LEAGUES,
        "orientation_note": ("All probabilities are for the HOME team when "
                             "home is known (SGO home/away field, else "
                             "Kalshi ticker convention), across every "
                             "venue."),
        "games": clean,
        "suspect": suspect,
        "arbs": sorted(credible, key=lambda a: a["profit_per_dollar"],
                       reverse=True),
        "arbs_suspect": sorted(broken, key=lambda a: a["profit_per_dollar"],
                               reverse=True),
        "below_volume_floor": below_volume,
        "disclaimer": ("Screener output, not financial advice. Prices move; "
                       "always verify live quotes, fees, liquidity, and "
                       "resolution rules on both venues before trading."),
    }


def _check_arb(rec_a, rec_b):
    """Both records are YES quotes for the SAME team on different venues.
    An arb = buy '<team> wins' on the cheap venue + buy '<opponent> wins'
    (equivalently NO <team>) on the other, total cost + fees < $1.00.
    NO ask is approximated as 1 - yes_bid. Conservative: asks + taker fees
    on both legs."""
    team, opponent = rec_a["team"], rec_a["opponent"]
    results = []
    for buy_yes, buy_no in ((rec_a, rec_b), (rec_b, rec_a)):
        yes_cost = buy_yes["yes_ask"] + buy_yes["fee_at_ask"]
        no_price = 1.0 - buy_no["yes_bid"]  # ask for NO ≈ 1 - bid for YES
        no_fee = (kalshi_taker_fee(no_price)
                  if buy_no["source"] == "kalshi"
                  else polymarket_fee(no_price,
                                      buy_no.get("fees_enabled", False),
                                      buy_no.get("fee_rate")))
        total = yes_cost + no_price + no_fee
        if total < 1.0:
            profit = 1.0 - total
            results.append({
                "leg_1": (f"BUY '{team} wins' (YES) on {buy_yes['source']} "
                          f"@ ${buy_yes['yes_ask']:.2f} "
                          f"(+${buy_yes['fee_at_ask']:.2f} fee)"),
                "leg_2": (f"BUY '{opponent} wins' on {buy_no['source']} "
                          f"@ ~${no_price:.2f} (+${no_fee:.2f} fee) "
                          f"[= NO on {team}]"),
                "buy_yes_on": buy_yes["source"], "yes_ask": buy_yes["yes_ask"],
                "buy_no_on": buy_no["source"], "no_ask_est": round(no_price, 3),
                "total_cost": round(total, 4),
                "profit_per_dollar": round(profit, 4),
                "note": "verify live order books before acting",
            })
    return max(results, key=lambda r: r["profit_per_dollar"]) if results else None


def _fold_adjacent_dates(by_key):
    """UTC/ET edge cases can shift dates across sources; merge within the
    league's match window. MLB window is 0 (series games must never merge)."""
    folded, used = {}, set()
    for key, game in by_key.items():
        if key in used:
            continue
        merged = dict(game)
        window = MATCH_WINDOW.get(game["league"], 0)
        for d in adjacent_dates(game["date"], window):
            alt = game_key(game["league"], game["team"], game["opponent"], d)
            if alt != key and alt in by_key and alt not in used:
                for src, rec in by_key[alt]["quotes"].items():
                    if src not in merged["quotes"]:
                        merged["quotes"][src] = rec
                used.add(alt)
        used.add(key)
        folded[key] = merged
    return folded


def _lookup_consensus(sgo_consensus, game):
    """Return the full consensus dict for this game (probs for both teams,
    _home, _books) or None."""
    window = MATCH_WINDOW.get(game["league"], 0)
    for d in adjacent_dates(game["date"], window):
        key = game_key(game["league"], game["team"], game["opponent"], d)
        if key in sgo_consensus:
            return sgo_consensus[key]
    return None


def _quote_public(rec):
    if not rec:
        return None
    return {"yes_bid": rec["yes_bid"], "yes_ask": rec["yes_ask"],
            "fee_at_ask": rec["fee_at_ask"], "volume_24h": rec["volume_24h"],
            "url": rec["url"]}


def _dump_debug(name, obj):
    os.makedirs("debug", exist_ok=True)
    with open(os.path.join("debug", name), "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    print(f"  [debug] wrote debug/{name}")


# ----------------------------------------------------------------------------
# REPORT
# ----------------------------------------------------------------------------

def print_report(snapshot):
    games = snapshot["games"]
    print(f"\n=== MARKET GAP snapshot @ {snapshot['generated_at']} ===")
    print(f"Matched game-sides: {len(games)} | Credible arbs: "
          f"{len(snapshot['arbs'])} | Suspect arbs (likely data error): "
          f"{len(snapshot.get('arbs_suspect', []))} | Quarantined rows: "
          f"{len(snapshot.get('suspect', []))} | Below volume floor "
          f"(${MIN_VOLUME_24H:.0f}): {snapshot.get('below_volume_floor', 0)}\n")
    if not games:
        print("No matched markets right now (off-season lull is normal in "
              "July is normal — MLB games populate daily around game times).")
        return
    hdr = (f"{'AWAY @ HOME':<24}{'DATE':<12}{'KALSHI':<10}{'POLY':<10}"
           f"{'BOOKS':<8}{'EDGE':<8}")
    print("(all probabilities = HOME team win chance)")
    print(hdr)
    print("-" * len(hdr))
    for r in games[:25]:
        k = r["kalshi"]["yes_ask"] if r["kalshi"] else None
        p = r["polymarket"]["yes_ask"] if r["polymarket"] else None
        b = r["book_consensus"]
        edge = max((abs(r.get("kalshi_edge_vs_books") or 0),
                    abs(r.get("polymarket_edge_vs_books") or 0)))
        label = f"{r['opponent']} @ {r['team']}"
        if r.get("orientation") != "home":
            label += " (?)"  # home unknown; oriented to ref side
        print(f"{label:<24}{r['date']:<12}"
              f"{('%.2f' % k) if k else '--':<10}"
              f"{('%.2f' % p) if p else '--':<10}"
              f"{('%.2f' % b) if b is not None else '--':<8}"
              f"{('%+.1f%%' % (edge*100)) if edge else '':<8}"
              f"{' ARB!' if r.get('arb') else ''}")
    signals = [r for r in games if r.get("signal")][:5]
    if signals:
        print("\nTOP SIGNALS (vs de-vigged book consensus — not advice, verify):")
        for r in signals:
            print(f"  {r['team']} v {r['opponent']} {r['date']}: {r['signal']}")
    for a in snapshot["arbs"][:5]:
        print(f"\n  ARB {a['team']} v {a['opponent']} "
              f"-> {a['profit_per_dollar']*100:.1f}% locked:")
        print(f"    Leg 1: {a['leg_1']}")
        print(f"    Leg 2: {a['leg_2']}")
        print(f"    Total ${a['total_cost']:.2f} per $1 payout ({a['note']})")


# ----------------------------------------------------------------------------
# SELF-TEST (offline, fixture-based)
# ----------------------------------------------------------------------------

def selftest():
    print("Running self-test on fixtures (MLB-only build)...")
    # 1) fee math matches Kalshi's published examples exactly
    assert kalshi_taker_fee(0.50) == 0.02, kalshi_taker_fee(0.50)  # 1.75c -> ceil 2c
    assert kalshi_taker_fee(0.10) == 0.01  # 0.63c -> ceil 1c
    assert kalshi_taker_fee(0.99) == 0.01  # tiny but ceils to 1c
    # 2) de-vig math
    ph, pa = devig_pair(american_to_prob(-150), american_to_prob(+130))
    assert abs((ph + pa) - 1.0) < 1e-9 and ph > pa
    # 2b) game-date normalization: ET keying + early-morning rollback
    assert date_only("2026-07-21T02:00:00Z") == "2026-07-20"   # 10pm ET
    assert date_only("2026-07-21T05:30:00Z") == "2026-07-20"   # 1:30am ET -> prev night
    assert date_only("2026-07-21T14:00:00Z") == "2026-07-21"   # 10am ET
    # 3) title parsing: order of appearance, multiword names, alias scoping
    assert find_teams_in_text("Will the Red Sox beat the Yankees?", "MLB") == ["BOS", "NYY"]
    assert find_teams_in_text("Will the Reds beat the Mariners?", "MLB") == ["CIN", "SEA"]
    assert "SFG" in find_teams_in_text("Giants to win the NL West", "MLB")
    # 3b) Kalshi ticker parsing + code aliases
    assert teams_from_kalshi_ticker("KXMLBGAME-26JUL20SEACIN", "MLB") == ["SEA", "CIN"]
    assert teams_from_kalshi_ticker("KXMLBGAME-26JUL20CHWTEX", "MLB") == ["CWS", "TEX"]
    assert find_team_code_token("SEA", "MLB") == "SEA"
    # 3c) REGRESSION: real tickers have a 4-digit game-time segment between
    # date and team codes (…26JUL231507TBTOR) that an earlier regex missed,
    # silently breaking ticker parsing (and therefore home-team detection)
    # on every single market.
    assert teams_from_kalshi_ticker("KXMLBGAME-26JUL231507TBTOR", "MLB") == ["TB", "TOR"]
    assert teams_from_kalshi_ticker("KXMLBGAME-26JUL221540ATHAZ", "MLB") == ["ATH", "ARI"]
    assert teams_from_kalshi_ticker("KXMLBGAME-26JUL221410SFKC", "MLB") == ["SFG", "KC"]
    # 3d) REGRESSION: 'St. Louis' (period) must match despite the period ->
    # space substitution that used to leave a double space and match nothing
    assert find_teams_in_text("Arizona vs St. Louis Winner?", "MLB") == ["ARI", "STL"]
    # 3e) REGRESSION: Miami was missing from city aliases entirely
    assert find_teams_in_text("Houston vs Miami Winner?", "MLB") == ["HOU", "MIA"]
    # 3f) REGRESSION: Kalshi disambiguates shared-city teams in yes_sub_title
    # with a trailing letter ("Chicago C" = Cubs, "New York Y" = Yankees).
    # These 6 patterns come directly from real rejected markets.
    shared_city_cases = [
        ("KXMLBGAME-26JUL222010DETCHC", "Chicago C", "CHC", "DET"),
        ("KXMLBGAME-26JUL222005CWSTEX", "Chicago WS", "CWS", "TEX"),
        ("KXMLBGAME-26JUL221840LADPHI", "Los Angeles D", "LAD", "PHI"),
        ("KXMLBGAME-26JUL221607STLLAA", "Los Angeles A", "LAA", "STL"),
        ("KXMLBGAME-26JUL221410NYMMIL", "New York M", "NYM", "MIL"),
        ("KXMLBGAME-26JUL221335PITNYY", "New York Y", "NYY", "PIT"),
    ]
    for ticker, sub_title, exp_team, exp_opp in shared_city_cases:
        mfix = {"title": "", "yes_sub_title": sub_title,
                "yes_bid_dollars": "0.50", "yes_ask_dollars": "0.51",
                "volume_24h_fp": "1000",
                "expected_expiration_time": "2026-07-23T02:00:00Z",
                "event_ticker": ticker}
        r = _kalshi_market_to_record(mfix, "MLB")
        assert r and r["team"] == exp_team and r["opponent"] == exp_opp, \
            f"{ticker} {sub_title}: got {r}"
    # 4) Kalshi record normalization on a realistic fixture
    kfix = {"title": "Will the **Mariners** beat the Reds?",
            "yes_sub_title": "Mariners", "yes_bid_dollars": "0.55",
            "yes_ask_dollars": "0.57", "volume_24h_fp": "5000",
            "expected_expiration_time": "2026-07-22T02:00:00Z",
            "event_ticker": "KXMLBGAME-26JUL21SEACIN"}
    rec = _kalshi_market_to_record(kfix, "MLB")
    assert rec and rec["team"] == "SEA" and rec["opponent"] == "CIN"
    assert rec["date"] == "2026-07-21"
    assert rec["fee_at_ask"] == kalshi_taker_fee(0.57)
    # 4b) city-name subtitle resolves the YES side
    kcity = {"title": "Seattle at Cincinnati Winner?", "yes_sub_title": "Seattle",
             "yes_bid_dollars": "0.44", "yes_ask_dollars": "0.46",
             "volume_24h_fp": "800",
             "expected_expiration_time": "2026-07-22T02:00:00Z",
             "event_ticker": "KXMLBGAME-26JUL21SEACIN"}
    rc = _kalshi_market_to_record(kcity, "MLB")
    assert rc and rc["team"] == "SEA" and rc["opponent"] == "CIN"
    # 4c) subject inversion: 'Will the Reds beat the Mariners?' -> team CIN
    pinv = {"title": "Mariners vs. Reds", "slug": "mlb-sea-cin-2026-07-21",
            "seriesSlug": "mlb", "endDate": "2026-07-22T02:00:00Z",
            "volume24hr": 500,
            "markets": [{"question": "Will the Reds beat the Mariners?",
                         "outcomes": '["Yes", "No"]',
                         "outcomePrices": '["0.45", "0.55"]',
                         "bestBid": 0.44, "bestAsk": 0.46,
                         "feesEnabled": False, "volume24hr": 500,
                         "endDate": "2026-07-22T02:00:00Z"}]}
    rinv = _gamma_event_to_records(pinv)
    assert len(rinv) == 1 and rinv[0]["team"] == "CIN" and rinv[0]["opponent"] == "SEA"
    # 4d) run-line / spread markets are rejected
    pspread = {"title": "Brewers vs. Mets", "slug": "mlb-mil-nym-2026-07-21",
               "seriesSlug": "mlb", "endDate": "2026-07-22T02:00:00Z",
               "volume24hr": 500,
               "markets": [{"question": "Will the Brewers beat the Mets by more than 2.5 runs?",
                            "outcomes": '["Yes", "No"]',
                            "outcomePrices": '["0.07", "0.93"]',
                            "bestBid": 0.06, "bestAsk": 0.08,
                            "feesEnabled": False, "volume24hr": 500,
                            "endDate": "2026-07-22T02:00:00Z"}]}
    assert _gamma_event_to_records(pspread) == []
    # 4e) allowlist rejects futures phrased as winner questions
    pfut = dict(pinv)
    pfut["markets"] = [dict(pinv["markets"][0],
                            question="Will the Mariners win the World Series?")]
    pfut["title"], pfut["slug"] = "World Series Champion", "world-series-champion"
    assert _gamma_event_to_records(pfut) == []
    # 5) Polymarket binary-market normalization
    pfix = {"title": "Mariners vs. Reds", "slug": "mlb-sea-cin-2026-07-21",
            "seriesSlug": "mlb", "endDate": "2026-07-22T02:00:00Z",
            "volume24hr": 9000,
            "markets": [{"question": "Will the Mariners beat the Reds?",
                         "outcomes": '["Yes", "No"]',
                         "outcomePrices": '["0.61", "0.39"]',
                         "bestBid": 0.60, "bestAsk": 0.62,
                         "feesEnabled": False, "volume24hr": 9000,
                         "endDate": "2026-07-22T02:00:00Z"}]}
    precs = _gamma_event_to_records(pfix)
    assert len(precs) == 1 and precs[0]["team"] == "SEA"
    # 5b) STRUCTURED sports path: side + home from opticOdds metadata,
    # fee from the market's own feeSchedule (5% of cheaper side)
    psport = {"title": "Mariners vs. Reds", "slug": "mlb-sea-cin-2026-07-21",
              "seriesSlug": "mlb", "endDate": "2026-07-22T02:00:00Z",
              "volume24hr": 4000,
              "markets": [{"question": "Will Seattle win on 2026-07-21?",
                           "outcomes": '["Yes", "No"]',
                           "outcomePrices": '["0.62", "0.38"]',
                           "bestBid": 0.60, "bestAsk": 0.62,
                           "feesEnabled": True, "volume24hr": 4000,
                           "endDate": "2026-07-22T02:00:00Z",
                           "sportsMarketType": "moneyline",
                           "feeSchedule": {"rate": 0.05, "takerOnly": True},
                           "marketMetadata": {
                               "opticOddsSelection": "Mariners",
                               "opticOddsSelectionLine": "away"}}]}
    rs = _gamma_event_to_records(psport)
    assert len(rs) == 1
    assert rs[0]["team"] == "SEA" and rs[0]["opponent"] == "CIN"
    assert rs[0]["home"] == "CIN"          # SEA is 'away' -> home is CIN
    assert abs(rs[0]["fee_at_ask"] - 0.05 * 0.38) < 1e-9  # 5% of cheaper side
    # 5c) draw and non-moneyline sports markets rejected via the label
    pdraw = dict(psport)
    pdraw["markets"] = [dict(psport["markets"][0],
                             marketMetadata={"opticOddsSelection": "Draw",
                                             "opticOddsSelectionLine": "draw"})]
    assert _gamma_event_to_records(pdraw) == []
    pspread2 = dict(psport)
    pspread2["markets"] = [dict(psport["markets"][0],
                                sportsMarketType="spread")]
    assert _gamma_event_to_records(pspread2) == []
    # 6) end-to-end: SMALL (credible) arb is detected and listed.
    # Ticker says SEA at CIN -> home = CIN, so the row is CIN-oriented:
    # kalshi SEA 0.53/0.55 becomes CIN 0.45/0.47; poly SEA 0.60/0.62
    # becomes CIN 0.38/0.40.
    kfix_small = dict(kfix, yes_bid_dollars="0.53", yes_ask_dollars="0.55")
    krec_small = _kalshi_market_to_record(kfix_small, "MLB")
    assert krec_small["home"] == "CIN"
    snap = build_snapshot([krec_small], precs, {})
    assert len(snap["games"]) == 1
    game = snap["games"][0]
    assert game["team"] == "CIN" and game["home_team"] == "CIN"
    assert game["kalshi"] and game["polymarket"]
    assert abs(game["kalshi"]["yes_ask"] - 0.47) < 1e-9
    assert abs(game["polymarket"]["yes_ask"] - 0.40) < 1e-9
    # YES CIN on poly @0.40 + NO CIN on kalshi @ 1-0.45=0.55 (+0.02) => 0.97
    assert game.get("arb"), "expected an arb flag"
    assert abs(game["arb"]["total_cost"] - 0.97) < 0.02
    assert game["arb"]["suspect"] is False
    assert "CIN wins" in game["arb"]["leg_1"] and "polymarket" in game["arb"]["leg_1"]
    assert "SEA wins" in game["arb"]["leg_2"] and "kalshi" in game["arb"]["leg_2"]
    assert len(snap["arbs"]) == 1 and snap["arbs_suspect"] == []
    # 6b) a HUGE 'arb' (23%) is flagged suspect = probable data error
    kfix_big = dict(kfix, yes_bid_dollars="0.33", yes_ask_dollars="0.35")
    krec_big = _kalshi_market_to_record(kfix_big, "MLB")
    snap_big = build_snapshot([krec_big], precs, {})
    barb = snap_big["games"][0]["arb"]
    assert barb["suspect"] is True and "data error" in barb["note"]
    assert snap_big["arbs"] == [] and len(snap_big["arbs_suspect"]) == 1
    # 6c) date-anchored winner questions are accepted ('win on July 21?')
    pdate = dict(pinv)
    pdate["markets"] = [dict(pinv["markets"][0],
                             question="Will the Mariners win on July 21?")]
    rdate = _gamma_event_to_records(pdate)
    assert len(rdate) == 1 and rdate[0]["team"] == "SEA" and rdate[0]["opponent"] == "CIN"
    # 6d) ambiguous YES side (both teams in subtitle) -> REJECTED, not guessed
    kamb = dict(kfix, yes_sub_title="Toronto at Tampa Bay")
    kamb["title"] = "Toronto at Tampa Bay Winner?"
    kamb["event_ticker"] = "KXMLBGAME-26JUL21TORTB"
    assert _kalshi_market_to_record(kamb, "MLB") is None
    # 6e) pair sanity: two markets in one event attributed to the SAME team
    # -> both dropped; a healthy complementary pair -> both kept
    dup = [dict(krec_small), dict(krec_small)]
    kept, dropped, _ = _kalshi_pair_sanity(dup)
    assert kept == [] and dropped == 2
    other = dict(krec_small, team="CIN", opponent="SEA",
                 yes_bid=0.43, yes_ask=0.45)
    kept2, dropped2, _ = _kalshi_pair_sanity([dict(krec_small), other])
    assert len(kept2) == 2 and dropped2 == 0
    # 7) consensus lookup, exact-date only (no folding for MLB); home-oriented
    ck = game_key("MLB", "SEA", "CIN", "2026-07-21")
    edge_snap = build_snapshot([krec_small], precs,
                               {ck: {"SEA": 0.52, "CIN": 0.48, "_home": "CIN",
                                     "_books": 5}})
    g = edge_snap["games"][0]
    assert g["book_consensus"] == 0.48   # CIN's number, matching CIN rows
    # kalshi mid 0.46 -> -2pts; poly mid 0.39 -> -9pts; both below consensus
    assert g["polymarket_edge_vs_books"] < g["kalshi_edge_vs_books"] < 0
    # poly gap is bigger: CIN underpriced there -> value side YES CIN on poly
    assert "BELOW" in g["signal"] and "polymarket" in g["signal"]
    assert "'CIN wins' (YES) on polymarket" in g["signal"]
    # 7b) an ADJACENT-date consensus must NOT match (series-game protection)
    ck_wrong = game_key("MLB", "SEA", "CIN", "2026-07-22")
    miss_snap = build_snapshot([krec_small], precs, {ck_wrong: {"SEA": 0.52, "CIN": 0.48}})
    assert miss_snap["games"][0]["book_consensus"] is None
    # 4f) implausible edges are quarantined out of the main table
    qsnap = build_snapshot([], rinv, {ck: {"CIN": 0.85, "SEA": 0.15}})
    assert qsnap["games"] == [] and len(qsnap["suspect"]) == 1
    print("All self-test groups passed (MLB-only build).")


# ----------------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description="MARKET GAP screener pipeline")
    ap.add_argument("--selftest", action="store_true")
    ap.add_argument("--no-sgo", action="store_true",
                    help="skip sportsbook consensus benchmark")
    ap.add_argument("--debug", action="store_true",
                    help="dump one raw sample per API to ./debug/")
    ap.add_argument("--out", default="snapshot.json")
    args = ap.parse_args()

    if args.selftest:
        selftest()
        return

    print("Fetching Kalshi...")
    kalshi_recs = fetch_kalshi(debug=args.debug)
    print(f"  {len(kalshi_recs)} game-side markets")

    print("Fetching Polymarket...")
    poly_recs = fetch_polymarket(debug=args.debug)
    print(f"  {len(poly_recs)} game-side markets")

    sgo_consensus = {}
    if not args.no_sgo:
        key = os.environ.get("SGO_API_KEY", "").strip()
        if not key:
            print("SGO_API_KEY not set — skipping sportsbook benchmark "
                  "(run with --no-sgo to silence this).")
        else:
            print("Fetching sportsbook consensus (SportsGameOdds)...")
            sgo_consensus = fetch_sgo_consensus(key, debug=args.debug)
            print(f"  consensus built for {len(sgo_consensus)} games")

    snapshot = build_snapshot(kalshi_recs, poly_recs, sgo_consensus)
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, indent=2)
    print(f"Wrote {args.out}")
    print_report(snapshot)


if __name__ == "__main__":
    main()
