"""
Collect alerts from @PikudHaOref_all Telegram channel and save to docs/data.json.
Reads secrets from environment: TG_API_ID, TG_API_HASH, TG_SESSION

Message types handled:
  🚨 ירי רקטות וטילים  — rocket/missile alert
  ✈  חדירת כלי טיס עוין — drone/UAV alert
  🚨 מבזק               — flash pre-warning (alerts expected soon)
  האירוע הסתיים         — event ended (includes specific cities for accurate shelter time)
"""
import asyncio
import json
import os
import re
from datetime import datetime, timezone, timedelta
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

API_ID   = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION  = os.environ["TG_SESSION"]

CHANNEL   = "@PikudHaOref_all"
DATA_FILE = Path(__file__).parent.parent / "docs" / "data.json"

# Israel Standard Time (UTC+2; approximate — IDT is UTC+3 in summer)
IST = timezone(timedelta(hours=2))

REGIONS = [
    "אזור שומרון", "אזור השפלה", "אזור יהודה", "אזור לכיש",
    "אזור שרון", "אזור ירקון", "אזור גליל עליון", "אזור שפלת יהודה",
    "אזור העמקים", "אזור בקעה", "אזור קו העימות", "אזור דן",
    "אזור גולן דרום", "אזור גליל תחתון", "אזור מרכז הגליל", "אזור ים המלח",
    "אזור הכרמל", "אזור דרום הנגב", "אזור ואדי ערה", "אזור בקעת בית שאן",
    "אזור מערב הנגב", "אזור המפרץ", "אזור ירושלים", "אזור מערב לכיש",
    "אזור מרכז הנגב", "אזור מנשה", "אזור עוטף עזה", "אזור גולן צפון",
    "אזור ערבה", "אזור אילת",
]

ALERT_KEYWORDS = [
    "ירי רקטות", "חדירת כלי טיס", "אירוע חבלני", "רעידת אדמה",
    "שיירת כלי טיס", "כלי טיס עוין", "חשש לחדירה", "צבע אדום", "כוננות",
]

_FRESH_FLASH_CONVERSION = lambda: {
    "converted": 0, "not_converted": 0,
    "total_gap_seconds": 0, "count_with_gap": 0,
}

# city_fp_count — cities warned in a מבזק but absent from the following real alert.
# flash_cities already counts total flash appearances; fp_rate = fp_count / flash_count.


# ─── Data loading / saving ────────────────────────────────────────────────────

def load_data() -> dict:
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Migrations: add any fields missing from older versions
        defaults = {
            "cities": {}, "drone_alerts": 0, "drone_regions": {}, "drone_cities": {},
            "flash_warnings": 0, "flash_regions": {}, "flash_cities": {},
            "shelter_minutes": {}, "pending_shelter": [],
            "city_by_region": {},
            "event_log": [],
            "flash_conversion": _FRESH_FLASH_CONVERSION(),
            "pending_flash": [],
            "hourly_counts": {str(h): 0 for h in range(24)},
            "city_fp_count": {},   # cities warned in מבזק but absent from subsequent real alert
            "daily_counts":  {},   # {date: N} — unique alert messages per day (excl. flash)
        }
        for k, v in defaults.items():
            if k not in data:
                data[k] = v
        return data
    return {
        "last_updated": None, "last_message_id": 0, "total_alerts": 0,
        "regions": {r: 0 for r in REGIONS},
        "cities": {},
        "drone_alerts": 0, "drone_regions": {}, "drone_cities": {},
        "flash_warnings": 0, "flash_regions": {}, "flash_cities": {},
        "shelter_minutes": {}, "pending_shelter": [],
        "city_by_region": {},
        "event_log": [],
        "flash_conversion": _FRESH_FLASH_CONVERSION(),
        "pending_flash": [],
        "hourly_counts": {str(h): 0 for h in range(24)},
        "city_fp_count": {},
        "daily_counts":  {},
        "daily": {}, "recent": [],
    }


def save_data(data: dict) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ─── Message classification ───────────────────────────────────────────────────

def is_alert(text: str) -> bool:
    return bool(text) and any(kw in text for kw in ALERT_KEYWORDS)

def is_drone_alert(text: str) -> bool:
    return bool(text) and "חדירת כלי טיס" in text and "האירוע הסתיים" not in text

def is_flash_warning(text: str) -> bool:
    """מבזק — pre-warning that alerts are expected soon in certain areas."""
    return bool(text) and "מבזק" in text and "האירוע הסתיים" not in text

def parse_event_end(text: str) -> dict | None:
    """
    Returns {'type': 'rocket'|'drone', 'cities': [...], 'regions': [...]}
    or None if this is not an event-end message.
    """
    if not text or "האירוע הסתיים" not in text:
        return None
    end_type = "drone" if ("חדירת כלי טיס" in text or "כלי טיס עוין" in text) else "rocket"
    return {
        "type":    end_type,
        "cities":  extract_cities(text),
        "regions": extract_regions(text),
    }


# ─── Field extractors ────────────────────────────────────────────────────────

def extract_regions(text: str) -> list[str]:
    return [r for r in REGIONS if r in text]


def extract_cities(text: str) -> list[str]:
    """
    Extract city names from alert / event-end text.
    Cities appear comma-separated on the line immediately after **אזור X** headers.
    Strips shelter-countdown suffixes like (**מיידי**) / (**30 שניות**).
    """
    cities = []
    lines = text.split("\n")
    next_is_city_line = False
    for line in lines:
        stripped = line.strip()
        if re.match(r"\*\*אזור .+\*\*", stripped):
            next_is_city_line = True
            continue
        if next_is_city_line:
            if stripped and not stripped.startswith("**") \
                    and not stripped.startswith("🚨") \
                    and not stripped.startswith("✈"):
                for city in stripped.split(","):
                    city = city.strip()
                    city = re.sub(r'\s*\(\*\*.*?\*\*\)', '', city).strip()
                    if city and len(city) > 1:
                        cities.append(city)
            next_is_city_line = False
    return list(dict.fromkeys(cities))


def extract_regions_with_cities(text: str) -> dict:
    """
    Returns {region_name: [city1, city2, ...]} preserving the region→city
    grouping from the message format. Used for city_by_region tracking.
    """
    result: dict[str, list[str]] = {}
    lines = text.split("\n")
    current_region: str | None = None
    for line in lines:
        stripped = line.strip()
        if re.match(r"\*\*אזור .+\*\*", stripped):
            region_name = stripped.strip("*").strip()
            if region_name in REGIONS:
                current_region = region_name
                result.setdefault(current_region, [])
            else:
                current_region = None
            continue
        if current_region is not None:
            if stripped and not stripped.startswith("**") \
                    and not stripped.startswith("🚨") \
                    and not stripped.startswith("✈"):
                for city in stripped.split(","):
                    city = city.strip()
                    city = re.sub(r'\s*\(\*\*.*?\*\*\)', '', city).strip()
                    if city and len(city) > 1:
                        result[current_region].append(city)
            current_region = None   # cities only on the line immediately after header
    return result


# ─── Flash conversion tracking ───────────────────────────────────────────────

def check_flash_conversion(
    data: dict, alert_time: datetime,
    alert_cities: list[str], alert_regions: list[str],
) -> None:
    """
    Check if this incoming alert was predicted by a recent flash warning.
    Matched flash entries are consumed (removed from pending_flash).
    Expired entries (>1 h) are counted as not_converted.
    """
    if alert_time.tzinfo is None:
        alert_time = alert_time.replace(tzinfo=timezone.utc)

    ac = set(alert_cities)
    ar = set(alert_regions)
    fc = data.setdefault("flash_conversion", _FRESH_FLASH_CONVERSION())
    remaining: list[dict] = []

    for pf in data.get("pending_flash", []):
        pf_time = datetime.fromisoformat(pf["time"])
        if pf_time.tzinfo is None:
            pf_time = pf_time.replace(tzinfo=timezone.utc)
        diff_sec = (alert_time - pf_time).total_seconds()

        if diff_sec < 0:
            remaining.append(pf)
            continue
        if diff_sec > 3600:
            fc["not_converted"] = fc.get("not_converted", 0) + 1
            continue

        pf_cities  = set(pf.get("cities",  []))
        pf_regions = set(pf.get("regions", []))
        if (pf_cities & ac) or (pf_regions & ar):
            fc["converted"]         = fc.get("converted", 0) + 1
            fc["total_gap_seconds"] = fc.get("total_gap_seconds", 0) + diff_sec
            fc["count_with_gap"]    = fc.get("count_with_gap", 0) + 1
            # False-positive cities: warned in the מבזק but absent from the real alert.
            # These residents were told to prepare but no rocket/drone reached their area.
            fp_map = data.setdefault("city_fp_count", {})
            for city in pf_cities - ac:           # in flash, NOT in actual alert
                fp_map[city] = fp_map.get(city, 0) + 1
            # consumed — don't re-add to remaining
        else:
            remaining.append(pf)

    data["pending_flash"] = remaining


# ─── Shelter time tracking ────────────────────────────────────────────────────

def resolve_shelter(data: dict, end_time: datetime, end_info: dict) -> None:
    """
    Match an event-end message to pending shelter entries.

    Strategy (most-accurate first):
      1. City overlap  — end message lists specific cities → match by city intersection.
         For each matched city use the EARLIEST pending start → accurate real duration.
      2. Region overlap — no cities in end message → match by region intersection.
      3. Type fallback  — no region info → match by alert type within 4 hours.
    """
    pending = data.get("pending_shelter", [])
    if not pending:
        return

    end_cities  = set(end_info.get("cities",  []))
    end_regions = set(end_info.get("regions", []))
    end_type    = end_info.get("type", "rocket")

    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    matched   = []
    city_earliest: dict[str, datetime] = {}

    for p in pending:
        start = datetime.fromisoformat(p["start"])
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        diff_min = (end_time - start).total_seconds() / 60
        if not (0 < diff_min <= 240):
            continue

        p_cities  = set(p.get("cities",  []))
        p_regions = set(p.get("regions", []))
        p_type    = p.get("type", "rocket")

        if end_cities:
            hit_cities = p_cities & end_cities
            if not hit_cities:
                continue
        elif end_regions:
            if not (p_regions & end_regions):
                continue
            hit_cities = p_cities
        else:
            if p_type != end_type:
                continue
            hit_cities = p_cities

        for city in hit_cities:
            if city not in city_earliest or start < city_earliest[city]:
                city_earliest[city] = start
        matched.append(p)

    # Add shelter duration per city
    for city, earliest in city_earliest.items():
        duration = max(10, int((end_time - earliest).total_seconds() / 60))
        data["shelter_minutes"][city] = data["shelter_minutes"].get(city, 0) + duration

    # Record resolved event in event_log (overall duration = end - earliest start)
    if city_earliest:
        overall_start    = min(city_earliest.values())
        overall_duration = max(10, int((end_time - overall_start).total_seconds() / 60))
        event_log = data.setdefault("event_log", [])
        event_log.insert(0, {
            "date":         end_time.isoformat(),
            "type":         end_type,
            "duration_min": overall_duration,
            "cities":       list(city_earliest.keys()),
            "regions":      end_info.get("regions", []),
        })
        data["event_log"] = event_log[:500]

    for p in matched:
        pending.remove(p)
    data["pending_shelter"] = pending


def expire_old_pending(data: dict) -> None:
    """Pending alerts older than 2 h with no matching end → default 10 min."""
    now = datetime.now(timezone.utc)
    remaining = []
    for p in data.get("pending_shelter", []):
        start = datetime.fromisoformat(p["start"])
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if (now - start).total_seconds() / 60 > 120:
            for city in p.get("cities", []):
                data["shelter_minutes"][city] = data["shelter_minutes"].get(city, 0) + 10
        else:
            remaining.append(p)
    data["pending_shelter"] = remaining

    # Expire old pending_flash entries → not_converted
    fc = data.setdefault("flash_conversion", _FRESH_FLASH_CONVERSION())
    flash_remaining = []
    for pf in data.get("pending_flash", []):
        pf_time = datetime.fromisoformat(pf["time"])
        if pf_time.tzinfo is None:
            pf_time = pf_time.replace(tzinfo=timezone.utc)
        if (now - pf_time).total_seconds() > 3600:
            fc["not_converted"] = fc.get("not_converted", 0) + 1
        else:
            flash_remaining.append(pf)
    data["pending_flash"] = flash_remaining


# ─── Main collector ───────────────────────────────────────────────────────────

async def collect() -> None:
    data    = load_data()
    last_id = data.get("last_message_id", 0)
    print(f"Starting — last_message_id={last_id}")

    expire_old_pending(data)

    async with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as client:
        try:
            entity = await client.get_entity(CHANNEL)
            print(f"Channel resolved: title='{entity.title}', username=@{entity.username}, id={entity.id}")
        except Exception as e:
            print(f"ERROR resolving channel '{CHANNEL}': {e}")
            return

        try:
            messages = []
            async for msg in client.iter_messages(entity, min_id=last_id, limit=500):
                messages.append(msg)
        except FloodWaitError as e:
            print(f"FloodWaitError: sleeping {e.seconds}s and exiting")
            await asyncio.sleep(min(e.seconds, 60))
            return

    print(f"Fetched {len(messages)} messages total")
    for msg in messages[:3]:
        print(f"  Sample msg id={msg.id}: {repr((msg.text or '')[:80].replace(chr(10),' '))}")

    if not messages:
        print("No new messages since last run")
        data["last_updated"] = datetime.now(timezone.utc).isoformat()
        save_data(data)
        return

    print(f"Fetched {len(messages)} new messages (since id={last_id})")
    new_alerts = new_flashes = 0

    for msg in reversed(messages):  # oldest → newest
        text     = msg.text or ""
        msg_time = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
        data["last_message_id"] = max(data["last_message_id"], msg.id)

        # ── 1. Event-ended → resolve shelter time ────────────────────────────
        end_info = parse_event_end(text)
        if end_info:
            resolve_shelter(data, msg_time, end_info)
            continue

        # ── 2. Flash pre-warning (מבזק) ──────────────────────────────────────
        if is_flash_warning(text):
            regions = extract_regions(text)
            cities  = extract_cities(text)
            if regions:
                new_flashes += 1
                data["flash_warnings"] = data.get("flash_warnings", 0) + 1
                for r in regions:
                    data["flash_regions"][r] = data["flash_regions"].get(r, 0) + 1
                for c in cities:
                    data["flash_cities"][c] = data["flash_cities"].get(c, 0) + 1
                # Add to pending_flash for conversion tracking
                data.setdefault("pending_flash", []).append({
                    "time":    msg_time.isoformat(),
                    "cities":  cities,
                    "regions": regions,
                })
                data["recent"].insert(0, {
                    "id": msg.id, "date": msg.date.isoformat(),
                    "text": text[:300], "regions": regions, "cities": cities,
                    "type": "flash",
                })
            continue

        # ── 3. Regular alert (rocket / drone) ────────────────────────────────
        if not is_alert(text):
            continue

        regions = extract_regions(text)
        if not regions:
            continue

        cities   = extract_cities(text)
        date_str = msg.date.astimezone(timezone.utc).strftime("%Y-%m-%d")
        a_type   = "drone" if is_drone_alert(text) else "rocket"

        data["total_alerts"] += 1
        new_alerts += 1
        # daily_counts tracks unique alert messages per day (not region-multiplied)
        dc = data.setdefault("daily_counts", {})
        dc[date_str] = dc.get(date_str, 0) + 1

        for region in regions:
            data["regions"][region] = data["regions"].get(region, 0) + 1
            data["daily"].setdefault(date_str, {})
            data["daily"][date_str][region] = data["daily"][date_str].get(region, 0) + 1

        for city in cities:
            data["cities"][city] = data["cities"].get(city, 0) + 1

        if a_type == "drone":
            data["drone_alerts"] = data.get("drone_alerts", 0) + 1
            for r in regions:
                data["drone_regions"][r] = data["drone_regions"].get(r, 0) + 1
            for c in cities:
                data["drone_cities"][c] = data["drone_cities"].get(c, 0) + 1

        # Update city_by_region (preserves region → city grouping from message)
        rc_map = extract_regions_with_cities(text)
        cbr = data.setdefault("city_by_region", {})
        for region, rcities in rc_map.items():
            cbr.setdefault(region, {})
            for city in rcities:
                cbr[region][city] = cbr[region].get(city, 0) + 1

        # Update hourly_counts (Israel time)
        hour_str = str(msg_time.astimezone(IST).hour)
        hc = data.setdefault("hourly_counts", {str(h): 0 for h in range(24)})
        hc[hour_str] = hc.get(hour_str, 0) + 1

        # Update city_hourly_counts — {city: {hour: count}} in Israel time
        chc = data.setdefault("city_hourly_counts", {})
        for city in cities:
            chc.setdefault(city, {})
            chc[city][hour_str] = chc[city].get(hour_str, 0) + 1

        # Check if this alert was predicted by a recent flash warning
        check_flash_conversion(data, msg_time, cities, regions)

        # Queue for shelter-time matching against future event-end messages
        data["pending_shelter"].append({
            "start":   msg_time.isoformat(),
            "cities":  cities,
            "regions": regions,
            "type":    a_type,
        })

        data["recent"].insert(0, {
            "id": msg.id, "date": msg.date.isoformat(),
            "text": text[:300], "regions": regions, "cities": cities,
            "type": a_type,
        })

    data["recent"] = data["recent"][:100]
    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    save_data(data)
    print(f"Done — {new_alerts} new alerts, {new_flashes} flash warnings, "
          f"last_message_id={data['last_message_id']}")


if __name__ == "__main__":
    asyncio.run(collect())
