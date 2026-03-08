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
from datetime import datetime, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

API_ID   = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION  = os.environ["TG_SESSION"]

CHANNEL   = "@PikudHaOref_all"
DATA_FILE = Path(__file__).parent.parent / "docs" / "data.json"

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

    The event-ended message now includes the specific cities that were under
    alert, enabling precise per-city shelter-time calculation.
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

    matched   = []          # pending entries to remove
    city_earliest: dict[str, datetime] = {}  # city → earliest alert start

    for p in pending:
        start = datetime.fromisoformat(p["start"])
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        diff_min = (end_time - start).total_seconds() / 60
        if not (0 < diff_min <= 240):
            continue  # out of window

        p_cities  = set(p.get("cities",  []))
        p_regions = set(p.get("regions", []))
        p_type    = p.get("type", "rocket")

        # Determine which cities from this pending entry are matched
        if end_cities:                          # best: city-level match
            hit_cities = p_cities & end_cities
            if not hit_cities:
                continue
        elif end_regions:                       # fallback: region-level match
            if not (p_regions & end_regions):
                continue
            hit_cities = p_cities
        else:                                   # last resort: type match
            if p_type != end_type:
                continue
            hit_cities = p_cities

        for city in hit_cities:
            if city not in city_earliest or start < city_earliest[city]:
                city_earliest[city] = start
        matched.append(p)

    # Add shelter duration per city (one entry per city, from its earliest alert)
    for city, earliest in city_earliest.items():
        duration = max(10, int((end_time - earliest).total_seconds() / 60))
        data["shelter_minutes"][city] = data["shelter_minutes"].get(city, 0) + duration

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
