"""
One-time backfill: fetch ALL messages from a start date and rebuild docs/data.json.
Handles rockets, drones, flash warnings, and event-end messages for accurate shelter time.

Usage:
    BACKFILL_FROM=2026-02-27 python scripts/backfill.py
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
BACKFILL_FROM = os.environ.get("BACKFILL_FROM", "2026-02-27")

CHANNEL   = "@PikudHaOref_all"
DATA_FILE = Path(__file__).parent.parent / "docs" / "data.json"

# Israel Standard Time (UTC+2)
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


def is_alert(text: str) -> bool:
    return bool(text) and any(kw in text for kw in ALERT_KEYWORDS)

def is_drone_alert(text: str) -> bool:
    return bool(text) and "חדירת כלי טיס" in text and "האירוע הסתיים" not in text

def is_flash_warning(text: str) -> bool:
    return bool(text) and "מבזק" in text and "האירוע הסתיים" not in text

def parse_event_end(text: str) -> dict | None:
    if not text or "האירוע הסתיים" not in text:
        return None
    end_type = "drone" if ("חדירת כלי טיס" in text or "כלי טיס עוין" in text) else "rocket"
    return {
        "type":    end_type,
        "cities":  extract_cities(text),
        "regions": extract_regions(text),
    }

def extract_regions(text: str) -> list[str]:
    return [r for r in REGIONS if r in text]

def extract_cities(text: str) -> list[str]:
    cities = []
    lines  = text.split("\n")
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
    """Returns {region_name: [city1, city2, ...]} preserving region→city grouping."""
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
            current_region = None
    return result


def resolve_shelter_backfill(
    pending: list[dict],
    shelter_minutes: dict,
    event_log: list,
    end_time: datetime,
    end_info: dict,
) -> None:
    """
    Same logic as collect.py resolve_shelter but operates on a local pending list.
    Also writes resolved events to event_log.
    """
    end_cities  = set(end_info.get("cities",  []))
    end_regions = set(end_info.get("regions", []))
    end_type    = end_info.get("type", "rocket")

    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=timezone.utc)

    matched: list[dict] = []
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

    for city, earliest in city_earliest.items():
        duration = max(10, int((end_time - earliest).total_seconds() / 60))
        shelter_minutes[city] = shelter_minutes.get(city, 0) + duration

    if city_earliest:
        overall_start    = min(city_earliest.values())
        overall_duration = max(10, int((end_time - overall_start).total_seconds() / 60))
        event_log.insert(0, {
            "date":         end_time.isoformat(),
            "type":         end_type,
            "duration_min": overall_duration,
            "cities":       list(city_earliest.keys()),
            "regions":      end_info.get("regions", []),
        })

    for p in matched:
        pending.remove(p)


async def backfill() -> None:
    start_date = datetime.strptime(BACKFILL_FROM, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    print(f"Backfilling from {start_date.date()} ...")

    data: dict = {
        "last_updated": None, "last_message_id": 0, "total_alerts": 0,
        "regions": {r: 0 for r in REGIONS},
        "cities": {},
        "drone_alerts": 0, "drone_regions": {}, "drone_cities": {},
        "flash_warnings": 0, "flash_regions": {}, "flash_cities": {},
        "shelter_minutes": {}, "pending_shelter": [],
        "city_by_region": {},
        "event_log": [],
        "flash_conversion": {
            "converted": 0, "not_converted": 0,
            "total_gap_seconds": 0, "count_with_gap": 0,
        },
        "pending_flash": [],
        "hourly_counts": {str(h): 0 for h in range(24)},
        "city_fp_count": {},   # cities warned in מבזק but absent from subsequent real alert
        "daily_counts":  {},   # {date: N} — unique alert messages per day (excl. flash)
        "daily": {}, "recent": [],
    }

    async with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as client:
        try:
            entity = await client.get_entity(CHANNEL)
            print(f"Channel resolved: title='{entity.title}', id={entity.id}")
        except Exception as e:
            print(f"ERROR resolving channel '{CHANNEL}': {e}")
            return

        print("Fetching messages ...")
        messages = []
        try:
            async for msg in client.iter_messages(
                entity, reverse=True, offset_date=start_date, limit=None,
            ):
                messages.append(msg)
                if len(messages) % 500 == 0:
                    print(f"  ... {len(messages)} messages fetched so far")
        except FloodWaitError as e:
            wait = min(e.seconds, 120)
            print(f"FloodWaitError: sleeping {wait}s then continuing")
            await asyncio.sleep(wait)
        except Exception as e:
            print(f"ERROR fetching messages: {e}")
            if not messages:
                return

    print(f"Total messages fetched: {len(messages)}")

    new_alerts = new_flashes = 0
    # Local pending lists for backfill matching
    pending_shelter: list[dict] = []
    pending_flash:   list[dict] = []
    event_log:       list[dict] = []
    fc = data["flash_conversion"]

    for msg in messages:  # chronological (oldest first)
        text     = msg.text or ""
        msg_time = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
        data["last_message_id"] = max(data["last_message_id"], msg.id)

        # ── 1. Event-ended → resolve shelter ─────────────────────────────────
        end_info = parse_event_end(text)
        if end_info:
            resolve_shelter_backfill(
                pending_shelter, data["shelter_minutes"], event_log,
                msg_time, end_info,
            )
            continue

        # ── 2. Flash pre-warning (מבזק) ───────────────────────────────────────
        if is_flash_warning(text):
            regions = extract_regions(text)
            cities  = extract_cities(text)
            if regions:
                new_flashes += 1
                data["flash_warnings"] += 1
                for r in regions:
                    data["flash_regions"][r] = data["flash_regions"].get(r, 0) + 1
                for c in cities:
                    data["flash_cities"][c] = data["flash_cities"].get(c, 0) + 1
                pending_flash.append({
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

        # ── 3. Regular alert ──────────────────────────────────────────────────
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
            data["drone_alerts"] += 1
            for r in regions:
                data["drone_regions"][r] = data["drone_regions"].get(r, 0) + 1
            for c in cities:
                data["drone_cities"][c] = data["drone_cities"].get(c, 0) + 1

        # Update city_by_region
        rc_map = extract_regions_with_cities(text)
        cbr = data["city_by_region"]
        for region, rcities in rc_map.items():
            cbr.setdefault(region, {})
            for city in rcities:
                cbr[region][city] = cbr[region].get(city, 0) + 1

        # Update hourly_counts (Israel time)
        hour_str = str(msg_time.astimezone(IST).hour)
        data["hourly_counts"][hour_str] = data["hourly_counts"].get(hour_str, 0) + 1

        # Check flash conversion: did a flash warning predict this alert?
        ac = set(cities)
        ar = set(regions)
        remaining_flash: list[dict] = []
        for pf in pending_flash:
            pf_time = datetime.fromisoformat(pf["time"])
            if pf_time.tzinfo is None:
                pf_time = pf_time.replace(tzinfo=timezone.utc)
            diff_sec = (msg_time - pf_time).total_seconds()
            if diff_sec < 0:
                remaining_flash.append(pf)
                continue
            if diff_sec > 3600:
                fc["not_converted"] += 1
                continue
            pf_cities  = set(pf.get("cities",  []))
            pf_regions = set(pf.get("regions", []))
            if (pf_cities & ac) or (pf_regions & ar):
                fc["converted"]         += 1
                fc["total_gap_seconds"] += diff_sec
                fc["count_with_gap"]    += 1
                # False-positive cities: warned in מבזק but absent from the real alert
                fp_map = data.setdefault("city_fp_count", {})
                for city in pf_cities - ac:
                    fp_map[city] = fp_map.get(city, 0) + 1
            else:
                remaining_flash.append(pf)
        pending_flash = remaining_flash

        pending_shelter.append({
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

    # Any unmatched pending_shelter → default 10 min each
    for p in pending_shelter:
        for city in p.get("cities", []):
            data["shelter_minutes"][city] = data["shelter_minutes"].get(city, 0) + 10

    # Any remaining pending_flash → not_converted
    fc["not_converted"] += len(pending_flash)

    # Cap event_log at 500
    data["event_log"]       = event_log[:500]
    data["pending_shelter"] = []
    data["pending_flash"]   = []
    data["recent"]          = data["recent"][:100]
    data["last_updated"]    = datetime.now(timezone.utc).isoformat()

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    total_shelter_h = sum(data["shelter_minutes"].values()) / 60
    conv_rate = (
        fc["converted"] / (fc["converted"] + fc["not_converted"]) * 100
        if (fc["converted"] + fc["not_converted"]) > 0 else 0
    )
    print(
        f"Done — {new_alerts} alerts, {data['drone_alerts']} drones, "
        f"{new_flashes} flash warnings ({conv_rate:.0f}% conversion rate), "
        f"last_message_id={data['last_message_id']}, "
        f"days={len(data['daily'])}, "
        f"total shelter time={total_shelter_h:.0f}h across all cities"
    )


if __name__ == "__main__":
    asyncio.run(backfill())
