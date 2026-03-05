"""
Collect alerts from @pikud_haoref Telegram channel and save to docs/data.json.
Reads secrets from environment: TG_API_ID, TG_API_HASH, TG_SESSION
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

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION = os.environ["TG_SESSION"]

CHANNEL = "@PikudHaOref_all"
DATA_FILE = Path(__file__).parent.parent / "docs" / "data.json"

REGIONS = [
    "אזור שומרון",
    "אזור השפלה",
    "אזור יהודה",
    "אזור לכיש",
    "אזור שרון",
    "אזור ירקון",
    "אזור גליל עליון",
    "אזור שפלת יהודה",
    "אזור העמקים",
    "אזור בקעה",
    "אזור קו העימות",
    "אזור דן",
    "אזור גולן דרום",
    "אזור גליל תחתון",
    "אזור מרכז הגליל",
    "אזור ים המלח",
    "אזור הכרמל",
    "אזור דרום הנגב",
    "אזור ואדי ערה",
    "אזור בקעת בית שאן",
    "אזור מערב הנגב",
    "אזור המפרץ",
    "אזור ירושלים",
    "אזור מערב לכיש",
    "אזור מרכז הנגב",
    "אזור מנשה",
    "אזור עוטף עזה",
    "אזור גולן צפון",
    "אזור ערבה",
    "אזור אילת",
]

ALERT_KEYWORDS = [
    "ירי רקטות",
    "חדירת כלי טיס",
    "אירוע חבלני",
    "רעידת אדמה",
    "שיירת כלי טיס",
    "כלי טיס עוין",
    "חשש לחדירה",
    "צבע אדום",
    "כוננות",
]


def load_data() -> dict:
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        # Migrations: add missing fields from older versions
        if "cities" not in data:
            data["cities"] = {}
        if "drone_alerts" not in data:
            data["drone_alerts"] = 0
        if "drone_regions" not in data:
            data["drone_regions"] = {}
        if "drone_cities" not in data:
            data["drone_cities"] = {}
        if "shelter_minutes" not in data:
            data["shelter_minutes"] = {}
        if "pending_shelter" not in data:
            data["pending_shelter"] = []
        return data
    return {
        "last_updated": None,
        "last_message_id": 0,
        "total_alerts": 0,
        "regions": {r: 0 for r in REGIONS},
        "cities": {},
        "drone_alerts": 0,
        "drone_regions": {},
        "drone_cities": {},
        "shelter_minutes": {},
        "pending_shelter": [],
        "daily": {},
        "recent": [],
    }


def save_data(data: dict) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def is_alert(text: str) -> bool:
    return bool(text) and any(kw in text for kw in ALERT_KEYWORDS)


def is_drone_alert(text: str) -> bool:
    """Returns True if this is a drone/UAV intrusion alert (not an event-end)."""
    return bool(text) and "חדירת כלי טיס" in text and "האירוע הסתיים" not in text


def event_end_type(text: str) -> str | None:
    """
    Returns 'drone', 'rocket', or None.
    'drone'  → drone event ended  (חדירת כלי טיס ... האירוע הסתיים)
    'rocket' → rocket event ended (ירי רקטות  ... האירוע הסתיים)
    None     → not an event-end message
    """
    if not text or "האירוע הסתיים" not in text:
        return None
    if "חדירת כלי טיס" in text or "כלי טיס עוין" in text:
        return "drone"
    return "rocket"


def extract_regions(text: str) -> list[str]:
    return [r for r in REGIONS if r in text]


def extract_cities(text: str) -> list[str]:
    """
    Extract city names from alert text.
    Cities appear comma-separated on the line after **אזור X** headers.
    Strips shelter-countdown suffixes like (**מיידי**) or (**30 שניות**).
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
            if stripped and not stripped.startswith("**") and not stripped.startswith("🚨") and not stripped.startswith("✈"):
                for city in stripped.split(","):
                    city = city.strip()
                    # Remove shelter-countdown suffix: (**מיידי**) / (**30 שניות**) etc.
                    city = re.sub(r'\s*\(\*\*.*?\*\*\)', '', city).strip()
                    if city and len(city) > 1:
                        cities.append(city)
            next_is_city_line = False
    return list(dict.fromkeys(cities))  # deduplicate, preserve order


def resolve_shelter(data: dict, end_time: datetime, end_type: str) -> None:
    """
    Match an event-end message to pending shelter entries of the same type.
    Calculates actual duration (min 10 min) and adds to shelter_minutes.
    """
    pending = data.get("pending_shelter", [])
    if not pending:
        return

    resolved = []
    for p in pending:
        start = datetime.fromisoformat(p["start"])
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        if end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)
        diff_min = (end_time - start).total_seconds() / 60

        # Match same alert type within 3 hours
        if 0 < diff_min <= 180 and p.get("type", "rocket") == end_type:
            duration = max(10, int(diff_min))
            for city in p.get("cities", []):
                data["shelter_minutes"][city] = data["shelter_minutes"].get(city, 0) + duration
            resolved.append(p)

    for r in resolved:
        pending.remove(r)
    data["pending_shelter"] = pending


def expire_old_pending(data: dict) -> None:
    """
    Pending alerts older than 2 hours with no matching end → default 10 min shelter.
    """
    now = datetime.now(timezone.utc)
    remaining = []
    for p in data.get("pending_shelter", []):
        start = datetime.fromisoformat(p["start"])
        if start.tzinfo is None:
            start = start.replace(tzinfo=timezone.utc)
        diff_min = (now - start).total_seconds() / 60
        if diff_min > 120:
            for city in p.get("cities", []):
                data["shelter_minutes"][city] = data["shelter_minutes"].get(city, 0) + 10
        else:
            remaining.append(p)
    data["pending_shelter"] = remaining


async def collect() -> None:
    data = load_data()
    last_id = data.get("last_message_id", 0)
    print(f"Starting — last_message_id={last_id}")

    # Expire old unresolved pending shelter entries
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
        snippet = (msg.text or "")[:80].replace("\n", " ")
        print(f"  Sample msg id={msg.id}: {repr(snippet)}")

    if not messages:
        print("No new messages since last run")
        data["last_updated"] = datetime.now(timezone.utc).isoformat()
        save_data(data)
        return

    print(f"Fetched {len(messages)} new messages (since id={last_id})")
    new_alerts = 0

    for msg in reversed(messages):  # oldest first
        text = msg.text or ""
        data["last_message_id"] = max(data["last_message_id"], msg.id)

        # ── Event-end message → resolve pending shelter ──────────────────────
        end_type = event_end_type(text)
        if end_type:
            msg_time = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
            resolve_shelter(data, msg_time, end_type)
            continue

        # ── Regular alert ────────────────────────────────────────────────────
        if not is_alert(text):
            continue

        regions = extract_regions(text)
        if not regions:
            continue

        cities = extract_cities(text)
        date_str = msg.date.astimezone(timezone.utc).strftime("%Y-%m-%d")
        a_type = "drone" if is_drone_alert(text) else "rocket"

        data["total_alerts"] += 1
        new_alerts += 1

        for region in regions:
            data["regions"][region] = data["regions"].get(region, 0) + 1
            data["daily"].setdefault(date_str, {})
            data["daily"][date_str][region] = data["daily"][date_str].get(region, 0) + 1

        for city in cities:
            data["cities"][city] = data["cities"].get(city, 0) + 1

        # Drone-specific tracking
        if a_type == "drone":
            data["drone_alerts"] = data.get("drone_alerts", 0) + 1
            for region in regions:
                data["drone_regions"][region] = data["drone_regions"].get(region, 0) + 1
            for city in cities:
                data["drone_cities"][city] = data["drone_cities"].get(city, 0) + 1

        # Add to pending shelter (resolved when matching end message arrives)
        msg_time = msg.date if msg.date.tzinfo else msg.date.replace(tzinfo=timezone.utc)
        data["pending_shelter"].append({
            "start": msg_time.isoformat(),
            "cities": cities,
            "type": a_type,
        })

        # Keep latest 100 alerts in recent[]
        data["recent"].insert(0, {
            "id": msg.id,
            "date": msg.date.isoformat(),
            "text": text[:300],
            "regions": regions,
            "cities": cities,
            "type": a_type,
        })

    data["recent"] = data["recent"][:100]
    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    save_data(data)
    print(f"Done — {new_alerts} new alerts, last_message_id={data['last_message_id']}")


if __name__ == "__main__":
    asyncio.run(collect())
