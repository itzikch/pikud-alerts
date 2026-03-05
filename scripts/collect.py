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

CHANNEL = "@pikud_haoref"
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
            return json.load(f)
    return {
        "last_updated": None,
        "last_message_id": 0,
        "total_alerts": 0,
        "regions": {r: 0 for r in REGIONS},
        "daily": {},
        "recent": [],
    }


def save_data(data: dict) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def is_alert(text: str) -> bool:
    return bool(text) and any(kw in text for kw in ALERT_KEYWORDS)


def extract_regions(text: str) -> list[str]:
    return [r for r in REGIONS if r in text]


async def collect() -> None:
    data = load_data()
    last_id = data.get("last_message_id", 0)
    print(f"Starting — last_message_id={last_id}")

    async with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as client:
        # Resolve and confirm channel
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

    # Print sample of last 3 messages for debugging
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

    for msg in reversed(messages):  # process oldest first
        text = msg.text or ""
        data["last_message_id"] = max(data["last_message_id"], msg.id)

        if not is_alert(text):
            continue

        regions = extract_regions(text)
        if not regions:
            continue

        date_str = msg.date.astimezone(timezone.utc).strftime("%Y-%m-%d")
        data["total_alerts"] += 1
        new_alerts += 1

        for region in regions:
            data["regions"][region] = data["regions"].get(region, 0) + 1
            data["daily"].setdefault(date_str, {})
            data["daily"][date_str][region] = (
                data["daily"][date_str].get(region, 0) + 1
            )

        # Keep latest 100 alerts
        data["recent"].insert(
            0,
            {
                "id": msg.id,
                "date": msg.date.isoformat(),
                "text": text[:300],
                "regions": regions,
            },
        )

    data["recent"] = data["recent"][:100]
    data["last_updated"] = datetime.now(timezone.utc).isoformat()
    save_data(data)
    print(
        f"Done — {new_alerts} new alerts, last_message_id={data['last_message_id']}"
    )


if __name__ == "__main__":
    asyncio.run(collect())
