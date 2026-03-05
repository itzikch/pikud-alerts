"""
One-time backfill: fetch ALL alerts from a start date and rebuild docs/data.json.

Usage:
    BACKFILL_FROM=2026-02-27 python scripts/backfill.py

Environment variables:
    TG_API_ID, TG_API_HASH, TG_SESSION  — same as collect.py
    BACKFILL_FROM                        — start date (YYYY-MM-DD), default 2026-02-27
"""
import asyncio
import json
import os
from datetime import datetime, timezone
from pathlib import Path

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession

API_ID = int(os.environ["TG_API_ID"])
API_HASH = os.environ["TG_API_HASH"]
SESSION = os.environ["TG_SESSION"]
BACKFILL_FROM = os.environ.get("BACKFILL_FROM", "2026-02-27")

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


def is_alert(text: str) -> bool:
    return bool(text) and any(kw in text for kw in ALERT_KEYWORDS)


def extract_regions(text: str) -> list[str]:
    return [r for r in REGIONS if r in text]


async def backfill() -> None:
    start_date = datetime.strptime(BACKFILL_FROM, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    print(f"Backfilling from {start_date.date()} ...")

    # Start with a completely fresh dataset
    data: dict = {
        "last_updated": None,
        "last_message_id": 0,
        "total_alerts": 0,
        "regions": {r: 0 for r in REGIONS},
        "daily": {},
        "recent": [],
    }

    async with TelegramClient(StringSession(SESSION), API_ID, API_HASH) as client:
        try:
            entity = await client.get_entity(CHANNEL)
            print(f"Channel resolved: title='{entity.title}', id={entity.id}")
        except Exception as e:
            print(f"ERROR resolving channel '{CHANNEL}': {e}")
            return

        print("Fetching messages (this may take a while for large date ranges)...")
        messages = []
        try:
            async for msg in client.iter_messages(
                entity,
                reverse=True,       # oldest → newest
                offset_date=start_date,  # start from this date going forward
                limit=None,         # no cap — fetch everything
            ):
                messages.append(msg)
                if len(messages) % 500 == 0:
                    print(f"  ... {len(messages)} messages fetched so far")
        except FloodWaitError as e:
            wait = min(e.seconds, 120)
            print(f"FloodWaitError: sleeping {wait}s then continuing")
            await asyncio.sleep(wait)
            # The messages collected so far are still usable
        except Exception as e:
            print(f"ERROR fetching messages: {e}")
            if not messages:
                return

    print(f"Total messages fetched: {len(messages)}")

    new_alerts = 0
    for msg in messages:  # already chronological (oldest first) thanks to reverse=True
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

        # Insert at front so recent[] is newest-first
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

    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(
        f"Done — {new_alerts} alerts saved, "
        f"last_message_id={data['last_message_id']}, "
        f"days covered={len(data['daily'])}"
    )


if __name__ == "__main__":
    asyncio.run(backfill())
