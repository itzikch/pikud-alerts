"""
One-time script to generate a Telethon StringSession string.

Run locally:
    pip install telethon
    python scripts/gen_session.py

Then add the printed string as the TG_SESSION secret in your GitHub repository:
    Settings → Secrets and variables → Actions → New repository secret
"""
import asyncio

from telethon import TelegramClient
from telethon.sessions import StringSession


async def main() -> None:
    print("=== Telethon StringSession Generator ===\n")
    api_id = int(input("Enter TG_API_ID: ").strip())
    api_hash = input("Enter TG_API_HASH: ").strip()

    print("\nStarting authentication — Telegram will send you a code...")
    async with TelegramClient(StringSession(), api_id, api_hash) as client:
        session_string = client.session.save()

    print("\n" + "=" * 60)
    print("TG_SESSION (copy the entire string below):")
    print("=" * 60)
    print(session_string)
    print("=" * 60)
    print("\nAdd this value as TG_SESSION in:")
    print("GitHub → Settings → Secrets and variables → Actions")


if __name__ == "__main__":
    asyncio.run(main())
