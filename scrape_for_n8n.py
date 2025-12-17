import asyncio
import json
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

from telethon import TelegramClient
from telethon.errors import UsernameInvalidError, UsernameNotOccupiedError, ChannelPrivateError
from telethon.tl.custom.message import Message


def parse_chat_identifier(raw_identifier: str, topic_id_env=None):
    """
    Supports plain usernames/IDs or full t.me links.
    If a message/topic id is present in the URL, use it as topic_id (thread).
    """
    topic_id = topic_id_env
    identifier = raw_identifier.strip()

    if identifier.startswith(("http://", "https://")):
        parsed = urlparse(identifier)
        parts = [p for p in parsed.path.split("/") if p]
        if parts:
            identifier = parts[0]  # username or channel slug
            if len(parts) >= 2 and topic_id is None:
                try:
                    topic_id = int(parts[1])
                except ValueError:
                    pass

    return identifier, topic_id


async def scrape(args):
    chat_identifier, topic_id = parse_chat_identifier(args.chat, args.topic_id)

    client = TelegramClient(args.session, args.api_id, args.api_hash)

    phone_or_token = args.phone_or_token
    if phone_or_token:
        await client.start(phone=phone_or_token)
    else:
        await client.start()

    try:
        entity = await client.get_entity(chat_identifier)
    except (UsernameInvalidError, UsernameNotOccupiedError, ChannelPrivateError) as e:
        await client.disconnect()
        raise RuntimeError(f"Unable to access chat {chat_identifier!r}: {e}")

    now_utc = datetime.now(timezone.utc)
    cutoff = now_utc - timedelta(days=args.days)

    iter_kwargs = {}
    if topic_id is not None:
        iter_kwargs["reply_to"] = topic_id

    rows = []
    async for msg in client.iter_messages(entity, reverse=False, **iter_kwargs):
        if not isinstance(msg, Message):
            continue
        if msg.date is None:
            continue
        if msg.date < cutoff:
            break

        rows.append(
            {
                "chat_identifier": chat_identifier,
                "message_id": msg.id,
                "date": msg.date.isoformat(),
                "sender_id": msg.sender_id if hasattr(msg, "sender_id") else None,
                "sender_username": getattr(getattr(msg, "sender", None), "username", None),
                "text": msg.message or "",
                "reply_to_msg_id": msg.reply_to_msg_id,
                "is_service": 1 if msg.action is not None else 0,
            }
        )

    await client.disconnect()

    # Emit JSON to stdout for n8n to consume
    json.dump(rows, fp=None or __import__("sys").stdout, ensure_ascii=False)


def main():
    parser = ArgumentParser(description="Scrape Telegram messages (last N days) and emit JSON for n8n.")
    parser.add_argument("--api-id", type=int, required=True, help="Telegram API_ID")
    parser.add_argument("--api-hash", required=True, help="Telegram API_HASH")
    parser.add_argument("--chat", required=True, help="Chat username/ID or t.me link; supports topic id in link")
    parser.add_argument("--days", type=int, default=30, help="Days back to include (default 30)")
    parser.add_argument("--session", default="telethon_session", help="Telethon session name/file")
    parser.add_argument("--topic-id", type=int, default=None, help="Explicit topic/thread id (optional)")
    parser.add_argument("--phone-or-token", default=None, help="Phone number (with country code) or bot token")

    args = parser.parse_args()
    asyncio.run(scrape(args))


if __name__ == "__main__":
    main()

