"""Разовый прогон пропущенных сообщений из MAX в Telegram.

Переиспользует форматирование и отправку из pymax_main.py, прогоняя историю чатов
через тот же handle_max_message.

Запуск:  .venv/bin/python backfill.py <часы> [--dry]
"""

import asyncio
import sys
import time

import pymax_main as mb


def parse_args():
    hours = 48.0
    dry = False
    for arg in sys.argv[1:]:
        if arg == "--dry":
            dry = True
        else:
            try:
                hours = float(arg)
            except ValueError:
                pass
    return hours, dry


async def main() -> None:
    hours, dry = parse_args()
    since_ms = int((time.time() - hours * 3600) * 1000)

    await mb.client.connect(mb.client.user_agent)
    await mb.client._sync(mb.client.user_agent)

    me_id = mb.client.me.id if mb.client.me else None
    print("ME_ID:", me_id, "WINDOW_H:", hours, "DRY:", dry)

    planned = []
    for max_chat_id, tg_id in mb.CHATS.items():
        try:
            history = await mb.client.fetch_history(
                chat_id=max_chat_id,
                from_time=int(time.time() * 1000),
                forward=0,
                backward=100,
            )
        except Exception as exc:
            print("HISTORY_ERR", max_chat_id, type(exc).__name__, str(exc)[:120])
            continue

        pending = []
        for msg in history or []:
            if msg.time < since_ms:
                continue
            if me_id is not None and msg.sender == me_id:
                continue
            if not (msg.text or "").strip() and not msg.attaches and not msg.link:
                continue
            pending.append(msg)

        pending.sort(key=lambda m: m.time)
        planned.append((max_chat_id, tg_id, pending))
        print("CHAT", max_chat_id, "-> TG", tg_id, "pending:", len(pending))

    if dry:
        for max_chat_id, tg_id, pending in planned:
            for msg in pending:
                stamp = time.strftime("%m-%d %H:%M", time.localtime(msg.time / 1000))
                body = (msg.text or "").replace("\n", " ")
                print("  ", stamp, "att=", len(msg.attaches or []),
                      "link=", "Y" if msg.link else "-", "::", body[:70])
        await mb.client.close()
        return

    forwarded = 0
    failed = 0
    for max_chat_id, tg_id, pending in planned:
        for msg in pending:
            msg.chat_id = max_chat_id
            try:
                await mb.handle_max_message(msg)
                forwarded += 1
                print("FORWARDED", max_chat_id, msg.id)
            except Exception as exc:
                failed += 1
                print("FORWARD_ERR", max_chat_id, msg.id, type(exc).__name__, str(exc)[:120])
            await asyncio.sleep(1.0)

    print("DONE forwarded=", forwarded, "failed=", failed)
    await mb.client.close()


if __name__ == "__main__":
    asyncio.run(main())
