import asyncio
import json
import logging
import os
import tempfile
from io import BytesIO
from pathlib import Path
from typing import Any

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv
from pymax import MaxClient, Message
from pymax.files import File, Photo, Video
from pymax.formatter import ColoredFormatter
from pymax.payloads import UserAgentPayload
from pymax.types import FileAttach, PhotoAttach, VideoAttach


load_dotenv(override=True)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers.clear()
root_handler = logging.StreamHandler()
root_handler.setFormatter(ColoredFormatter())
root_logger.addHandler(root_handler)
logger = logging.getLogger("maxbridge")

MAX_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

DOWNLOAD_HEADERS = {
    "User-Agent": MAX_USER_AGENT,
    "Accept": "*/*",
}


class BridgeMaxClient(MaxClient):
    async def _send_notification_response(self, chat_id: int, message_id: str) -> None:
        logger.debug("Skip MAX message ACK: chat_id=%s message_id=%s", chat_id, message_id)

    async def _handle_message_notifications(self, data: dict[str, Any]) -> None:
        try:
            await super()._handle_message_notifications(data)
        except Exception:
            logger.exception("PyMax не смог обработать входящее MAX-сообщение: %r", data)


PHONE = os.getenv("PHONE")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHATS_RAW = os.getenv("CHATS")

if not PHONE:
    raise RuntimeError("В .env не задан PHONE")
if not BOT_TOKEN:
    raise RuntimeError("В .env не задан BOT_TOKEN")


def parse_chats(value: str | None) -> tuple[dict[int, int], dict[int, int], set[int]]:
    if not value:
        raise RuntimeError("В .env не задан CHATS")

    try:
        raw = json.loads(value)
    except json.JSONDecodeError as error:
        raise RuntimeError("В .env CHATS должен быть валидным JSON (список или объект)") from error

    chats: dict[int, int] = {}
    tg_to_max: set[int] = set()

    def process_item(max_val: Any, tg_val: Any, tg2max_val: Any) -> None:
        try:
            max_id = int(max_val)
        except (TypeError, ValueError) as err:
            raise RuntimeError(f"Некорректный max id: {max_val}") from err
        try:
            tg_id = int(tg_val)
        except (TypeError, ValueError) as err:
            raise RuntimeError(f"Некорректный tg id: {tg_val}") from err

        is_tg2max = False
        if isinstance(tg2max_val, bool):
            is_tg2max = tg2max_val
        elif isinstance(tg2max_val, (int, float)):
            is_tg2max = bool(tg2max_val)
        elif isinstance(tg2max_val, str):
            is_tg2max = tg2max_val.strip().lower() in ("true", "1", "yes")

        chats[max_id] = tg_id
        if is_tg2max:
            tg_to_max.add(tg_id)

    if isinstance(raw, list):
        for item in raw:
            if not isinstance(item, dict):
                raise RuntimeError(f"Элемент CHATS должен быть объектом {{max, tg, tg2max}}: {item}")
            max_val = item.get("max") or item.get("max_id")
            tg_val = item.get("tg") or item.get("tg_id") or item.get("telegram_id")
            tg2max_val = item.get("tg2max", item.get("forward_tg_to_max", False))
            if max_val is None or tg_val is None:
                raise RuntimeError(f"В объекте чата должны быть поля max и tg: {item}")
            process_item(max_val, tg_val, tg2max_val)

    elif isinstance(raw, dict):
        if "max" in raw and "tg" in raw:
            process_item(raw["max"], raw["tg"], raw.get("tg2max", False))
        else:
            for max_key, val in raw.items():
                if isinstance(val, dict):
                    tg_val = val.get("tg") or val.get("tg_id") or val.get("telegram_id")
                    if tg_val is None:
                        raise RuntimeError(f"В настройках чата {max_key} не указан tg: {val}")
                    tg2max_val = val.get("tg2max", val.get("forward_tg_to_max", False))
                    process_item(max_key, tg_val, tg2max_val)
                elif isinstance(val, (list, tuple)) and len(val) >= 1:
                    tg_val = val[0]
                    tg2max_val = val[1] if len(val) > 1 else False
                    process_item(max_key, tg_val, tg2max_val)
                else:
                    process_item(max_key, val, False)
    else:
        raise RuntimeError("CHATS должен быть JSON-списком или JSON-объектом")

    if not chats:
        raise RuntimeError("CHATS не содержит ни одного чата")

    chats_tg = {tg_id: max_id for max_id, tg_id in chats.items()}
    return chats, chats_tg, tg_to_max


CHATS, CHATS_TELEGRAM, TG_TO_MAX_CHATS = parse_chats(CHATS_RAW)

MAX_HEADERS = UserAgentPayload(
    device_type="WEB",
    device_name="Chrome",
    os_version="macOS 10.15.7",
    header_user_agent=MAX_USER_AGENT,
    screen="1440x900 1.0x",
    timezone="Europe/Moscow",
)

client = BridgeMaxClient(phone=PHONE, work_dir="cache", headers=MAX_HEADERS, reconnect=True)
client.logger.propagate = False
telegram_bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()


def build_author_name(user: object | None, fallback: str = "MAX") -> str:
    names = getattr(user, "names", None) or []
    if names:
        name = getattr(names[0], "name", None)
        if name:
            return str(name)
    return fallback


def build_text(author: str, text: str | None) -> str:
    text = (text or "").strip()
    return f"{author}: {text}" if text else f"{author}:"


async def author_name_from_sender(sender: int | None, fallback: str = "MAX") -> str:
    if sender is None:
        return fallback

    try:
        user = await client.get_user(sender)
    except Exception:
        logger.exception("Не удалось получить имя MAX-пользователя %s", sender)
        return fallback

    return build_author_name(user, fallback)


async def resolve_linked_message(message: Message) -> tuple[Message, int | None]:
    current = message
    source_chat_id = message.chat_id

    for _ in range(20):
        if current.link is None or current.link.message is None:
            break
        source_chat_id = current.link.chat_id or source_chat_id
        current = current.link.message

    return current, source_chat_id


def build_caption(author: str, text: str | None) -> str:
    return build_text(author, text)[:1024]


def filename_from_response(response: aiohttp.ClientResponse, fallback: str) -> str:
    return response.headers.get("X-File-Name") or fallback


async def download_bytes(url: str, fallback_name: str) -> tuple[bytes, str]:
    timeout = aiohttp.ClientTimeout(total=90, connect=20, sock_read=60)
    last_error: BaseException | None = None

    for attempt in range(1, 4):
        logger.info("Скачиваю файл из MAX, попытка %s/3: %s", attempt, url)
        try:
            async with aiohttp.ClientSession(timeout=timeout, headers=DOWNLOAD_HEADERS) as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    attach_bytes = BytesIO(await response.read())
                    attach_bytes.name = filename_from_response(response, fallback_name)
                    logger.info(
                        "Файл из MAX скачан: %s, %s байт",
                        attach_bytes.name,
                        len(attach_bytes.getvalue()),
                    )
                    return attach_bytes.getvalue(), attach_bytes.name
        except Exception as error:
            last_error = error
            logger.exception("Не удалось скачать файл из MAX, попытка %s/3: %s", attempt, url)
            if attempt < 3:
                await asyncio.sleep(attempt)

    if last_error is not None:
        raise RuntimeError("Не удалось скачать файл из MAX после 3 попыток") from last_error
    raise RuntimeError("Не удалось скачать файл из MAX после 3 попыток")


async def send_max_attachment_to_telegram(
    tg_id: int,
    max_chat_id: int,
    max_message: Message,
    attach: object,
    caption: str,
) -> None:
    if isinstance(attach, PhotoAttach):
        photo_bytes, filename = await download_bytes(attach.base_url, "photo.jpg")
        logger.info("Отправляю фото в Telegram: %s, %s байт", filename, len(photo_bytes))
        try:
            await telegram_bot.send_photo(
                chat_id=tg_id,
                caption=caption,
                photo=types.BufferedInputFile(photo_bytes, filename=filename),
            )
            logger.info("Фото отправлено в Telegram: %s", filename)
        except TelegramBadRequest:
            logger.exception("Telegram не принял файл как фото, отправляю документом: %s", filename)
            await telegram_bot.send_document(
                chat_id=tg_id,
                caption=caption,
                document=types.BufferedInputFile(photo_bytes, filename=filename),
            )
            logger.info("Фото отправлено в Telegram документом: %s", filename)
        return

    if isinstance(attach, VideoAttach):
        video = await client.get_video_by_id(
            chat_id=max_chat_id,
            message_id=max_message.id,
            video_id=attach.video_id,
        )
        if video is None:
            raise RuntimeError(f"MAX не вернул ссылку на видео {attach.video_id}")

        video_bytes, filename = await download_bytes(video.url, "video.mp4")
        logger.info("Отправляю видео в Telegram: %s, %s байт", filename, len(video_bytes))
        await telegram_bot.send_video(
            chat_id=tg_id,
            caption=caption,
            video=types.BufferedInputFile(video_bytes, filename=filename),
        )
        logger.info("Видео отправлено в Telegram: %s", filename)
        return

    if isinstance(attach, FileAttach):
        file = await client.get_file_by_id(
            chat_id=max_chat_id,
            message_id=max_message.id,
            file_id=attach.file_id,
        )
        if file is None:
            raise RuntimeError(f"MAX не вернул ссылку на файл {attach.file_id}")

        file_bytes, filename = await download_bytes(file.url, attach.name or "file")
        logger.info("Отправляю документ в Telegram: %s, %s байт", filename, len(file_bytes))
        await telegram_bot.send_document(
            chat_id=tg_id,
            caption=caption,
            document=types.BufferedInputFile(file_bytes, filename=filename),
        )
        logger.info("Документ отправлен в Telegram: %s", filename)


@client.on_message()
async def handle_max_message(message: Message) -> None:
    try:
        incoming_chat_id = message.chat_id
        if incoming_chat_id is None:
            return

        tg_id = CHATS.get(incoming_chat_id)
        if tg_id is None:
            return

        max_message, source_chat_id = await resolve_linked_message(message)
        max_chat_id = source_chat_id or incoming_chat_id

        author = await author_name_from_sender(message.sender)
        caption = build_caption(author, max_message.text)

        if max_message.attaches:
            for attach in max_message.attaches:
                await send_max_attachment_to_telegram(
                    tg_id=tg_id,
                    max_chat_id=max_chat_id,
                    max_message=max_message,
                    attach=attach,
                    caption=caption,
                )
            return

        await telegram_bot.send_message(
            chat_id=tg_id,
            text=build_text(author, max_message.text),
        )
    except aiohttp.ClientError:
        logger.exception("Не удалось скачать вложение из MAX")
    except Exception:
        logger.exception("Не удалось переслать сообщение из MAX в Telegram")


async def telegram_file_bytes(bot: Bot, file_id: str) -> bytes:
    telegram_file = await bot.get_file(file_id)
    if telegram_file.file_path is None:
        raise RuntimeError("Telegram не вернул путь к файлу")
    buffer = await bot.download_file(telegram_file.file_path)
    if buffer is None:
        raise RuntimeError("Telegram не скачал файл")
    return buffer.read()


async def send_telegram_attachment_to_max(
    bot: Bot,
    max_id: int,
    text: str,
    message: types.Message,
) -> bool:
    if message.photo:
        photo = message.photo[-1]
        raw = await telegram_file_bytes(bot, photo.file_id)
        await client.send_message(
            chat_id=max_id,
            text=text,
            attachment=Photo(raw=raw, path="telegram_photo.jpg"),
            notify=True,
        )
        return True

    if message.video:
        raw = await telegram_file_bytes(bot, message.video.file_id)
        await client.send_message(
            chat_id=max_id,
            text=text,
            attachment=Video(raw=raw, path=message.video.file_name or "telegram_video.mp4"),
            notify=True,
        )
        return True

    if message.document:
        raw = await telegram_file_bytes(bot, message.document.file_id)
        filename = message.document.file_name or "telegram_file"
        upload_dir = Path(tempfile.gettempdir()) / "maxbridge_uploads"
        upload_dir.mkdir(parents=True, exist_ok=True)
        upload_path = upload_dir / Path(filename).name
        upload_path.write_bytes(raw)
        try:
            await client.send_message(
                chat_id=max_id,
                text=text,
                attachment=File(path=str(upload_path)),
                notify=True,
            )
        finally:
            upload_path.unlink(missing_ok=True)
        return True

    return False


@dp.message()
async def handle_telegram_message(message: types.Message, bot: Bot) -> None:
    max_id = CHATS_TELEGRAM.get(message.chat.id)
    if max_id is None:
        return

    if message.chat.id not in TG_TO_MAX_CHATS:
        return

    if message.from_user is None:
        return

    signature = message.from_user.first_name
    try:
        member = await bot.get_chat_member(chat_id=message.chat.id, user_id=message.from_user.id)
        custom_title = getattr(member, "custom_title", None)
        if custom_title:
            signature = f"{member.user.first_name} ({custom_title})"
    except Exception:
        pass

    text = build_text(signature, message.text or message.caption)

    try:
        sent_attachment = await send_telegram_attachment_to_max(bot, max_id, text, message)
        if not sent_attachment:
            await client.send_message(chat_id=max_id, text=text, notify=True)
    except Exception:
        logger.exception("Не удалось переслать сообщение из Telegram в MAX")


async def main() -> None:
    logger.info("Запуск maxbridge. Сконфигурировано связей чатов: %d", len(CHATS))
    for max_id, tg_id in CHATS.items():
        tg2max = tg_id in TG_TO_MAX_CHATS
        logger.info(
            "  • MAX %s <-> TG %s | MAX->TG: да | TG->MAX (tg2max): %s",
            max_id,
            tg_id,
            "ДА" if tg2max else "нет",
        )

    telegram_task = asyncio.create_task(dp.start_polling(telegram_bot))

    try:
        await client.start()
    finally:
        telegram_task.cancel()
        await client.close()
        await telegram_bot.session.close()
        try:
            await telegram_task
        except asyncio.CancelledError:
            pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Программа остановлена пользователем.")
