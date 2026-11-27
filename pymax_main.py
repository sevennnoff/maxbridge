import asyncio
import json
import logging
import os
import tempfile
import time
import urllib.parse
from hashlib import sha256
from io import BytesIO
from pathlib import Path
from typing import Any

import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.exceptions import TelegramBadRequest, TelegramEntityTooLarge
from aiogram.filters import Command
from dotenv import load_dotenv
from pymax import MaxClient, Message
from pymax.files import File, Photo, Video
from pymax.formatter import ColoredFormatter
from pymax.payloads import UserAgentPayload
from max_transport import BridgeTransportClient
from message_edits import Delivery, ForwardStore
from telegram_retry import retry_chat_request
from pymax.types import (
    AudioAttach,
    FileAttach,
    PhotoAttach,
    StickerAttach,
    VideoAttach,
)


load_dotenv(override=True)

START_TIME = time.time()

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

MAX_RECONNECT_MIN_DELAY = 5.0
MAX_RECONNECT_MAX_DELAY = 300.0
MAX_RECONNECT_STABLE_SECONDS = 60.0


class BridgeMaxClient(BridgeTransportClient):
    token_invalid: bool = False

    async def _send_notification_response(self, chat_id: int, message_id: str) -> None:
        logger.debug("Skip MAX message ACK: chat_id=%s message_id=%s", chat_id, message_id)

    async def _handle_message_notifications(self, data: dict[str, Any]) -> None:
        try:
            await super()._handle_message_notifications(data)
        except Exception:
            logger.exception("PyMax не смог обработать входящее MAX-сообщение: %r", data)

    async def _sync(self, user_agent: Any = None) -> None:
        try:
            await super()._sync(user_agent)
        except Exception as error:
            text = str(error)
            if "FAIL_LOGIN_TOKEN" in text or "login.token" in text:
                self.token_invalid = True
            raise


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

max_ws_uri = os.getenv("MAX_WS_URI", "").strip()

client = BridgeMaxClient(
    **({"uri": max_ws_uri} if max_ws_uri else {}),
    phone=PHONE,
    work_dir="cache",
    headers=MAX_HEADERS,
    reconnect=False,
    reconnect_delay=MAX_RECONNECT_MIN_DELAY,
)
client.logger.propagate = False
telegram_bot = Bot(token=BOT_TOKEN)
telegram_bot.session.middleware(retry_chat_request)
dp = Dispatcher()
forward_store = ForwardStore(Path("cache/forwarded.sqlite3"))
message_locks = tuple(asyncio.Lock() for _ in range(128))


def format_uptime(seconds: float) -> str:
    seconds = int(seconds)
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)
    parts = []
    if days:
        parts.append(f"{days}д")
    if hours:
        parts.append(f"{hours}ч")
    if minutes:
        parts.append(f"{minutes}м")
    parts.append(f"{seconds}с")
    return " ".join(parts)


def format_preview(text: str | None, max_len: int = 60) -> str:
    cleaned = " ".join((text or "").split())
    if len(cleaned) > max_len:
        return cleaned[:max_len].rstrip() + "..."
    return cleaned


def build_author_name(user: object | None, fallback: str = "MAX") -> str:
    names = getattr(user, "names", None) or []
    if names:
        name = getattr(names[0], "name", None)
        if name:
            return str(name)
    return fallback


def build_text(author: str, text: str | None, reply_header: str = "") -> str:
    text = (text or "").strip()
    if reply_header:
        body = (reply_header + "\n" + text).strip() if text else reply_header.strip()
        return f"{author}:\n{body}"
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


def filename_from_response(response: aiohttp.ClientResponse, fallback: str) -> str:
    raw = response.headers.get("X-File-Name")
    if not raw:
        return fallback
    return urllib.parse.unquote(raw)


async def download_bytes(url: str, fallback_name: str) -> tuple[bytes, str]:
    timeout = aiohttp.ClientTimeout(total=120, connect=20, sock_read=90)
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


async def fetch_file_download_url(
    max_client: MaxClient,
    candidates: list[tuple[int, int]],
    file_id: int,
) -> Any:
    last_error: BaseException | None = None
    seen: set[tuple[int, int]] = set()
    unique_candidates: list[tuple[int, int]] = []
    for c_id, m_id in candidates:
        if not c_id or not m_id:
            continue
        key = (int(c_id), int(m_id))
        if key not in seen:
            seen.add(key)
            unique_candidates.append(key)

    for c_id, m_id in unique_candidates:
        try:
            logger.info("Запрос ссылки на file_id=%s (chat_id=%s, message_id=%s)", file_id, c_id, m_id)
            file_res = await max_client.get_file_by_id(chat_id=c_id, message_id=m_id, file_id=file_id)
            if file_res and getattr(file_res, "url", None):
                return file_res
        except Exception as err:
            logger.warning("Не удалось получить ссылку на file_id=%s через chat_id=%s message_id=%s: %s", file_id, c_id, m_id, err)
            last_error = err

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"MAX не вернул ссылку на файл {file_id}")


async def fetch_video_download_url(
    max_client: MaxClient,
    candidates: list[tuple[int, int]],
    video_id: int,
) -> Any:
    last_error: BaseException | None = None
    seen: set[tuple[int, int]] = set()
    unique_candidates: list[tuple[int, int]] = []
    for c_id, m_id in candidates:
        if not c_id or not m_id:
            continue
        key = (int(c_id), int(m_id))
        if key not in seen:
            seen.add(key)
            unique_candidates.append(key)

    for c_id, m_id in unique_candidates:
        try:
            logger.info("Запрос ссылки на video_id=%s (chat_id=%s, message_id=%s)", video_id, c_id, m_id)
            video_res = await max_client.get_video_by_id(chat_id=c_id, message_id=m_id, video_id=video_id)
            if video_res and getattr(video_res, "url", None):
                return video_res
        except Exception as err:
            logger.warning("Не удалось получить ссылку на video_id=%s через chat_id=%s message_id=%s: %s", video_id, c_id, m_id, err)
            last_error = err

    if last_error is not None:
        raise last_error
    raise RuntimeError(f"MAX не вернул ссылку на видео {video_id}")


async def send_max_attachment_to_telegram(
    tg_id: int,
    candidates: list[tuple[int, int]],
    attach: object,
    caption: str | None,
) -> types.Message | None:
    try:
        if isinstance(attach, PhotoAttach):
            photo_bytes, filename = await download_bytes(attach.base_url, "photo.jpg")
            filename = urllib.parse.unquote(filename)
            logger.info("Отправляю фото в Telegram: %s, %s байт", filename, len(photo_bytes))
            try:
                sent = await telegram_bot.send_photo(
                    chat_id=tg_id,
                    caption=caption or None,
                    photo=types.BufferedInputFile(photo_bytes, filename=filename),
                )
                logger.info("Фото отправлено в Telegram: %s", filename)
            except TelegramBadRequest:
                logger.exception("Telegram не принял файл как фото, отправляю документом: %s", filename)
                sent = await telegram_bot.send_document(
                    chat_id=tg_id,
                    caption=caption or None,
                    document=types.BufferedInputFile(photo_bytes, filename=filename),
                )
                logger.info("Фото отправлено в Telegram документом: %s", filename)
            return sent

        if isinstance(attach, VideoAttach):
            video = await fetch_video_download_url(client, candidates, attach.video_id)
            video_bytes, filename = await download_bytes(video.url, "video.mp4")
            filename = urllib.parse.unquote(filename)
            logger.info("Отправляю видео в Telegram: %s, %s байт", filename, len(video_bytes))
            try:
                sent = await telegram_bot.send_video(
                    chat_id=tg_id,
                    caption=caption or None,
                    video=types.BufferedInputFile(video_bytes, filename=filename),
                )
                logger.info("Видео отправлено в Telegram: %s", filename)
            except TelegramBadRequest:
                logger.exception("Telegram не принял как видео, отправляю документом: %s", filename)
                sent = await telegram_bot.send_document(
                    chat_id=tg_id,
                    caption=caption or None,
                    document=types.BufferedInputFile(video_bytes, filename=filename),
                )
            return sent

        if isinstance(attach, FileAttach):
            file = await fetch_file_download_url(client, candidates, attach.file_id)
            fallback_name = urllib.parse.unquote(attach.name or "file")
            file_bytes, filename = await download_bytes(file.url, fallback_name)
            filename = urllib.parse.unquote(filename)
            logger.info("Отправляю документ в Telegram: %s, %s байт", filename, len(file_bytes))
            sent = await telegram_bot.send_document(
                chat_id=tg_id,
                caption=caption or None,
                document=types.BufferedInputFile(file_bytes, filename=filename),
            )
            logger.info("Документ отправлен в Telegram: %s", filename)
            return sent

        if isinstance(attach, AudioAttach):
            audio_bytes, filename = await download_bytes(attach.url, "voice.ogg")
            filename = urllib.parse.unquote(filename)
            try:
                sent = await telegram_bot.send_voice(
                    chat_id=tg_id,
                    caption=caption or None,
                    voice=types.BufferedInputFile(audio_bytes, filename=filename),
                )
            except TelegramBadRequest:
                sent = await telegram_bot.send_audio(
                    chat_id=tg_id,
                    caption=caption or None,
                    audio=types.BufferedInputFile(audio_bytes, filename=filename),
                )
            return sent

        if isinstance(attach, StickerAttach):
            sticker_bytes, filename = await download_bytes(attach.url, "sticker.webp")
            filename = urllib.parse.unquote(filename)
            sent = await telegram_bot.send_document(
                chat_id=tg_id,
                caption=caption or None,
                document=types.BufferedInputFile(sticker_bytes, filename=filename),
            )
            return sent

        logger.warning("Пропущен неизвестный тип вложения: %r", attach)
        return None

    except (TelegramEntityTooLarge, TelegramBadRequest) as err:
        logger.exception("Telegram отклонил вложение %r: %s", attach, err)
        raw_name = getattr(attach, "name", None) or getattr(attach, "file_id", "файл")
        name = urllib.parse.unquote(str(raw_name))
        try:
            await telegram_bot.send_message(
                chat_id=tg_id,
                text=f"⚠️ Не удалось отправить вложение из MAX в TG (слишком большой файл или неподдерживаемый формат): {name}",
            )
        except Exception:
            pass
        return None
    except Exception as err:
        logger.exception("Ошибка при обработке/отправке вложения %r: %s", attach, err)
        raw_name = getattr(attach, "name", None) or getattr(attach, "file_id", "файл")
        name = urllib.parse.unquote(str(raw_name))
        try:
            await telegram_bot.send_message(
                chat_id=tg_id,
                text=f"⚠️ Не удалось скачать/переслать вложение из MAX: {name}",
            )
        except Exception:
            pass
        return None


async def try_send_media_group(
    tg_id: int,
    media_items: list[tuple[str, bytes, str]],  # (kind, data, filename)
    caption: str | None,
) -> types.Message | None:
    try:
        group: list[types.InputMediaPhoto | types.InputMediaVideo] = []
        for idx, (kind, data, filename) in enumerate(media_items):
            file_obj = types.BufferedInputFile(data, filename=filename)
            item_caption = caption if idx == 0 else None
            if kind == "photo":
                group.append(types.InputMediaPhoto(media=file_obj, caption=item_caption))
            elif kind == "video":
                group.append(types.InputMediaVideo(media=file_obj, caption=item_caption))
        if group:
            sent = await telegram_bot.send_media_group(chat_id=tg_id, media=group)
            return sent[0] if sent else None
    except Exception:
        logger.exception("Не удалось отправить media group в Telegram")
    return None


async def render_max_message(
    message: Message, incoming_chat_id: int,
) -> tuple[str, bool, list[tuple[Any, list[tuple[int, int]]]]]:
    author = await author_name_from_sender(message.sender)

    items_to_send: list[tuple[Any, list[tuple[int, int]]]] = []

    # 1. Direct attachments on root message
    if message.attaches:
        for att in message.attaches:
            items_to_send.append((att, [(incoming_chat_id, message.id)]))

    reply_header = ""
    forward_info = ""

    # 2. Inspect links (replies / forwards)
    curr_link = message.link
    depth = 0
    while curr_link and curr_link.message and depth < 10:
        depth += 1
        l_msg = curr_link.message
        l_chat_id = curr_link.chat_id
        link_type = (curr_link.type or "").upper()

        att_candidates = [
            (incoming_chat_id, message.id),
            (l_chat_id, l_msg.id) if l_chat_id and l_msg.id else None,
            (incoming_chat_id, l_msg.id) if incoming_chat_id and l_msg.id else None,
            (l_chat_id, message.id) if l_chat_id and message.id else None,
        ]
        valid_candidates = [c for c in att_candidates if c is not None]

        is_explicit_reply = "REPLY" in link_type
        has_own_content = bool(message.text and message.text.strip()) or bool(message.attaches)

        # Collect attaches if:
        # - it is a FORWARD, OR
        # - the root message is just an empty forward/share wrapper (no own text and no own attaches)
        if not is_explicit_reply or not has_own_content:
            if l_msg.attaches:
                for att in l_msg.attaches:
                    items_to_send.append((att, valid_candidates))

        # Header formatting
        if is_explicit_reply and depth == 1 and has_own_content:
            reply_author = await author_name_from_sender(l_msg.sender, fallback="сообщение")
            # Clean and collapse multi-line/nested text into a short preview (max 60 chars)
            preview = format_preview(l_msg.text, max_len=60)
            if preview:
                reply_header = f"💬 В ответ на {reply_author}: «{preview}»"
            else:
                reply_header = f"💬 В ответ на {reply_author}"
        elif not is_explicit_reply:
            # Only include forward_info if this is an actual forward, NOT a reply
            fwd_author = await author_name_from_sender(l_msg.sender, fallback="сообщение")
            l_text = (l_msg.text or "").strip()
            if not forward_info:
                if l_text:
                    forward_info = f"↪️ Переслано от {fwd_author}:\n{l_text}"
                else:
                    forward_info = f"↪️ Переслано от {fwd_author}"

        curr_link = l_msg.link

    # Build composite text
    my_text = (message.text or "").strip()
    text_parts = []
    if my_text:
        text_parts.append(my_text)
    if forward_info:
        text_parts.append(forward_info)

    final_content = "\n\n".join(text_parts) if text_parts else None
    return build_text(author, final_content, reply_header), bool(final_content or reply_header), items_to_send


def content_hash(text: str) -> str:
    return sha256(text.encode()).hexdigest()


def remember_delivery(key: tuple[int, str], sent: types.Message, text: str, caption: bool) -> None:
    forward_store.record(key, Delivery(
        sent.chat.id, sent.message_id,
        sent.message_thread_id if sent.is_topic_message else None,
        caption, content_hash(text),
    ))


@client.on_message()
async def handle_max_message(message: Message) -> None:
    if message.chat_id is None or not message.id:
        return
    tg_id = CHATS.get(message.chat_id)
    if tg_id is None:
        return
    key = (message.chat_id, str(message.id))
    async with message_locks[hash(key) % len(message_locks)]:
        await forward_max_message(message, key, tg_id)


async def forward_max_message(message: Message, key: tuple[int, str], tg_id: int) -> None:
    try:
        if forward_store.get(key) is not None:
            return
        text, has_text, items_to_send = await render_max_message(message, key[0])
        caption = text[:1024]

        if items_to_send:
            # If all attachments are photos/videos (between 2 and 10), attempt to group them as an album
            all_media = all(isinstance(att, (PhotoAttach, VideoAttach)) for att, _ in items_to_send)
            if all_media and 2 <= len(items_to_send) <= 10:
                try:
                    downloaded_media: list[tuple[str, bytes, str]] = []
                    for att, cand in items_to_send:
                        if isinstance(att, PhotoAttach):
                            p_bytes, f_name = await download_bytes(att.base_url, "photo.jpg")
                            downloaded_media.append(("photo", p_bytes, urllib.parse.unquote(f_name)))
                        elif isinstance(att, VideoAttach):
                            v_obj = await fetch_video_download_url(client, cand, att.video_id)
                            v_bytes, f_name = await download_bytes(v_obj.url, "video.mp4")
                            downloaded_media.append(("video", v_bytes, urllib.parse.unquote(f_name)))

                    album_message = await try_send_media_group(tg_id, downloaded_media, caption)
                except Exception:
                    logger.exception("Media group download/send failed, fallback to sequential sending")
                else:
                    if album_message:
                        remember_delivery(key, album_message, caption, caption=True)
                        return

            # Sequential sending
            sent_caption = False
            for attach, candidates in items_to_send:
                current_caption = caption if not sent_caption else None
                sent = await send_max_attachment_to_telegram(
                    tg_id=tg_id,
                    candidates=candidates,
                    attach=attach,
                    caption=current_caption,
                )
                if sent and not sent_caption:
                    remember_delivery(key, sent, caption, caption=True)
                    sent_caption = True
                if len(items_to_send) > 1:
                    await asyncio.sleep(0.3)

            # If attachments were sent but NONE of them could take the caption (or all attachments failed), send text separately
            if not sent_caption and has_text:
                sent = await telegram_bot.send_message(chat_id=tg_id, text=text)
                remember_delivery(key, sent, text, caption=False)
            return

        sent = await telegram_bot.send_message(chat_id=tg_id, text=text)
        remember_delivery(key, sent, text, caption=False)
    except aiohttp.ClientError:
        logger.exception("Не удалось скачать вложение из MAX")
    except Exception:
        logger.exception("Не удалось переслать сообщение из MAX в Telegram")


@client.on_message_edit()
async def handle_max_edit(message: Message) -> None:
    if message.chat_id is None or not message.id:
        return
    key = (message.chat_id, str(message.id))
    async with message_locks[hash(key) % len(message_locks)]:
        try:
            delivery = forward_store.get(key)
            if delivery is None:
                return
            text, _, _ = await render_max_message(message, key[0])
            text = text[:1024 if delivery.caption else 4096]
            digest = content_hash(text)
            if digest == delivery.content_hash:
                return
            target = dict(chat_id=delivery.chat_id, message_id=delivery.message_id)
            try:
                if delivery.caption:
                    await telegram_bot.edit_message_caption(**target, caption=text)
                else:
                    await telegram_bot.edit_message_text(**target, text=text)
            except TelegramBadRequest as error:
                # An earlier attempt may have edited the copy before its notification failed.
                if "message is not modified" not in error.message.lower():
                    raise
            await telegram_bot.send_message(
                chat_id=delivery.chat_id,
                message_thread_id=delivery.thread_id,
                text="Сообщение изменено в MAX.",
                reply_parameters=types.ReplyParameters(
                    message_id=delivery.message_id, allow_sending_without_reply=False,
                ),
            )
            forward_store.mark_notified(key, digest)
        except Exception:
            logger.exception("Не удалось применить правку MAX: chat=%s message=%s", *key)


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


@dp.message(Command("status"))
async def handle_status_command(message: types.Message) -> None:
    current_tg_id = message.chat.id
    linked_max_id = CHATS_TELEGRAM.get(current_tg_id)
    tg2max_active = current_tg_id in TG_TO_MAX_CHATS

    chat_title = message.chat.title or message.chat.full_name or message.chat.type

    is_conn = getattr(client, "is_connected", False)
    conn_status = "🟢 Подключен к MAX" if is_conn else "🔴 Не подключен к MAX"
    uptime_str = format_uptime(time.time() - START_TIME)

    lines = [
        "📊 <b>Статус MaxBridge</b>",
        "",
        f"<b>Статус шлюза:</b> {conn_status}",
        f"<b>Аптайм процесса:</b> <code>{uptime_str}</code>",
        "",
        "<b>📍 Текущий чат:</b>",
        f"• Название: <i>{chat_title}</i>",
        f"• TG ID: <code>{current_tg_id}</code>",
    ]

    if linked_max_id is not None:
        lines.append(f"• MAX ID: <code>{linked_max_id}</code>")
        lines.append("• MAX ➔ TG: ✅ Включено")
        lines.append(f"• TG ➔ MAX: {'✅ Включено' if tg2max_active else '❌ Выключено'}")
    else:
        lines.append("• Связка: ⚠️ <i>Этот чат не настроен в CHATS</i>")

    lines.append("")
    lines.append(f"<b>⚙️ Всего настроено чатов:</b> {len(CHATS)}")

    for idx, (max_id, tg_id) in enumerate(CHATS.items(), 1):
        t2m = "✅" if tg_id in TG_TO_MAX_CHATS else "❌"
        is_current = " (👉 этот чат)" if tg_id == current_tg_id else ""
        lines.append(f"{idx}. MAX <code>{max_id}</code> ➔ TG <code>{tg_id}</code> [TG2MAX: {t2m}]{is_current}")

    await message.reply("\n".join(lines), parse_mode="HTML")


@dp.message()
async def handle_telegram_message(message: types.Message, bot: Bot) -> None:
    # Ignore bot commands
    if message.text and message.text.startswith("/"):
        return

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


async def run_max_client_forever() -> None:
    delay = MAX_RECONNECT_MIN_DELAY

    while not client.token_invalid:
        attempt_started = time.monotonic()
        try:
            await client.start()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("MAX клиент упал с неожиданной ошибкой")

        if client.token_invalid:
            logger.critical(
                "Токен MAX недействителен (FAIL_LOGIN_TOKEN). "
                "Автопереподключение остановлено — требуется повторная авторизация."
            )
            break

        uptime = time.monotonic() - attempt_started
        if uptime >= MAX_RECONNECT_STABLE_SECONDS:
            delay = MAX_RECONNECT_MIN_DELAY

        logger.warning("MAX отключен (сессия жила %.0f c). Переподключение через %.0f c", uptime, delay)
        await asyncio.sleep(delay)
        delay = min(delay * 2, MAX_RECONNECT_MAX_DELAY)


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
        await run_max_client_forever()
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
