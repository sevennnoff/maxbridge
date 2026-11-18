"""Интерактивная авторизация MAX через QR.

Пишет актуальную ссылку в qr_link.txt и ждёт подтверждения в мобильном приложении MAX.
После подтверждения сохраняет токен сессии в cache/session.db и выходит.
"""

import asyncio
import datetime
import logging
import os
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(BASE_DIR))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(BASE_DIR / ".env", override=True)

from pymax import MaxClient  # noqa: E402
from pymax.payloads import UserAgentPayload
from max_transport import BridgeTransportClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("maxbridge-login")

UA = UserAgentPayload(
    device_type="WEB",
    device_name="Chrome",
    os_version="macOS 10.15.7",
    header_user_agent=(
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    screen="1440x900 1.0x",
    timezone="Europe/Moscow",
)

LINK_FILE = BASE_DIR / "qr_link.txt"
STATUS_FILE = BASE_DIR / "qr_status.txt"


def write_status(text: str) -> None:
    STATUS_FILE.write_text(text, encoding="utf-8")
    logger.info("STATUS: %s", text)


async def poll_once(client: MaxClient) -> bool:
    data = await client._request_qr_login()

    link = data.get("qrLink")
    track_id = data.get("trackId")
    expires_at = data.get("expiresAt")
    poll_interval = data.get("pollingInterval") or 5000

    if not link or not track_id or not expires_at:
        raise RuntimeError(f"Некорректные данные QR-логина: {data}")

    LINK_FILE.write_text(link, encoding="utf-8")
    logger.info("Новая QR-ссылка: %s", link)
    write_status(f"WAITING {link}")

    while True:
        now_ms = datetime.datetime.now().timestamp() * 1000
        if now_ms >= expires_at:
            write_status(f"EXPIRED {link}")
            return False

        resp = await client._send_and_wait(
            opcode=__import__("pymax").Opcode.GET_QR_STATUS,
            payload={"trackId": track_id},
        )
        payload = resp.get("payload", {}) or {}
        status = payload.get("status") or {}
        if status.get("loginAvailable"):
            write_status(f"CONFIRMED {link}")
            login_data = await client._get_qr_login_data(track_id)
            login_attrs = login_data.get("tokenAttrs", {}).get("LOGIN", {})
            token = login_attrs.get("token")
            if not token:
                write_status(f"NO_TOKEN {login_data}")
                raise RuntimeError(f"Сервер не вернул токен: {login_data}")
            client._token = token
            client._database.update_auth_token(client._device_id, token)
            write_status("SUCCESS")
            return True

        await asyncio.sleep(max(poll_interval / 1000, 2))


async def main() -> None:
    write_status("STARTING")
    max_ws_uri = os.getenv("MAX_WS_URI", "").strip()
    client = BridgeTransportClient(
        **({"uri": max_ws_uri} if max_ws_uri else {}),
        phone=os.getenv("PHONE"),
        work_dir=str(BASE_DIR / "cache"),
        headers=UA,
        reconnect=False,
    )
    await client.connect(client.user_agent)
    try:
        while True:
            try:
                if await poll_once(client):
                    logger.info("Авторизация MAX прошла успешно, токен сохранён")
                    return
            except Exception:
                logger.exception("Ошибка QR-логина, пробуем заново через 10 c")
                await asyncio.sleep(10)
    finally:
        try:
            await client.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
