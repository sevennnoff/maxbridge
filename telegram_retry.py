import asyncio
import logging

from aiogram.exceptions import TelegramRetryAfter

logger = logging.getLogger('maxbridge')
MAX_ATTEMPTS = 3
MAX_TOTAL_WAIT = 60


async def retry_chat_request(make_request, bot, method):
    """Retry only acknowledged flood rejects, never ambiguous transport errors."""
    if getattr(method, 'chat_id', None) is None:
        return await make_request(bot, method)

    remaining = MAX_TOTAL_WAIT
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            return await make_request(bot, method)
        except TelegramRetryAfter as error:
            delay = max(1, error.retry_after)
            if attempt == MAX_ATTEMPTS or delay > remaining:
                raise
            remaining -= delay
            logger.warning('Telegram ограничил %s: повтор через %s с (%s/%s)',
                           method.__api_method__, delay, attempt, MAX_ATTEMPTS - 1)
            await asyncio.sleep(delay)
