import unittest
from unittest.mock import AsyncMock, call, patch

from aiogram.exceptions import TelegramNetworkError, TelegramRetryAfter
from aiogram.methods import GetUpdates, SendDocument
from aiogram.types import BufferedInputFile

from telegram_retry import retry_chat_request


class TelegramRetryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.bot = object()
        self.method = SendDocument(chat_id=-20, document=BufferedInputFile(b'%PDF-', 'test.pdf'))
        self.limited = TelegramRetryAfter(self.method, 'Flood control', 4)
        self.sleep = self.enterContext(patch('telegram_retry.asyncio.sleep', new_callable=AsyncMock))

    async def test_retries_same_upload_after_server_delay(self):
        sent = object()
        request = AsyncMock(side_effect=[self.limited, sent])
        self.assertIs(await retry_chat_request(request, self.bot, self.method), sent)
        self.assertEqual(request.await_args_list, [call(self.bot, self.method)] * 2)
        self.sleep.assert_awaited_once_with(4)

    async def test_stops_after_three_attempts(self):
        request = AsyncMock(side_effect=self.limited)
        with self.assertRaises(TelegramRetryAfter):
            await retry_chat_request(request, self.bot, self.method)
        self.assertEqual(request.await_count, 3)
        self.assertEqual(self.sleep.await_args_list, [call(4), call(4)])

    async def test_does_not_retry_network_failure_or_polling(self):
        error = TelegramNetworkError(method=self.method, message='Timeout')
        request = AsyncMock(side_effect=error)
        with self.assertRaises(TelegramNetworkError):
            await retry_chat_request(request, self.bot, self.method)
        request.assert_awaited_once_with(self.bot, self.method)
        poll = GetUpdates()
        request = AsyncMock(side_effect=TelegramRetryAfter(poll, 'Flood control', 4))
        with self.assertRaises(TelegramRetryAfter):
            await retry_chat_request(request, self.bot, poll)
        request.assert_awaited_once_with(self.bot, poll)
        self.sleep.assert_not_awaited()

    async def test_bounds_total_wait(self):
        request = AsyncMock(side_effect=TelegramRetryAfter(self.method, 'Flood control', 40))
        with self.assertRaises(TelegramRetryAfter):
            await retry_chat_request(request, self.bot, self.method)
        self.assertEqual(request.await_count, 2)
        self.sleep.assert_awaited_once_with(40)
