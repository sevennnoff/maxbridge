import asyncio
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from pymax import Message, Opcode

from message_edits import ForwardStore

with patch.dict(os.environ, {
    'PHONE': '+70000000000', 'BOT_TOKEN': '123:fixture',
    'CHATS': '[{"max":-10,"tg":-20,"tg2max":false}]',
}), patch('dotenv.load_dotenv'):
    import pymax_main as bridge


def event(text, edited=False, **fields):
    return {
        'opcode': Opcode.NOTIF_MESSAGE.value,
        'payload': {'chatId': -10, 'message': {
            'id': 117643000000000001, 'time': 1, 'text': text, 'type': 'USER',
            **({'status': 'EDITED'} if edited else {}), **fields,
        }},
    }


def message(text, edited=False, **fields):
    return Message.from_dict(event(text, edited, **fields)['payload'])


def delivery(mid):
    return SimpleNamespace(chat=SimpleNamespace(id=-20), message_id=mid,
                           is_topic_message=True, message_thread_id=37)


class MessageEditsTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        tmp = self.enterContext(tempfile.TemporaryDirectory())
        self.path = Path(tmp) / 'forwarded.db'
        self.enterContext(patch.object(bridge, 'forward_store', ForwardStore(self.path)))
        self.enterContext(patch.object(bridge, 'CHATS', {-10: -20}))
        self.bot = self.enterContext(patch.object(bridge, 'telegram_bot', AsyncMock()))
        self.bot.send_message.return_value = delivery(40)

    async def dispatch(self, raw):
        tasks = []

        def schedule(coro, **kwargs):
            tasks.append(asyncio.create_task(coro, **kwargs))

        with patch.object(bridge.client, '_create_safe_task', schedule):
            await bridge.client._handle_message_notifications(raw)
        self.assertEqual(len(tasks), 1)
        await asyncio.gather(*tasks)

    async def test_dispatch_edit_after_restart_to_original_chat_and_thread(self):
        await self.dispatch(event('до'))
        with patch.object(bridge, 'forward_store', ForwardStore(self.path)), patch.object(bridge, 'CHATS', {-10: -99}):
            await self.dispatch(event('после', edited=True))
        self.bot.edit_message_text.assert_awaited_once_with(chat_id=-20, message_id=40, text='MAX: после')
        notice = self.bot.send_message.await_args.kwargs
        self.assertEqual((notice['chat_id'], notice['message_thread_id']), (-20, 37))
        self.assertEqual(notice['reply_parameters'].message_id, 40)
        self.assertFalse(notice['reply_parameters'].allow_sending_without_reply)

    async def test_unknown_message_never_sends_anything(self):
        await bridge.handle_max_edit(message('без пары', edited=True))
        self.assertEqual(self.bot.mock_calls, [])

    async def test_album_caption_edit_uses_first_item_and_deduplicates(self):
        photos = [{'_type': 'PHOTO', 'baseUrl': 'https://example.invalid/photo', 'height': 1,
                   'width': 1, 'photoId': i, 'photoToken': 'fixture'} for i in (1, 2)]
        with patch.object(bridge, 'download_bytes', AsyncMock(return_value=(b'photo', 'test.jpg'))):
            self.bot.send_media_group.return_value = [delivery(41), delivery(42)]
            await bridge.handle_max_message(message('до', attaches=photos))
        edited = message('после', edited=True, attaches=photos)
        await asyncio.gather(bridge.handle_max_edit(edited), bridge.handle_max_edit(edited))
        self.bot.edit_message_caption.assert_awaited_once_with(chat_id=-20, message_id=41, caption='MAX: после')
        self.bot.send_message.assert_awaited_once()
        self.assertEqual(self.bot.send_message.await_args.kwargs['reply_parameters'].message_id, 41)

    async def test_edit_failure_does_not_claim_message_was_updated(self):
        await bridge.handle_max_message(message('до'))
        self.bot.edit_message_text.side_effect = RuntimeError('test failure')
        edited = message('после', edited=True)
        with self.assertLogs('maxbridge', level='ERROR'):
            await bridge.handle_max_edit(edited)
        self.assertEqual(self.bot.send_message.await_count, 1)
        self.bot.edit_message_text.side_effect = None
        await bridge.handle_max_edit(edited)
        self.assertEqual(self.bot.send_message.await_count, 2)
