import unittest

from pymax import Opcode

from max_transport import BinaryWebSocket, BridgeTransportClient


class AttachmentMessageIDTests(unittest.TestCase):
    def test_numeric_id_only_for_binary_attachment_requests(self):
        for binary in (False, True):
            for opcode in (Opcode.FILE_DOWNLOAD, Opcode.VIDEO_PLAY, Opcode.PING):
                with self.subTest(binary=binary, opcode=opcode):
                    client = BridgeTransportClient.__new__(BridgeTransportClient)
                    client._ws = BinaryWebSocket(None) if binary else None
                    client._seq = 0
                    client._pending = {}
                    import logging
                    client.logger = logging.getLogger(__name__)
                    payload = {"messageId": "117643000000000001"}
                    expected = int(payload["messageId"]) if binary and opcode != Opcode.PING else payload["messageId"]
                    self.assertEqual(client._make_message(opcode, payload)["payload"]["messageId"], expected)
                    self.assertIsInstance(payload["messageId"], str)
