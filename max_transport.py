import json
import os
import struct

import lz4.block
import msgpack
import zstandard
from pymax import MaxClient

HEADER = struct.Struct('>BBHHI')
MAX_PAYLOAD = 10 * 1024 * 1024


def unpack_value(code, data):
    return msgpack.unpackb(data, raw=False, strict_map_key=False, ext_hook=unpack_value) if code == 1 else msgpack.ExtType(code, data)


def encode_frame(message):
    payload = msgpack.packb(message['payload'], use_bin_type=True)
    if len(payload) > MAX_PAYLOAD:
        raise ValueError('MAX frame too large')
    return HEADER.pack(10, message['cmd'], message['seq'], message['opcode'], len(payload)) + payload


def decode_frame(raw):
    if isinstance(raw, str):
        return json.loads(raw)
    ver, cmd, seq, opcode, signed_length = HEADER.unpack_from(raw)
    flags, length = signed_length >> 24, signed_length & 0xffffff
    if length != len(raw) - HEADER.size or length > MAX_PAYLOAD:
        raise ValueError('Invalid MAX frame length')
    payload = raw[HEADER.size:]
    if flags == 255:
        with zstandard.ZstdDecompressor().stream_reader(payload) as stream:
            payload = stream.read(MAX_PAYLOAD + 1)
    elif flags:
        payload = lz4.block.decompress(payload, uncompressed_size=MAX_PAYLOAD)
    if len(payload) > MAX_PAYLOAD:
        raise ValueError('Decompressed MAX frame too large')
    return dict(ver=ver, cmd=cmd, seq=seq, opcode=opcode,
                payload=msgpack.unpackb(payload, raw=False, strict_map_key=False, ext_hook=unpack_value) if payload else {})


class BinaryWebSocket:
    def __init__(self, websocket):
        self.websocket = websocket

    def __getattr__(self, name):
        return getattr(self.websocket, name)

    async def send(self, message):
        await self.websocket.send(encode_frame(json.loads(message)))

    async def recv(self):
        return decode_frame(await self.websocket.recv())


class BridgeTransportClient(MaxClient):
    def _make_message(self, opcode, payload, cmd=0):
        if isinstance(self._ws, BinaryWebSocket):
            while True:
                self._seq %= 65535
                if self._seq + 1 not in self._pending:
                    break
                self._seq += 1
        return super()._make_message(opcode, payload, cmd)

    def _parse_json(self, raw):
        return raw if isinstance(raw, dict) else super()._parse_json(raw)

    async def _handshake(self, user_agent):
        if os.getenv('MAX_WS_BINARY') == 'true':
            if not isinstance(self._ws, BinaryWebSocket):
                self._ws = BinaryWebSocket(self.ws)
        return await super()._handshake(user_agent)
