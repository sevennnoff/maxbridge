"""Рендер QR-кода из qr_link.txt в PNG без сторонних зависимостей."""

import struct
import sys
import zlib
from pathlib import Path

import qrcode

BASE_DIR = Path(__file__).resolve().parent


def write_png(path: Path, matrix: list[list[bool]], scale: int = 8, border: int = 4) -> None:
    n = len(matrix)
    size = (n + border * 2) * scale
    white = b"\xff\xff\xff"
    black = b"\x00\x00\x00"

    rows = []
    for y in range(size):
        row = bytearray()
        row.append(0)
        my = y // scale - border
        for x in range(size):
            mx = x // scale - border
            if 0 <= mx < n and 0 <= my < n and matrix[my][mx]:
                row += black
            else:
                row += white
        rows.append(bytes(row))

    raw = b"".join(rows)

    def chunk(kind: bytes, data: bytes) -> bytes:
        body = kind + data
        return struct.pack(">I", len(data)) + body + struct.pack(">I", zlib.crc32(body) & 0xFFFFFFFF)

    png = b"\x89PNG\r\n\x1a\n"
    png += chunk(b"IHDR", struct.pack(">IIBBBBB", size, size, 8, 2, 0, 0, 0))
    png += chunk(b"IDAT", zlib.compress(raw, 9))
    png += chunk(b"IEND", b"")

    path.write_bytes(png)


def main() -> None:
    link_file = BASE_DIR / "qr_link.txt"
    out_file = Path(sys.argv[1]) if len(sys.argv) > 1 else BASE_DIR / "qr.png"

    link = link_file.read_text(encoding="utf-8").strip()
    if not link:
        raise SystemExit("qr_link.txt пуст")

    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=1,
        border=0,
    )
    qr.add_data(link)
    qr.make(fit=True)
    write_png(out_file, qr.get_matrix())

    print(f"LINK={link}")
    print(f"PNG={out_file}")


if __name__ == "__main__":
    main()
