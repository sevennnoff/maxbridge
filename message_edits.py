import sqlite3
from contextlib import closing, contextmanager
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Delivery:
    chat_id: int
    message_id: int
    thread_id: int | None
    caption: bool
    content_hash: str


class ForwardStore:
    """Map MAX messages to actual Telegram deliveries without storing message text."""

    def __init__(self, path: Path):
        self.path = path

    @contextmanager
    def _connect(self):
        with closing(sqlite3.connect(self.path)) as db, db:
            db.execute('''CREATE TABLE IF NOT EXISTS deliveries (
                max_chat INTEGER NOT NULL, max_message TEXT NOT NULL,
                tg_chat INTEGER NOT NULL, tg_message INTEGER NOT NULL,
                tg_thread INTEGER, caption INTEGER NOT NULL, content_hash TEXT NOT NULL,
                PRIMARY KEY (max_chat, max_message)
            )''')
            yield db

    def get(self, key: tuple[int, str]) -> Delivery | None:
        with self._connect() as db:
            row = db.execute('''SELECT tg_chat, tg_message, tg_thread, caption, content_hash
                                FROM deliveries WHERE max_chat=? AND max_message=?''', key).fetchone()
        return Delivery(*row) if row else None

    def record(self, key: tuple[int, str], delivery: Delivery) -> None:
        with self._connect() as db:
            db.execute('INSERT OR IGNORE INTO deliveries VALUES (?, ?, ?, ?, ?, ?, ?)', (
                *key, delivery.chat_id, delivery.message_id, delivery.thread_id,
                delivery.caption, delivery.content_hash,
            ))

    def mark_notified(self, key: tuple[int, str], content_hash: str) -> None:
        with self._connect() as db:
            db.execute('UPDATE deliveries SET content_hash=? WHERE max_chat=? AND max_message=?',
                       (content_hash, *key))
