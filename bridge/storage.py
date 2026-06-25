"""SQLite-backed persistence for sensor readings, so history/graphs survive restarts.

Uses the stdlib `sqlite3` only. Writes go through a single dedicated writer thread (fed
by a queue) so the packet-processing thread never blocks on disk and we sidestep
sqlite3's same-thread connection restriction. Reads (from the async web server) open
their own short-lived connection; WAL mode lets a reader run concurrently with the
writer.
"""

import queue
import sqlite3
import threading
import time
from typing import Any, Optional

from .config import STATS_DB_PATH, STATS_RETENTION_DAYS

# (sensor_key, topic, value, ts) tuples awaiting persistence.
_write_queue: 'queue.Queue[tuple[str, str, Any, float]]' = queue.Queue()
_db_path: str = STATS_DB_PATH

# How often the writer prunes rows older than the retention window.
_PRUNE_INTERVAL_SECONDS = 3600.0


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(_db_path)
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn


def _init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        '''
        CREATE TABLE IF NOT EXISTS readings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_key TEXT NOT NULL,
            topic TEXT NOT NULL,
            value TEXT NOT NULL,
            value_num REAL,
            ts REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_readings_key_ts ON readings (sensor_key, ts);
        '''
    )
    conn.commit()


def init() -> None:
    """Create the schema and start the writer thread. Call once at startup."""
    conn = _connect()
    _init_schema(conn)
    conn.close()

    threading.Thread(target=_writer_worker, name='storage-writer', daemon=True).start()


def record(sensor_key: str, topic: str, value: Any, ts: float) -> None:
    """Queue a reading for persistence. Safe to call from any thread; never blocks."""
    _write_queue.put((sensor_key, topic, value, ts))


def _coerce_num(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _writer_worker() -> None:
    conn = _connect()
    last_prune = 0.0

    while True:
        # Block for the first item, then drain whatever else is queued into one batch.
        batch = [_write_queue.get()]
        try:
            while len(batch) < 500:
                batch.append(_write_queue.get_nowait())
        except queue.Empty:
            pass

        rows = [(key, topic, str(value), _coerce_num(value), ts) for key, topic, value, ts in batch]
        try:
            conn.executemany(
                'INSERT INTO readings (sensor_key, topic, value, value_num, ts) VALUES (?, ?, ?, ?, ?)',
                rows,
            )
            conn.commit()
        except sqlite3.Error as e:
            print(f'storage: failed to write {len(rows)} readings: {e}')

        now = time.time()
        if now - last_prune > _PRUNE_INTERVAL_SECONDS:
            _prune(conn)
            last_prune = now


def _prune(conn: sqlite3.Connection) -> None:
    cutoff = time.time() - STATS_RETENTION_DAYS * 86400
    try:
        conn.execute('DELETE FROM readings WHERE ts < ?', (cutoff,))
        conn.commit()
    except sqlite3.Error as e:
        print(f'storage: failed to prune old readings: {e}')


def query_history(sensor_key: str, since: float, topic: Optional[str] = None) -> list[dict[str, Any]]:
    """Return readings for a sensor since `since` (unix epoch), oldest first. Opens its
    own connection so it is safe to call from the web server's event loop thread."""
    conn = _connect()
    try:
        sql = 'SELECT topic, value, value_num, ts FROM readings WHERE sensor_key = ? AND ts >= ?'
        params: list[Any] = [sensor_key, since]
        if topic is not None:
            sql += ' AND topic = ?'
            params.append(topic)
        sql += ' ORDER BY ts ASC'
        cursor = conn.execute(sql, params)
        return [
            {'topic': row[0], 'value': row[1], 'value_num': row[2], 'ts': row[3]}
            for row in cursor.fetchall()
        ]
    finally:
        conn.close()


def recent_message_timestamps(sensor_key: str, limit: int) -> list[float]:
    """The `limit` most recent message timestamps for a sensor, oldest first.

    A single packet produces one row per topic, all sharing the same `ts`, so we select
    distinct timestamps to count messages rather than readings. Used to seed the live
    average-interval stat at startup."""
    conn = _connect()
    try:
        cursor = conn.execute(
            'SELECT DISTINCT ts FROM readings WHERE sensor_key = ? ORDER BY ts DESC LIMIT ?',
            (sensor_key, limit),
        )
        return sorted(row[0] for row in cursor.fetchall())
    finally:
        conn.close()
