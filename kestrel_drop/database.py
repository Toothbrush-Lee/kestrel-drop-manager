"""Transactional query ledger populated only from verified committed archives.

CSV and its checkpoint remain the BLE transport's recovery authority. This
ledger provides typed SQL queries and replay-safe publication across devices.
There is deliberately no cross-file transaction claim: a CSV commit can be
replayed into SQLite on the next refresh after interruption.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sqlite3
import tempfile

from . import protocol

SCHEMA_VERSION = 1
MEASUREMENTS = (
    'temperature_c', 'relative_humidity_pct', 'station_pressure_hpa',
    'heat_index_c', 'dew_point_c', 'density_altitude_m_raw',
    'density_altitude_m_official_integer_estimate',
)

SCHEMA = (
    '''CREATE TABLE IF NOT EXISTS devices (
        serial TEXT PRIMARY KEY, model TEXT NOT NULL, archive_path TEXT NOT NULL,
        first_cursor INTEGER NOT NULL CHECK(first_cursor >= 0),
        next_cursor INTEGER NOT NULL, row_count INTEGER NOT NULL CHECK(row_count > 0),
        first_timestamp TEXT NOT NULL, last_timestamp TEXT NOT NULL,
        csv_sha256 TEXT NOT NULL, fieldnames_json TEXT NOT NULL, updated_utc TEXT NOT NULL,
        CHECK(next_cursor - first_cursor = row_count))''',
    '''CREATE TABLE IF NOT EXISTS observations (
        serial TEXT NOT NULL REFERENCES devices(serial), absolute_cursor INTEGER NOT NULL,
        timestamp_device_local TEXT NOT NULL,
        temperature_c REAL NOT NULL, relative_humidity_pct REAL NOT NULL,
        station_pressure_hpa REAL NOT NULL, heat_index_c REAL NOT NULL,
        dew_point_c REAL NOT NULL, density_altitude_m_raw REAL NOT NULL,
        density_altitude_m_official_integer_estimate INTEGER NOT NULL,
        unknown_byte INTEGER NOT NULL, block INTEGER NOT NULL, position_in_block INTEGER NOT NULL,
        record_raw BLOB NOT NULL CHECK(length(record_raw) = 21), row_json TEXT NOT NULL,
        PRIMARY KEY(serial, absolute_cursor)) WITHOUT ROWID''',
    '''CREATE INDEX IF NOT EXISTS observations_time
       ON observations(serial, timestamp_device_local, absolute_cursor)''',
    '''CREATE TABLE IF NOT EXISTS ingestion_batches (
        id INTEGER PRIMARY KEY, serial TEXT NOT NULL REFERENCES devices(serial),
        imported_utc TEXT NOT NULL, first_added_cursor INTEGER NOT NULL,
        next_cursor INTEGER NOT NULL, added_records INTEGER NOT NULL CHECK(added_records >= 0),
        csv_sha256 TEXT NOT NULL, UNIQUE(serial, csv_sha256))''',
)


class Store:
    def __init__(self, path: Path):
        self.path = Path(path).expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as connection:
            version = connection.execute('PRAGMA user_version').fetchone()[0]
            tables = {r[0] for r in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if version not in (0, SCHEMA_VERSION) or (version == 0 and tables):
                raise RuntimeError('数据库格式或版本不受支持，未尝试覆盖。请保留文件并检查。')
            connection.execute('PRAGMA journal_mode=WAL')
            with connection:
                connection.execute('BEGIN IMMEDIATE')
                for statement in SCHEMA:
                    connection.execute(statement)
                connection.execute(f'PRAGMA user_version={SCHEMA_VERSION}')

    @contextmanager
    def connect(self):
        connection = sqlite3.connect(self.path, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA foreign_keys=ON')
        connection.execute('PRAGMA synchronous=FULL')
        try:
            yield connection
        finally:
            connection.close()

    def ingest(self, output: Path, state: dict, names: list[str], rows: list[dict]) -> dict:
        """Append a verified full snapshot, or fail without changing the ledger."""
        serial, count = str(state['serial']), len(rows)
        if not serial.isascii() or not serial.isdigit() or 'DROP 3' not in state['model'].upper():
            raise RuntimeError('数据库目前只接收序列号已核验的 DROP D3 档案')
        next_cursor = int(state['next_cursor'])
        first = next_cursor - count
        if not rows or count != state['row_count'] or first < 0 or names != state['fieldnames']:
            raise RuntimeError('CSV 行数、表头或游标与状态不一致，拒绝入库')
        protocol._check_order(rows)
        encoded = []
        for row in rows:
            raw = bytes.fromhex(row['record_hex'])
            decoded = protocol.decode_record(raw, block=int(row['block']), position=int(row['position_in_block']))
            if row['timestamp_device_local'] != decoded['timestamp_device_local']:
                raise RuntimeError('记录时间与原始字节不一致，拒绝入库')
            for field in MEASUREMENTS:
                value = float(row[field])
                if not math.isfinite(value) or abs(value - decoded[field]) > 0.000001:
                    raise RuntimeError(f'记录字段 {field} 与原始字节不一致，拒绝入库')
            if int(row['unknown_byte']) != decoded['unknown_byte']:
                raise RuntimeError('记录字段 unknown_byte 与原始字节不一致，拒绝入库')
            encoded.append((raw, decoded, json.dumps(row, sort_keys=True, ensure_ascii=False)))
        now = datetime.now(timezone.utc).isoformat()
        with self.connect() as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            existing = connection.execute('SELECT * FROM devices WHERE serial=?', (serial,)).fetchone()
            old_count = 0
            if existing:
                old_count = existing['row_count']
                if existing['model'] != state['model'] or existing['first_cursor'] != first or old_count > count:
                    raise RuntimeError('SQLite 中的设备、起始游标或记录数与 CSV 历史冲突，拒绝覆盖')
                saved = connection.execute(
                    'SELECT * FROM observations WHERE serial=? ORDER BY absolute_cursor',
                    (serial,)).fetchall()
                if len(saved) != old_count:
                    raise RuntimeError('SQLite 记录数与设备状态不一致，拒绝继续入库')
                for index, record in enumerate(saved):
                    if (record['absolute_cursor'], record['record_raw'], record['row_json']) != (
                            first + index, encoded[index][0], encoded[index][2]):
                        raise RuntimeError(f'设备 {serial} 游标 {first + index} 的既有记录冲突，事务已撤回')
                    decoded = encoded[index][1]
                    if any(record[field] != decoded[field] for field in (*MEASUREMENTS, 'timestamp_device_local',
                                                                         'unknown_byte', 'block', 'position_in_block')):
                        raise RuntimeError(f'设备 {serial} 游标 {first + index} 的数据库测量字段冲突，事务已撤回')
                if existing['next_cursor'] != first + old_count:
                    raise RuntimeError('SQLite 游标状态不连续，拒绝继续入库')
            connection.execute('''INSERT INTO devices VALUES(?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(serial) DO UPDATE SET
                archive_path=excluded.archive_path, next_cursor=excluded.next_cursor,
                row_count=excluded.row_count, last_timestamp=excluded.last_timestamp,
                csv_sha256=excluded.csv_sha256, fieldnames_json=excluded.fieldnames_json,
                updated_utc=excluded.updated_utc''',
                (serial, state['model'], str(Path(output).resolve()), first, next_cursor, count,
                 rows[0]['timestamp_device_local'], rows[-1]['timestamp_device_local'],
                 state['csv_sha256'], json.dumps(names), now))
            additions = []
            for index in range(old_count, count):
                raw, decoded, row_json = encoded[index]
                additions.append((serial, first + index, decoded['timestamp_device_local'],
                                  *(decoded[field] for field in MEASUREMENTS), decoded['unknown_byte'],
                                  decoded['block'], decoded['position_in_block'], raw, row_json))
            connection.executemany('INSERT INTO observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', additions)
            connection.execute('''INSERT INTO ingestion_batches
                (serial, imported_utc, first_added_cursor, next_cursor, added_records, csv_sha256)
                VALUES(?,?,?,?,?,?) ON CONFLICT(serial, csv_sha256) DO NOTHING''',
                (serial, now, first + old_count, next_cursor, count - old_count, state['csv_sha256']))
        return {'database': str(self.path), 'database_records': count,
                'database_next_cursor': next_cursor, 'database_added_records': count - old_count}

    def device(self, serial: str) -> dict:
        with self.connect() as connection:
            row = connection.execute('SELECT * FROM devices WHERE serial=?', (serial,)).fetchone()
        if row is None:
            raise FileNotFoundError(f'数据库中没有设备 {serial}，请先下载或导入')
        return dict(row)

    def query(self, serial: str, *, limit=None, since=None, until=None, end_exclusive=False) -> list[dict]:
        self.device(serial)
        conditions, values = ['serial=?'], [serial]
        if since:
            conditions.append('timestamp_device_local>=?')
            values.append(since)
        if until:
            conditions.append('timestamp_device_local<?' if end_exclusive else 'timestamp_device_local<=?')
            values.append(until)
        ordering = 'DESC' if limit is not None else 'ASC'
        sql = 'SELECT absolute_cursor, row_json FROM observations WHERE ' + ' AND '.join(conditions)
        sql += f' ORDER BY absolute_cursor {ordering}'
        if limit is not None:
            sql += ' LIMIT ?'
            values.append(limit)
        with self.connect() as connection:
            rows = connection.execute(sql, values).fetchall()
        if limit is not None:
            rows.reverse()
        return [dict(json.loads(row['row_json']), serial=serial, absolute_cursor=row['absolute_cursor']) for row in rows]

    def summary(self) -> dict:
        with self.connect() as connection:
            connection.execute('BEGIN')
            integrity = [row[0] for row in connection.execute('PRAGMA quick_check')]
            if integrity != ['ok']:
                raise RuntimeError('SQLite 完整性检查未通过：' + '; '.join(integrity))
            foreign = connection.execute('PRAGMA foreign_key_check').fetchall()
            if foreign:
                raise RuntimeError('SQLite 外键检查未通过')
            for row in connection.execute('''SELECT d.serial, d.row_count, d.first_cursor, d.next_cursor,
                    count(o.absolute_cursor) AS actual, min(o.absolute_cursor) AS first,
                    max(o.absolute_cursor) AS last FROM devices d
                    LEFT JOIN observations o ON o.serial=d.serial GROUP BY d.serial'''):
                if (row['actual'], row['first'], row['last']) != (row['row_count'], row['first_cursor'], row['next_cursor'] - 1):
                    raise RuntimeError(f'SQLite 设备 {row["serial"]} 的记录数或游标连续性异常')
            devices = [dict(row) for row in connection.execute(
                'SELECT serial, model, row_count, first_cursor, next_cursor FROM devices ORDER BY serial')]
            observations = connection.execute('SELECT count(*) FROM observations').fetchone()[0]
            batches = connection.execute('SELECT count(*) FROM ingestion_batches').fetchone()[0]
        return {'database': str(self.path), 'schema_version': SCHEMA_VERSION, 'integrity': 'ok',
                'records': observations, 'batches': batches, 'devices': devices}

    def backup(self, destination: Path) -> Path:
        destination = Path(destination).expanduser().resolve()
        if destination.exists():
            raise FileExistsError(f'备份文件已存在，不会覆盖：{destination}')
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix='.kestrel-backup-', suffix='.sqlite', dir=destination.parent)
        os.close(fd)
        temporary = Path(temporary)
        try:
            with self.connect() as source:
                target = sqlite3.connect(temporary)
                try:
                    source.backup(target)
                    if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                        raise RuntimeError('备份完整性检查失败，未发布文件')
                finally:
                    target.close()
            with temporary.open('rb') as stream:
                os.fsync(stream.fileno())
            os.link(temporary, destination)  # atomic, exclusive publication on the same filesystem
        finally:
            temporary.unlink(missing_ok=True)
        return destination
