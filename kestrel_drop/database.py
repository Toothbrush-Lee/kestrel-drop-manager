"""Single durable authority for observations and BLE recovery (schema 2).

Block transactions commit before ACK. Final publication, cursor advancement and
pending cleanup are one transaction. Read operations never repair or reindex.
"""
from __future__ import annotations

from contextlib import contextmanager, closing
from datetime import datetime, timezone
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import tempfile

from . import protocol
from .errors import MigrationRequired

SCHEMA_VERSION = 2
APPLICATION_ID = 0x4B445250
DECODER = 'd3-21-v1'
MEASUREMENTS = ('temperature_c', 'relative_humidity_pct', 'station_pressure_hpa',
                'heat_index_c', 'dew_point_c', 'density_altitude_m_raw',
                'density_altitude_m_official_integer_estimate')
FIELDS = ('timestamp_device_local', *MEASUREMENTS, 'unknown_byte', 'block', 'position_in_block', 'record_hex')
SCHEMA = (
    '''CREATE TABLE devices (serial TEXT PRIMARY KEY, model TEXT NOT NULL, alias TEXT NOT NULL DEFAULT '')''',
    '''CREATE TABLE histories (
        id INTEGER PRIMARY KEY, serial TEXT NOT NULL REFERENCES devices(serial),
        generation INTEGER NOT NULL DEFAULT 1 CHECK(generation > 0),
        first_cursor INTEGER, next_cursor INTEGER, UNIQUE(serial, generation),
        CHECK((first_cursor IS NULL AND next_cursor IS NULL) OR
              (first_cursor >= 0 AND next_cursor > first_cursor)))''',
    '''CREATE TABLE transfers (
        id INTEGER PRIMARY KEY, history_id INTEGER NOT NULL REFERENCES histories(id),
        requested_cursor INTEGER NOT NULL CHECK(requested_cursor >= 0),
        expected_count INTEGER NOT NULL CHECK(expected_count >= 0),
        started_utc TEXT NOT NULL, finished_utc TEXT, decoder_version TEXT NOT NULL,
        status TEXT NOT NULL CHECK(status IN ('receiving','interrupted','completed','superseded','imported')),
        final_cursor INTEGER, validation_json TEXT)''',
    '''CREATE TABLE pending_blocks (
        transfer_id INTEGER NOT NULL REFERENCES transfers(id), sequence INTEGER NOT NULL CHECK(sequence >= 0),
        payload BLOB NOT NULL CHECK(length(payload)>0 AND length(payload)%21=0),
        sha256 TEXT NOT NULL, PRIMARY KEY(transfer_id, sequence)) WITHOUT ROWID''',
    '''CREATE TABLE observations (
        history_id INTEGER NOT NULL REFERENCES histories(id), absolute_cursor INTEGER NOT NULL CHECK(absolute_cursor>=0),
        timestamp_device_local TEXT NOT NULL, temperature_c REAL NOT NULL, relative_humidity_pct REAL NOT NULL,
        station_pressure_hpa REAL NOT NULL, heat_index_c REAL NOT NULL, dew_point_c REAL NOT NULL,
        density_altitude_m_raw REAL NOT NULL, density_altitude_m_official_integer_estimate INTEGER NOT NULL,
        unknown_byte INTEGER NOT NULL, block INTEGER NOT NULL, position_in_block INTEGER NOT NULL,
        record_raw BLOB NOT NULL CHECK(length(record_raw)=21),
        PRIMARY KEY(history_id, absolute_cursor)) WITHOUT ROWID''',
    'CREATE INDEX observations_time ON observations(history_id, timestamp_device_local, absolute_cursor)',
    'CREATE INDEX transfers_history ON transfers(history_id, id)',
)


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def checked_rows(rows):
    result = []
    for row in rows:
        raw = bytes.fromhex(row['record_hex'])
        decoded = protocol.decode_record(raw, block=int(row['block']), position=int(row['position_in_block']))
        if any(not math.isfinite(float(row[k])) or abs(float(row[k]) - decoded[k]) > 1e-6 for k in MEASUREMENTS):
            raise RuntimeError('记录测量字段与原始字节不一致')
        if any(str(row[k]) != str(decoded[k]) for k in ('timestamp_device_local', 'unknown_byte')):
            raise RuntimeError('记录时间或未知字段与原始字节不一致')
        if decoded['block'] < 0 or decoded['position_in_block'] < 0:
            raise RuntimeError('记录块位置无效')
        result.append((raw, decoded))
    protocol._check_order([d for _, d in result])
    return result


class Store:
    def __init__(self, path: Path, *, create=False):
        self.path = Path(path).expanduser().resolve()
        if not self.path.exists() and not create:
            raise FileNotFoundError(f'尚无数据库：{self.path}')
        if create:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.connect(writable=True, create=True) as connection:
                version = connection.execute('PRAGMA user_version').fetchone()[0]
                if version == 0:
                    if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table'").fetchone():
                        raise RuntimeError('数据库格式不受支持，拒绝覆盖')
                    connection.execute('PRAGMA journal_mode=WAL')
                    with connection:
                        connection.execute('BEGIN IMMEDIATE')
                        for statement in SCHEMA:
                            connection.execute(statement)
                        connection.execute(f'PRAGMA application_id={APPLICATION_ID}')
                        connection.execute(f'PRAGMA user_version={SCHEMA_VERSION}')
        with self.connect() as connection:
            if connection.execute('PRAGMA user_version').fetchone()[0] == 1:
                raise MigrationRequired('旧版 SQLite 需要运行 migrate；文件未被覆盖')
            if (connection.execute('PRAGMA user_version').fetchone()[0] != SCHEMA_VERSION or
                    connection.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID):
                raise RuntimeError('数据库需要迁移或版本不受支持；旧版本请运行 migrate，文件未被覆盖')

    @contextmanager
    def connect(self, *, writable=False, create=False):
        mode = 'rwc' if create else 'rw' if writable else 'ro'
        connection = sqlite3.connect(self.path.as_uri() + '?mode=' + mode, uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        connection.execute('PRAGMA foreign_keys=ON')
        if writable:
            connection.execute('PRAGMA synchronous=FULL')
        else:
            connection.execute('PRAGMA query_only=ON')
        try:
            yield connection
        finally:
            connection.close()

    @contextmanager
    def transaction(self):
        with self.connect(writable=True) as connection, connection:
            connection.execute('BEGIN IMMEDIATE')
            yield connection

    def _history(self, connection, serial):
        row = connection.execute('SELECT * FROM histories WHERE serial=? ORDER BY generation DESC LIMIT 1', (serial,)).fetchone()
        if row is None:
            raise FileNotFoundError(f'数据库中没有设备 {serial}')
        return row

    def ensure_device(self, serial, model):
        if not serial.isascii() or not serial.isdigit() or not 1 <= len(serial) <= 20 or 'DROP 3' not in model.upper():
            raise RuntimeError('当前仅支持已核验序列号的 DROP D3')
        with self.transaction() as connection:
            old = connection.execute('SELECT model FROM devices WHERE serial=?', (serial,)).fetchone()
            if old:
                if old['model'] != model:
                    raise RuntimeError('设备型号与已有档案冲突')
            else:
                connection.execute('INSERT INTO devices(serial,model) VALUES(?,?)', (serial, model))
                connection.execute('INSERT INTO histories(serial) VALUES(?)', (serial,))

    def _pending(self, connection, history):
        """Decode and verify every durable boundary, including across reconnects."""
        cursor = history['next_cursor'] or 0
        rows = []
        transfers = connection.execute("SELECT * FROM transfers WHERE history_id=? AND status IN ('receiving','interrupted') ORDER BY id", (history['id'],)).fetchall()
        for transfer in transfers:
            if transfer['decoder_version'] != DECODER:
                raise RuntimeError('下载解析器版本不受支持，拒绝猜测恢复位置')
            if transfer['requested_cursor'] != cursor:
                raise RuntimeError('待确认下载游标不连续')
            current, _ = self._blocks(connection, transfer['id'])
            rows.extend(current)
            cursor += len(current)
        protocol._check_order(rows)
        last = connection.execute('SELECT timestamp_device_local FROM observations WHERE history_id=? ORDER BY absolute_cursor DESC LIMIT 1', (history['id'],)).fetchone()
        if last and rows and rows[0]['timestamp_device_local'] < last[0]:
            raise RuntimeError('待确认数据早于已提交历史')
        return rows, cursor

    def _blocks(self, connection, transfer_id):
        rows, meta = [], []
        for block in connection.execute('SELECT * FROM pending_blocks WHERE transfer_id=? ORDER BY sequence', (transfer_id,)):
            payload = block['payload']
            if block['sequence'] != len(meta) or hashlib.sha256(payload).hexdigest() != block['sha256']:
                raise RuntimeError('待确认块的序号或摘要校验失败')
            records = [protocol.decode_record(payload[i:i+21], block=block['sequence'], position=i//21) for i in range(0, len(payload), 21)]
            rows.extend(records)
            meta.append({'block': block['sequence'], 'records': len(records)})
        protocol._check_order(rows)
        return rows, meta

    def resume(self, serial):
        with self.connect() as connection:
            connection.execute('BEGIN')
            history = self._history(connection, serial)
            if history['next_cursor'] is None:
                # Cursor zero can be clamped to the ring buffer's oldest record.
                return 0, []
            rows, cursor = self._pending(connection, history)
            return cursor, rows

    def begin_transfer(self, serial, requested_cursor, expected_count):
        with self.transaction() as connection:
            history = self._history(connection, serial)
            if history['next_cursor'] is None:
                if requested_cursor != 0:
                    raise RuntimeError('首次下载必须请求游标零')
                connection.execute("UPDATE transfers SET status='superseded', finished_utc=? WHERE history_id=? AND status IN ('receiving','interrupted')", (utcnow(), history['id']))
            else:
                _, cursor = self._pending(connection, history)
                if cursor != requested_cursor:
                    raise RuntimeError('下载恢复位置与事务中的边界不一致')
                connection.execute("UPDATE transfers SET status='interrupted' WHERE history_id=? AND status='receiving'", (history['id'],))
            return connection.execute('''INSERT INTO transfers(history_id,requested_cursor,expected_count,started_utc,decoder_version,status)
                VALUES(?,?,?,?,?,'receiving')''', (history['id'], requested_cursor, expected_count, utcnow(), DECODER)).lastrowid

    def append_block(self, transfer_id, rows, blocks):
        encoded = checked_rows(rows)
        if len(blocks) != 1 or not rows or blocks[0]['records'] != len(rows):
            raise RuntimeError('无效的接收块')
        sequence = blocks[0]['block']
        if any(d['block'] != sequence or d['position_in_block'] != i for i, (_, d) in enumerate(encoded)):
            raise RuntimeError('接收块内的位置不连续')
        payload = b''.join(raw for raw, _ in encoded)
        with self.transaction() as connection:
            transfer = connection.execute('SELECT * FROM transfers WHERE id=?', (transfer_id,)).fetchone()
            if transfer is None or transfer['status'] != 'receiving':
                raise RuntimeError('下载会话不是接收状态')
            last_block = connection.execute('SELECT sequence FROM pending_blocks WHERE transfer_id=? ORDER BY sequence DESC LIMIT 1', (transfer_id,)).fetchone()
            if sequence != (last_block[0] + 1 if last_block else 0):
                raise RuntimeError('接收块序号不连续')
            history = connection.execute('SELECT * FROM histories WHERE id=?', (transfer['history_id'],)).fetchone()
            prior = connection.execute("""SELECT b.payload,b.sha256 FROM pending_blocks b JOIN transfers t ON t.id=b.transfer_id
                WHERE t.history_id=? AND t.status IN ('receiving','interrupted') ORDER BY t.id DESC,b.sequence DESC LIMIT 1""", (history['id'],)).fetchone()
            last = None
            if prior:
                if hashlib.sha256(prior['payload']).hexdigest() != prior['sha256']:
                    raise RuntimeError('待确认块摘要校验失败')
                last = protocol.decode_record(prior['payload'][-21:], block=0, position=0)['timestamp_device_local']
            else:
                saved = connection.execute('SELECT timestamp_device_local FROM observations WHERE history_id=? ORDER BY absolute_cursor DESC LIMIT 1', (history['id'],)).fetchone()
                last = saved[0] if saved else None
            if last and rows[0]['timestamp_device_local'] < last:
                raise RuntimeError('接收记录时间倒退')
            connection.execute('INSERT INTO pending_blocks VALUES(?,?,?,?)',
                               (transfer_id, sequence, payload, hashlib.sha256(payload).hexdigest()))
        # Returning from this transaction is the only ACK-eligible boundary.

    def finish_transfer(self, transfer_id, *, next_cursor, elapsed_seconds, maxrows, received_rows):
        with self.transaction() as connection:
            transfer = connection.execute('SELECT * FROM transfers WHERE id=?', (transfer_id,)).fetchone()
            if not transfer or transfer['status'] != 'receiving':
                raise RuntimeError('下载会话不可重复提交')
            current, blocks = self._blocks(connection, transfer_id)
            if current != received_rows:
                raise RuntimeError('持久化记录与接收到的记录不同')
            validation = protocol.verify_completed_transfer(rows=current, blocks=blocks,
                initial_count=transfer['expected_count'], requested_cursor=transfer['requested_cursor'],
                next_cursor=next_cursor, elapsed_seconds=elapsed_seconds, maxrows=maxrows)
            history = connection.execute('SELECT * FROM histories WHERE id=?', (transfer['history_id'],)).fetchone()
            rows, cursor = self._pending(connection, history)
            if history['next_cursor'] is not None and cursor != next_cursor:
                raise RuntimeError('正式游标与累计待确认记录数不一致')
            first = next_cursor - len(rows)
            self._insert(connection, history['id'], first, checked_rows(rows))
            connection.execute('UPDATE histories SET first_cursor=coalesce(first_cursor,?), next_cursor=? WHERE id=?', (first, next_cursor, history['id']))
            connection.execute("UPDATE transfers SET status='completed', finished_utc=? WHERE history_id=? AND status IN ('receiving','interrupted')", (utcnow(), history['id']))
            connection.execute('UPDATE transfers SET final_cursor=?,validation_json=? WHERE id=?', (next_cursor, json.dumps(validation), transfer_id))
            connection.execute('DELETE FROM pending_blocks WHERE transfer_id IN (SELECT id FROM transfers WHERE history_id=?)', (history['id'],))
        return validation

    def _insert(self, connection, history_id, first, encoded):
        connection.executemany('INSERT INTO observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            [(history_id, first+i, d['timestamp_device_local'], *(d[k] for k in MEASUREMENTS),
              d['unknown_byte'], d['block'], d['position_in_block'], raw) for i, (raw, d) in enumerate(encoded)])

    def import_rows(self, serial, model, rows, next_cursor, provenance):
        encoded = checked_rows(rows)
        first = next_cursor - len(rows)
        if not rows or first < 0:
            raise RuntimeError('导入记录数量或游标无效')
        if not serial.isascii() or not serial.isdigit() or not 1 <= len(serial) <= 20 or 'DROP 3' not in model.upper():
            raise RuntimeError('仅支持 D3 档案导入')
        with self.transaction() as connection:
            if connection.execute('SELECT 1 FROM devices WHERE serial=?', (serial,)).fetchone():
                raise FileExistsError(f'设备 {serial} 已有档案，拒绝覆盖')
            connection.execute('INSERT INTO devices(serial,model) VALUES(?,?)', (serial, model))
            history_id = connection.execute('INSERT INTO histories(serial,first_cursor,next_cursor) VALUES(?,?,?)', (serial, first, next_cursor)).lastrowid
            self._insert(connection, history_id, first, encoded)
            connection.execute('''INSERT INTO transfers(history_id,requested_cursor,expected_count,started_utc,finished_utc,decoder_version,status,final_cursor,validation_json)
                VALUES(?,?,?,?,?,?,'imported',?,?)''', (history_id, first, len(rows), utcnow(), utcnow(), DECODER, next_cursor, json.dumps(provenance)))

    def device(self, serial):
        with self.connect() as connection:
            connection.execute('BEGIN')
            history = self._history(connection, serial)
            identity = connection.execute('SELECT * FROM devices WHERE serial=?', (serial,)).fetchone()
            stats = connection.execute('SELECT count(*), min(timestamp_device_local), max(timestamp_device_local) FROM observations WHERE history_id=?', (history['id'],)).fetchone()
            return {**dict(identity), **dict(history), 'row_count': stats[0], 'first_timestamp': stats[1], 'last_timestamp': stats[2]}

    def status(self):
        result = []
        with self.connect() as connection:
            connection.execute('BEGIN')
            histories = connection.execute('SELECT h.*, d.model FROM histories h JOIN devices d USING(serial) ORDER BY serial,generation').fetchall()
            for h in histories:
                count, earliest, latest = connection.execute('SELECT count(*),min(timestamp_device_local),max(timestamp_device_local) FROM observations WHERE history_id=?', (h['id'],)).fetchone()
                if h['next_cursor'] is None:
                    pending = connection.execute('SELECT coalesce(sum(length(payload)/21),0) FROM pending_blocks b JOIN transfers t ON t.id=b.transfer_id WHERE history_id=?', (h['id'],)).fetchone()[0]
                    attempts = connection.execute("SELECT count(*) FROM transfers WHERE history_id=? AND status IN ('receiving','interrupted','superseded')", (h['id'],)).fetchone()[0]
                    resume = 0
                else:
                    rows, resume = self._pending(connection, h)
                    pending, attempts = len(rows), 0
                result.append({'serial': h['serial'], 'model': h['model'], 'history_id': h['id'],
                    'committed_records': count, 'database_records': count, 'pending_records': pending,
                    'first_cursor': h['first_cursor'], 'next_cursor': h['next_cursor'], 'resume_cursor': resume,
                    'first_timestamp': earliest, 'last_timestamp': latest, 'initial_attempts': attempts,
                    'database': str(self.path)})
        return result

    def query(self, serial, *, limit=None, since=None, until=None, end_exclusive=False):
        conditions, values = ['h.serial=?'], [serial]
        if since:
            conditions.append('o.timestamp_device_local>=?'); values.append(since)
        if until:
            conditions.append('o.timestamp_device_local' + ('<?' if end_exclusive else '<=?')); values.append(until)
        sql = 'SELECT o.*,h.generation FROM observations o JOIN histories h ON h.id=o.history_id WHERE ' + ' AND '.join(conditions)
        sql += ' ORDER BY h.generation ' + ('DESC' if limit else 'ASC') + ',absolute_cursor ' + ('DESC' if limit else 'ASC')
        if limit:
            sql += ' LIMIT ?'; values.append(limit)
        self.device(serial)
        with self.connect() as connection:
            records = connection.execute(sql, values).fetchall()
        if limit:
            records.reverse()
        return [dict(serial=serial, history_generation=r['generation'], absolute_cursor=r['absolute_cursor'],
                     **{k: r[k] for k in FIELDS if k != 'record_hex'}, record_hex=r['record_raw'].hex()) for r in records]

    def check(self):
        with self.connect() as connection:
            connection.execute('BEGIN')
            integrity = [r[0] for r in connection.execute('PRAGMA integrity_check')]
            if integrity != ['ok'] or connection.execute('PRAGMA foreign_key_check').fetchall():
                raise RuntimeError('数据库完整性或外键检查失败')
            for h in connection.execute('SELECT * FROM histories'):
                records = connection.execute('SELECT * FROM observations WHERE history_id=? ORDER BY absolute_cursor', (h['id'],)).fetchall()
                if h['next_cursor'] is None:
                    if records: raise RuntimeError('未初始化历史包含正式记录')
                elif (len(records) != h['next_cursor'] - h['first_cursor'] or
                      any(r['absolute_cursor'] != h['first_cursor'] + i for i, r in enumerate(records))):
                    raise RuntimeError('历史游标不连续')
                checked_rows([dict(r, record_hex=r['record_raw'].hex()) for r in records])
                self._pending(connection, h)
                for t in connection.execute('SELECT id,decoder_version,status FROM transfers WHERE history_id=?', (h['id'],)):
                    self._blocks(connection, t['id'])
                    if t['decoder_version'] != DECODER: raise RuntimeError('解析器版本不受支持')
                    if t['status'] in ('completed','imported') and connection.execute('SELECT 1 FROM pending_blocks WHERE transfer_id=?', (t['id'],)).fetchone():
                        raise RuntimeError('完成的下载仍含待确认数据')
        return self.summary()

    def summary(self):
        devices = self.status()
        with self.connect() as connection:
            sessions = connection.execute('SELECT count(*) FROM transfers').fetchone()[0]
        return {'database': str(self.path), 'schema_version': SCHEMA_VERSION,
                'records': sum(d['database_records'] for d in devices), 'transfers': sessions, 'devices': devices}

    def backup(self, destination):
        destination = Path(destination).expanduser().resolve()
        if destination.exists():
            raise FileExistsError(f'备份文件已存在，不会覆盖：{destination}')
        destination.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix='.kestrel-backup-', suffix='.sqlite', dir=destination.parent)
        os.close(fd)
        temporary = Path(temporary)
        try:
            with self.connect() as source, closing(sqlite3.connect(temporary)) as target:
                source.backup(target)
                target.execute('PRAGMA journal_mode=DELETE')
                if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                    raise RuntimeError('备份完整性检查失败')
            Store(temporary).check()
            with temporary.open('rb') as stream: os.fsync(stream.fileno())
            os.link(temporary, destination)
            protocol._fsync_directory(destination.parent)
        finally:
            temporary.unlink(missing_ok=True)
        return destination
