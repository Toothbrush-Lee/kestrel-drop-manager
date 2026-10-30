"""Single durable authority for observations and BLE recovery (schema 5).

Block transactions commit before ACK. Final publication, cursor advancement and
pending cleanup are one transaction. Read operations never repair or reindex.
"""
from __future__ import annotations

from contextlib import contextmanager, closing
from datetime import datetime, timezone
from functools import lru_cache
import re
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import tempfile

from . import protocol
from .errors import MigrationRequired
from .devices import canonical_model, model_type, capabilities, serial_number
from .layouts import MEASUREMENTS, FIELDS, profile

SCHEMA_VERSION = 5
APPLICATION_ID = 0x4B445250
DECODER = 'd3-21-v2'

SCHEMA = (
    '''CREATE TABLE devices (serial TEXT PRIMARY KEY, model TEXT NOT NULL, alias TEXT NOT NULL DEFAULT '',
        management_code TEXT NOT NULL DEFAULT '', participant_code TEXT NOT NULL DEFAULT '')''',
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
        payload BLOB NOT NULL CHECK(length(payload)>0),
        sha256 TEXT NOT NULL, PRIMARY KEY(transfer_id, sequence)) WITHOUT ROWID''',
    '''CREATE TABLE observations (
        history_id INTEGER NOT NULL REFERENCES histories(id), absolute_cursor INTEGER NOT NULL CHECK(absolute_cursor>=0),
        timestamp_device_local TEXT NOT NULL, temperature_c REAL NOT NULL, relative_humidity_pct REAL,
        station_pressure_hpa REAL, heat_index_c REAL, dew_point_c REAL,
        density_altitude_m_raw REAL, density_altitude_m_official_integer_estimate INTEGER,
        unknown_byte INTEGER, block INTEGER NOT NULL, position_in_block INTEGER NOT NULL,
        record_raw BLOB NOT NULL CHECK(length(record_raw) IN (9,16,21)),
        PRIMARY KEY(history_id, absolute_cursor)) WITHOUT ROWID''',
    'CREATE INDEX observations_time ON observations(history_id, timestamp_device_local, absolute_cursor)',
    'CREATE INDEX transfers_history ON transfers(history_id, id)',
    '''CREATE TABLE device_actions (
        id INTEGER PRIMARY KEY, serial TEXT NOT NULL REFERENCES devices(serial),
        started_utc TEXT NOT NULL, finished_utc TEXT, requested_json TEXT NOT NULL,
        before_json TEXT NOT NULL, after_json TEXT, error TEXT,
        status TEXT NOT NULL CHECK(status IN ('started','verified','failed_or_unknown')))''',
    "CREATE UNIQUE INDEX devices_management_code ON devices(management_code) WHERE management_code<>''",
    '''CREATE VIEW device_overview AS
        SELECT d.serial,d.model,d.alias,d.management_code,d.participant_code,h.generation,h.first_cursor,h.next_cursor,
        (SELECT count(*) FROM observations o WHERE o.history_id=h.id) AS records,
        (SELECT timestamp_device_local FROM observations o WHERE o.history_id=h.id ORDER BY absolute_cursor DESC LIMIT 1) AS last_record_time,
        (SELECT max(finished_utc) FROM transfers t WHERE t.history_id=h.id AND status='completed') AS last_download_utc
        FROM devices d JOIN histories h USING(serial)
        WHERE h.generation=(SELECT max(generation) FROM histories WHERE serial=d.serial)''',

)


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def iter_checked_rows(rows, model='D3'):
    for row in rows:
        raw = bytes.fromhex(row['record_hex'])
        decoded = protocol.decode_record(raw, block=int(row['block']), position=int(row['position_in_block']), model=model)
        for k in MEASUREMENTS:
            if decoded[k] is None:
                if row.get(k) not in (None, ''): raise RuntimeError('型号不具备的测量字段必须为空')
            elif row.get(k) is None or not math.isfinite(float(row[k])) or abs(float(row[k]) - decoded[k]) > 1e-6:
                raise RuntimeError('记录测量字段与原始字节不一致')
        if (row['timestamp_device_local'] != decoded['timestamp_device_local'] or
                row.get('unknown_byte') not in ((None, '') if decoded['unknown_byte'] is None else (decoded['unknown_byte'], str(decoded['unknown_byte'])))):
            raise RuntimeError('记录时间或未知字段与原始字节不一致')
        if decoded['block'] < 0 or decoded['position_in_block'] < 0:
            raise RuntimeError('记录块位置无效')
        yield raw, decoded


def checked_rows(rows, model='D3'):
    return list(iter_checked_rows(rows, model))


SCHEMA_OBJECTS = {re.match(r'CREATE (?:UNIQUE )?(?:TABLE|INDEX|VIEW) (\w+)', sql)[1]: sql for sql in SCHEMA}


@lru_cache(maxsize=1)
def schema_contract():
    """Expected public columns and index keys, independent of formatting/DDL order."""
    with closing(sqlite3.connect(':memory:')) as connection:
        for sql in SCHEMA: connection.execute(sql)
        result = {}
        for kind, name, owner in connection.execute(
                "SELECT type,name,tbl_name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'"):
            pragma = 'index_info' if kind == 'index' else 'table_info'
            columns = tuple(tuple(row) for row in connection.execute(f'PRAGMA {pragma}({name})'))
            result[name] = (kind, owner, columns)
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
                if version in (2, 3):
                    if connection.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID:
                        raise RuntimeError('数据库来源不受支持，拒绝升级')
                    self._check_schema(connection)
                    self._upgrade_v2(connection, version=version)
                if connection.execute('PRAGMA user_version').fetchone()[0] == 4:
                    self._check_schema(connection)
                    self._upgrade_metadata(connection,backup=version not in (2,3))
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
                current = connection.execute('PRAGMA user_version').fetchone()[0]
                if current == SCHEMA_VERSION and connection.execute('PRAGMA application_id').fetchone()[0] == APPLICATION_ID:
                    self._check_schema(connection)
                    if connection.execute('PRAGMA journal_mode').fetchone()[0] != 'wal':
                        if connection.execute('PRAGMA journal_mode=WAL').fetchone()[0] != 'wal':
                            raise RuntimeError('运行档案无法启用 WAL；请使用支持 SQLite 的本地数据目录')
        with self.connect() as connection:
            if connection.execute('PRAGMA user_version').fetchone()[0] == 1:
                raise MigrationRequired('旧版 SQLite 需要运行 migrate；文件未被覆盖')
            if (connection.execute('PRAGMA user_version').fetchone()[0] not in (2, 3, 4, SCHEMA_VERSION) or
                    connection.execute('PRAGMA application_id').fetchone()[0] != APPLICATION_ID):
                raise RuntimeError('数据库需要迁移或版本不受支持；旧版本请运行 migrate，文件未被覆盖')

    def _upgrade_v2(self, connection, *, version=2):
        # Publish a complete, independent schema-2 snapshot before any DDL.
        import uuid
        directory = self.path.parent / 'backups'
        directory.mkdir(exist_ok=True)
        backup = directory / (f'schema{version}-before-upgrade-' + uuid.uuid4().hex[:12] + '.sqlite')
        Store(self.path).backup(backup)
        with connection:
            connection.execute('BEGIN IMMEDIATE')
            if version == 2: self._copy_v2_tables(connection)
            for name in ('transfers_history', 'device_actions', 'device_overview'):
                statement = SCHEMA_OBJECTS[name]
                if name == 'transfers_history': statement = statement.replace('CREATE INDEX ', 'CREATE INDEX IF NOT EXISTS ', 1)
                if name=='device_overview':statement=statement.replace('d.management_code,d.participant_code,','')
                connection.execute(statement)
            # Recompute the corrected signed 24-bit heat index from immutable raw bytes.
            for h in connection.execute('SELECT h.id,d.model FROM histories h JOIN devices d USING(serial)'):
                layout = profile(h['model'])
                if layout.kind != 'D1':
                    cursor = connection.execute('SELECT * FROM observations WHERE history_id=? ORDER BY absolute_cursor', (h['id'],))
                    while batch := cursor.fetchmany(1000):
                        updates = ((layout.decode(r['record_raw'], block=r['block'], position=r['position_in_block'])['heat_index_c'], h['id'], r['absolute_cursor']) for r in batch)
                        connection.executemany('UPDATE observations SET heat_index_c=? WHERE history_id=? AND absolute_cursor=?', updates)
                connection.execute('UPDATE transfers SET decoder_version=? WHERE history_id=?', (layout.decoder,h['id']))
            connection.execute('PRAGMA user_version=4')

    def _upgrade_metadata(self, connection, *, backup=True):
        import uuid
        directory=self.path.parent/'backups';directory.mkdir(exist_ok=True)
        if backup:Store(self.path).backup(directory/f'schema4-before-upgrade-{uuid.uuid4().hex[:12]}.sqlite')
        with connection:
            connection.execute('BEGIN IMMEDIATE')
            connection.execute("ALTER TABLE devices ADD COLUMN management_code TEXT NOT NULL DEFAULT ''")
            connection.execute("ALTER TABLE devices ADD COLUMN participant_code TEXT NOT NULL DEFAULT ''")
            connection.execute(SCHEMA_OBJECTS['devices_management_code'])
            connection.execute('DROP VIEW device_overview')
            connection.execute(SCHEMA_OBJECTS['device_overview'])
            connection.execute('PRAGMA user_version=5')

    def _copy_v2_tables(self, connection):
        for name in ('pending_blocks', 'observations'):
            connection.execute(SCHEMA_OBJECTS[name].replace('CREATE TABLE ' + name, 'CREATE TABLE ' + name + '_v3', 1))
            connection.execute(f'INSERT INTO {name}_v3 SELECT * FROM {name}')
            connection.execute(f'DROP TABLE {name}')
            connection.execute(f'ALTER TABLE {name}_v3 RENAME TO {name}')
        connection.execute(SCHEMA_OBJECTS['observations_time'])

    def _check_schema(self, connection):
        version = connection.execute('PRAGMA user_version').fetchone()[0]
        required = set(SCHEMA_OBJECTS)
        if version<5:required.discard('devices_management_code')
        if version in (2, 3): required -= {'device_actions', 'device_overview'}
        actual = {row[1]: (row[0], row[2]) for row in connection.execute(
            "SELECT type,name,tbl_name FROM sqlite_master WHERE name NOT LIKE 'sqlite_%'")}
        for name in sorted(required):
            kind, owner, expected = schema_contract()[name]
            if version<5 and name in ('devices','device_overview'):
                expected=tuple(row for row in expected if row[1] not in ('management_code','participant_code'))
                expected=tuple((i,*row[1:]) for i,row in enumerate(expected))
            if actual.get(name) != (kind, owner):
                raise RuntimeError(f'数据库结构缺失或类型错误：{name}（schema {version}）')
            pragma = 'index_info' if kind == 'index' else 'table_info'
            columns = tuple(tuple(row) for row in connection.execute(f'PRAGMA {pragma}({name})'))
            if version == 2 and name == 'observations':
                legacy_required = {'relative_humidity_pct', 'station_pressure_hpa', 'heat_index_c', 'dew_point_c', 'density_altitude_m_raw'}
                columns = tuple((*row[:3], 0, *row[4:]) if row[1] in legacy_required else row for row in columns)
            if name=='devices_management_code':
                index=next(row for row in connection.execute('PRAGMA index_list(devices)') if row[1]==name)
                sql=connection.execute("SELECT sql FROM sqlite_master WHERE name=?",(name,)).fetchone()[0]
                if not index[2] or not index[4] or re.sub(r'\s+','',sql).lower()!=re.sub(r'\s+','',SCHEMA_OBJECTS[name]).lower():
                    raise RuntimeError('管理编号唯一索引不符合 schema 5')
            if columns != expected:
                raise RuntimeError(f'数据库列或索引不符合 schema {version}：{name}')

    def _layout(self, connection, history_id):
        model = connection.execute('SELECT d.model FROM devices d JOIN histories h USING(serial) WHERE h.id=?', (history_id,)).fetchone()
        if model is None: raise RuntimeError('下载缺少设备身份')
        return profile(model[0], legacy=connection.execute('PRAGMA user_version').fetchone()[0] < 4)

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
        profile(model)
        self.register_device(serial, model)

    def register_device(self, serial, model):
        serial = serial_number(serial)
        model = canonical_model(model)
        with self.transaction() as connection:
            old = connection.execute('SELECT model FROM devices WHERE serial=?', (serial,)).fetchone()
            if old:
                if model_type(old['model']) != model_type(model):
                    raise RuntimeError('同一序列号的设备型号与已有档案冲突，拒绝覆盖')
            else:
                connection.execute('INSERT INTO devices(serial,model) VALUES(?,?)', (serial, model))
                connection.execute('INSERT INTO histories(serial) VALUES(?)', (serial,))

    @staticmethod
    def metadata_columns(connection):
        names={row[1] for row in connection.execute('PRAGMA table_info(devices)')}
        return ','.join('d.'+key if key in names else "'' AS "+key for key in ('management_code','participant_code'))

    def update_metadata(self, updates, *, expected=None):
        """Apply an already validated batch atomically; audit local changes, never BLE."""
        from .metadata import clean
        updates=[(serial_number(serial),{key:clean(value,key) for key,value in fields.items()}) for serial,fields in updates]
        if len({serial for serial,_ in updates})!=len(updates):raise ValueError('批次中序列号重复')
        with self.transaction() as c:
            changed=[]
            for serial,fields in updates:
                row=c.execute('SELECT * FROM devices WHERE serial=?',(serial,)).fetchone()
                if row is None:raise ValueError(f'设备 {serial} 尚未登记，请先核验身份')
                before={key:row[key] for key in ('management_code','participant_code')}
                if expected and serial in expected and any(before[key]!=value for key,value in expected[serial].items()):
                    raise ValueError('资料已被其他操作更新，请刷新本地列表后重新编辑')
                after={**before,**fields}
                if after==before:continue
                owner=c.execute("SELECT serial FROM devices WHERE management_code=? AND management_code<>'' AND serial<>?",(after['management_code'],serial)).fetchone()
                if owner:raise ValueError(f"管理编号 {after['management_code']} 已被 {owner['serial']} 占用")
                if re.match(r'^D[123]-',after['management_code']) and after['management_code'][:2]!=model_type(row['model']):
                    raise ValueError('管理编号的D1/D2/D3前缀与设备型号不符')
                c.execute('UPDATE devices SET management_code=?,participant_code=? WHERE serial=?',
                          (after['management_code'],after['participant_code'],serial))
                stamp=utcnow()
                c.execute("INSERT INTO device_actions(serial,started_utc,finished_utc,requested_json,before_json,after_json,status) VALUES(?,?,?,?,?,?,'verified')",
                          (serial,stamp,stamp,json.dumps({'local_metadata':fields},ensure_ascii=False),json.dumps(before,ensure_ascii=False),json.dumps(after,ensure_ascii=False)))
                changed.append(serial)
            return {'updated':len(changed),'serials':changed}

    def _pending(self, connection, history):
        return self._pending_state(connection, history, collect=True)

    def _pending_state(self, connection, history, *, collect=False):
        """Verify all recovery boundaries; only download callers retain decoded rows."""
        cursor = history['next_cursor'] or 0
        rows, count = [], 0
        transfers = connection.execute("SELECT * FROM transfers WHERE history_id=? AND status IN ('receiving','interrupted') ORDER BY id", (history['id'],))
        for transfer in transfers:
            if transfer['requested_cursor'] != cursor:
                raise RuntimeError('待确认下载游标不连续')
            for records, _ in self._iter_blocks(connection, transfer['id']):
                if collect: rows.extend(records)
                count += len(records)
                cursor += len(records)
        return (rows if collect else count), cursor

    def _iter_blocks(self, connection, transfer_id):
        transfer = connection.execute('SELECT history_id,decoder_version FROM transfers WHERE id=?', (transfer_id,)).fetchone()
        if transfer is None: raise RuntimeError('下载会话不存在')
        layout = self._layout(connection, transfer['history_id'])
        if transfer['decoder_version'] != layout.decoder: raise RuntimeError('下载解析器与型号不一致')
        width = layout.record_bytes
        for sequence, block in enumerate(connection.execute('SELECT * FROM pending_blocks WHERE transfer_id=? ORDER BY sequence', (transfer_id,))):
            payload = block['payload']
            if block['sequence'] != sequence or hashlib.sha256(payload).hexdigest() != block['sha256']:
                raise RuntimeError('待确认块的序号或摘要校验失败')
            if len(payload) % width: raise RuntimeError('待确认记录长度与设备型号不一致')
            records = [layout.decode(payload[i:i+width], block=sequence, position=i//width) for i in range(0, len(payload), width)]
            yield records, {'block': sequence, 'records': len(records)}

    def _blocks(self, connection, transfer_id):
        rows, meta = [], []
        for records, block in self._iter_blocks(connection, transfer_id):
            rows.extend(records); meta.append(block)
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
                VALUES(?,?,?,?,?,'receiving')''', (history['id'], requested_cursor, expected_count, utcnow(), self._layout(connection, history['id']).decoder)).lastrowid

    def append_block(self, transfer_id, rows, blocks):
        if len(blocks) != 1 or not rows or blocks[0]['records'] != len(rows):
            raise RuntimeError('无效的接收块')
        sequence = blocks[0]['block']
        with self.transaction() as connection:
            transfer = connection.execute('SELECT * FROM transfers WHERE id=?', (transfer_id,)).fetchone()
            if transfer is None or transfer['status'] != 'receiving':
                raise RuntimeError('下载会话不是接收状态')
            layout = self._layout(connection, transfer['history_id'])
            encoded = checked_rows(rows, model=layout)
            if any(d['block'] != sequence or d['position_in_block'] != i for i, (_, d) in enumerate(encoded)):
                raise RuntimeError('接收块内的位置不连续')
            payload = b''.join(raw for raw, _ in encoded)
            last_block = connection.execute('SELECT sequence FROM pending_blocks WHERE transfer_id=? ORDER BY sequence DESC LIMIT 1', (transfer_id,)).fetchone()
            if sequence != (last_block[0] + 1 if last_block else 0):
                raise RuntimeError('接收块序号不连续')
            history = connection.execute('SELECT * FROM histories WHERE id=?', (transfer['history_id'],)).fetchone()
            prior = connection.execute("""SELECT b.payload,b.sha256 FROM pending_blocks b JOIN transfers t ON t.id=b.transfer_id
                WHERE t.history_id=? AND t.status IN ('receiving','interrupted') ORDER BY t.id DESC,b.sequence DESC LIMIT 1""", (history['id'],)).fetchone()
            if prior:
                if hashlib.sha256(prior['payload']).hexdigest() != prior['sha256']:
                    raise RuntimeError('待确认块摘要校验失败')
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
                next_cursor=next_cursor, elapsed_seconds=elapsed_seconds, maxrows=maxrows, allow_clock_changes=True)
            history = connection.execute('SELECT * FROM histories WHERE id=?', (transfer['history_id'],)).fetchone()
            rows, cursor = self._pending(connection, history)
            if history['next_cursor'] is not None and cursor != next_cursor:
                raise RuntimeError('正式游标与累计待确认记录数不一致')
            first = next_cursor - len(rows)
            self._insert(connection, history['id'], first, checked_rows(rows, model=self._layout(connection, history['id'])))
            connection.execute('UPDATE histories SET first_cursor=coalesce(first_cursor,?), next_cursor=? WHERE id=?', (first, next_cursor, history['id']))
            connection.execute("UPDATE transfers SET status='completed', finished_utc=? WHERE history_id=? AND status IN ('receiving','interrupted')", (utcnow(), history['id']))
            connection.execute('UPDATE transfers SET final_cursor=?,validation_json=? WHERE id=?', (next_cursor, json.dumps(validation), transfer_id))
            connection.execute('DELETE FROM pending_blocks WHERE transfer_id IN (SELECT id FROM transfers WHERE history_id=?)', (history['id'],))
        return validation

    def _insert(self, connection, history_id, first, encoded):
        connection.executemany('INSERT INTO observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
            ((history_id, first+i, d['timestamp_device_local'], *(d[k] for k in MEASUREMENTS),
              d['unknown_byte'], d['block'], d['position_in_block'], raw) for i, (raw, d) in enumerate(encoded)))

    def import_rows(self, serial, model, rows, next_cursor, provenance):
        encoded = checked_rows(rows)
        first = next_cursor - len(rows)
        if not rows or first < 0:
            raise RuntimeError('导入记录数量或游标无效')
        if not serial.isascii() or not serial.isdigit() or not 1 <= len(serial) <= 20 or model_type(model) != 'D3':
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
            stats = connection.execute('SELECT count(*), (SELECT timestamp_device_local FROM observations WHERE history_id=? ORDER BY absolute_cursor LIMIT 1), (SELECT timestamp_device_local FROM observations WHERE history_id=? ORDER BY absolute_cursor DESC LIMIT 1) FROM observations WHERE history_id=?', (history['id'], history['id'], history['id'])).fetchone()
            return {**dict(identity), **dict(history), 'row_count': stats[0], 'first_timestamp': stats[1], 'last_timestamp': stats[2]}

    def status(self, serial=None):
        result = []
        with self.connect() as connection:
            connection.execute('BEGIN')
            statement = 'SELECT h.*, d.model,d.alias,'+self.metadata_columns(connection)+' FROM histories h JOIN devices d USING(serial)'
            values = ()
            if serial is not None:
                statement += ' WHERE h.serial=?'
                values = (serial_number(serial),)
            histories = connection.execute(statement + ' ORDER BY serial,generation', values).fetchall()
            for h in histories:
                count, earliest, latest = connection.execute('SELECT count(*),(SELECT timestamp_device_local FROM observations WHERE history_id=? ORDER BY absolute_cursor LIMIT 1),(SELECT timestamp_device_local FROM observations WHERE history_id=? ORDER BY absolute_cursor DESC LIMIT 1) FROM observations WHERE history_id=?', (h['id'], h['id'], h['id'])).fetchone()
                item = {'serial': h['serial'], 'model': h['model'], 'alias':h['alias'],
                        'management_code':h['management_code'],'participant_code':h['participant_code'], 'history_id': h['id'],
                        'committed_records': count, 'database_records': count,
                        'first_cursor': h['first_cursor'], 'next_cursor': h['next_cursor'],
                        'first_timestamp': earliest, 'last_timestamp': latest,
                        'database': str(self.path), 'identity_verified': True,
                        **capabilities(h['model'])}
                try:
                    if h['next_cursor'] is None:
                        pending = connection.execute("SELECT coalesce(sum(length(payload)/?),0) FROM pending_blocks b JOIN transfers t ON t.id=b.transfer_id WHERE history_id=? AND t.status IN ('receiving','interrupted')", (profile(h['model']).record_bytes, h['id'])).fetchone()[0]
                        attempts = connection.execute("SELECT count(*) FROM transfers WHERE history_id=? AND status IN ('receiving','interrupted','superseded')", (h['id'],)).fetchone()[0]
                        resume = 0
                    else:
                        pending, resume = self._pending_state(connection, h)
                        attempts = 0
                    retained = connection.execute("SELECT coalesce(sum(length(payload)/?),0) FROM pending_blocks b JOIN transfers t ON t.id=b.transfer_id WHERE t.history_id=? AND t.status='superseded'", (profile(h['model']).record_bytes, h['id'])).fetchone()[0]
                    item.update(pending_records=pending, initial_attempts=attempts, resume_cursor=resume,
                                superseded_records=retained, resumable_records=0 if h['next_cursor'] is None else pending)
                except (RuntimeError, ValueError) as exc:
                    item.update(error=str(exc), pending_records=None, initial_attempts=0)
                result.append(item)
        return result

    @contextmanager
    def _query_cursor(self, serial, *, limit=None, since=None, until=None, end_exclusive=False):
        conditions, values = ['h.serial=?'], [serial_number(serial)]
        if since:
            conditions.append('o.timestamp_device_local>=?'); values.append(since)
        if until:
            conditions.append('o.timestamp_device_local' + ('<?' if end_exclusive else '<=?')); values.append(until)
        sql = 'SELECT o.*,h.generation FROM observations o JOIN histories h ON h.id=o.history_id WHERE ' + ' AND '.join(conditions)
        sql += ' ORDER BY h.generation ' + ('DESC' if limit else 'ASC') + ',absolute_cursor ' + ('DESC' if limit else 'ASC')
        if limit:
            sql += ' LIMIT ?'; values.append(limit)
        with self.connect() as connection:
            connection.execute('BEGIN')
            self._history(connection, serial)
            yield connection.execute(sql, values)

    @staticmethod
    def _record(serial, row):
        return dict(serial=serial, history_generation=row['generation'], absolute_cursor=row['absolute_cursor'],
                    device_sequence=row['absolute_cursor']+1,
                    **{key: row[key] for key in FIELDS if key != 'record_hex'}, record_hex=row['record_raw'].hex())

    def iter_query(self, serial, *, since=None, until=None, end_exclusive=False):
        """Stream one stable read snapshot; callers may close early on cancellation."""
        with self._query_cursor(serial, since=since, until=until, end_exclusive=end_exclusive) as cursor:
            while batch := cursor.fetchmany(1000):
                for row in batch:
                    yield self._record(serial, row)

    def query(self, serial, *, limit=None, since=None, until=None, end_exclusive=False):
        if not limit:
            return list(self.iter_query(serial, since=since, until=until, end_exclusive=end_exclusive))
        with self._query_cursor(serial, limit=limit, since=since, until=until, end_exclusive=end_exclusive) as cursor:
            records = cursor.fetchall()
        return [self._record(serial, row) for row in reversed(records)]

    def check(self):
        with self.connect() as connection:
            connection.execute('BEGIN')
            integrity = [r[0] for r in connection.execute('PRAGMA integrity_check')]
            if integrity != ['ok'] or connection.execute('PRAGMA foreign_key_check').fetchall():
                raise RuntimeError('数据库完整性或外键检查失败')
            self._check_schema(connection)
            for h in connection.execute('SELECT h.*,d.model FROM histories h JOIN devices d USING(serial)'):
                cursor = connection.execute('SELECT * FROM observations WHERE history_id=? ORDER BY absolute_cursor', (h['id'],))
                layout = self._layout(connection, h['id'])
                expected = h['first_cursor']
                while batch := cursor.fetchmany(1000):
                    if h['next_cursor'] is None: raise RuntimeError('未初始化历史包含正式记录')
                    for row in batch:
                        if row['absolute_cursor'] != expected: raise RuntimeError('历史游标不连续')
                        expected += 1
                    for _ in iter_checked_rows((dict(r, record_hex=r['record_raw'].hex()) for r in batch), model=layout):
                        pass
                if expected != h['next_cursor']: raise RuntimeError('历史游标不连续')
                self._pending_state(connection, h)
                for t in connection.execute('SELECT id,decoder_version,status FROM transfers WHERE history_id=?', (h['id'],)):
                    for _ in self._iter_blocks(connection, t['id']): pass
                    if t['decoder_version'] != layout.decoder: raise RuntimeError('解析器版本不受支持')
                    if t['status'] in ('completed','imported') and connection.execute('SELECT 1 FROM pending_blocks WHERE transfer_id=?', (t['id'],)).fetchone():
                        raise RuntimeError('完成的下载仍含待确认数据')
            if connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='device_actions'").fetchone():
                for action in connection.execute('SELECT * FROM device_actions'):
                    for field in ('requested_json','before_json','after_json'):
                        if action[field] is not None and not isinstance(json.loads(action[field]),dict):
                            raise RuntimeError('设备配置审计 JSON 格式无效')
                    if action['status']=='verified' and (action['after_json'] is None or action['finished_utc'] is None or action['error'] is not None):
                        raise RuntimeError('已验证配置缺少完成证据')
                    if action['status']=='failed_or_unknown' and (action['finished_utc'] is None or action['error'] is None):
                        raise RuntimeError('失败配置缺少结果信息')
        return self.summary()

    def summary(self):
        devices = self.status()
        with self.connect() as connection:
            sessions = connection.execute('SELECT count(*) FROM transfers').fetchone()[0]
        with self.connect() as connection:
            version = connection.execute('PRAGMA user_version').fetchone()[0]
            journal = connection.execute('PRAGMA journal_mode').fetchone()[0]
        return {'database': str(self.path), 'schema_version': version, 'journal_mode': journal,
                'records': sum(d['database_records'] for d in devices), 'transfers': sessions, 'devices': devices}

    def overview(self):
        with self.connect() as connection:
            statement = 'SELECT * FROM device_overview ORDER BY serial' if connection.execute("SELECT 1 FROM sqlite_master WHERE type='view' AND name='device_overview'").fetchone() else SCHEMA[-1].split(' AS\n',1)[1].replace('d.management_code,d.participant_code,','')+' ORDER BY d.serial'
            return [dict(row) for row in connection.execute(statement)]

    def actions(self, serial, limit=20):
        if not 1 <= limit <= 1000: raise ValueError('审计条数必须为 1–1000')
        with self.connect() as c:
            if not c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='device_actions'").fetchone(): return []
            result=[]
            for row in c.execute('SELECT * FROM device_actions WHERE serial=? ORDER BY id DESC LIMIT ?', (serial_number(serial),limit)):
                item=dict(row)
                for field in ('requested_json','before_json','after_json'):
                    raw=item.pop(field)
                    item[field.removesuffix('_json')]=json.loads(raw) if raw is not None else None
                result.append(item)
            return result

    def begin_action(self, serial, changes, before):
        with self.transaction() as c:
            return c.execute("INSERT INTO device_actions(serial,started_utc,requested_json,before_json,status) VALUES(?,?,?,?,'started')",
                             (serial,utcnow(),json.dumps(changes,ensure_ascii=False),json.dumps(before,ensure_ascii=False))).lastrowid

    def finish_action(self, action, *, after=None, error=None):
        if error is None and after is None: raise ValueError('成功的配置必须有回读结果')
        with self.transaction() as c:
            updated=c.execute('UPDATE device_actions SET finished_utc=?,after_json=?,error=?,status=? WHERE id=? AND status=\'started\'',
                      (utcnow(),json.dumps(after,ensure_ascii=False) if after is not None else None,error,
                       'verified' if error is None else 'failed_or_unknown',action))
            if updated.rowcount!=1: raise RuntimeError('配置审计不可重复结束或覆盖')

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
