"""Schema 2: transaction boundaries, read purity, snapshots and migration."""
import asyncio
from contextlib import redirect_stderr, redirect_stdout, closing
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import select
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from kestrel_drop.database import Store, APPLICATION_ID
from kestrel_drop.manager import Library
from kestrel_drop import protocol, storage
from test_manager import FakeBLE, record


def rows(minutes=(20, 25, 25)):
    return [protocol.decode_record(record(m), block=0, position=i) for i, m in enumerate(minutes)]


def fixture(root, name='source.csv', serial='2866855'):
    source = root / name
    protocol.write_csv(source, rows())
    Path(str(source) + '.json').write_text(json.dumps({
        'serial': serial, 'model': 'Kestrel DROP 3', 'rows': 3,
        'first_record_time': rows()[0]['timestamp_device_local'],
        'last_record_time': rows()[-1]['timestamp_device_local'],
        'validation': {'next_cursor': 103, 'inferred_first_cursor': 100}}))
    return source


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.library = Library(self.root / 'archive')
        self.source = fixture(self.root)
        self.library.import_archive(self.source)

    def download(self, fake, library=None):
        async def resolve(_): return object()
        with fake.patches(), patch.object(protocol, 'resolve_ble_device', resolve), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            return asyncio.run((library or self.library).download('2866855', execute=True))

    def pending(self):
        tid = self.library.store.begin_transfer('2866855', 103, 1)
        decoded = rows((30,))
        self.library.store.append_block(tid, decoded, [{'block': 0, 'records': 1}])
        return tid, decoded

    def test_typed_schema_has_no_duplicate_json_or_csv_authority(self):
        with self.library.store.connect() as c:
            tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            self.assertEqual(tables, {'devices','histories','transfers','pending_blocks','observations','device_actions'})
            self.assertNotIn('row_json', {r[1] for r in c.execute('PRAGMA table_info(observations)')})
        self.assertFalse(list(self.library.root.rglob('*.csv')))
        self.assertEqual(self.library.database_info()['records'], 3)
        self.assertEqual([r['absolute_cursor'] for r in self.library.records('2866855')], [100,101,102])

    def test_query_status_and_integrity_check_are_read_only(self):
        before = self.library.database_path.read_bytes()
        stamp = self.library.database_path.stat().st_mtime_ns
        with patch.object(protocol, '_rows_checked', side_effect=AssertionError('must not scan CSV')):
            for _ in range(3):
                self.library.records('2866855'); self.library.status(); self.library.database_info()
        self.assertEqual(before, self.library.database_path.read_bytes())
        self.assertEqual(stamp, self.library.database_path.stat().st_mtime_ns)
        with self.library.store.connect() as c:
            with self.assertRaises(sqlite3.OperationalError): c.execute('DELETE FROM observations')

    def test_pending_excluded_then_published_once_with_cleanup(self):
        tid, decoded = self.pending()
        self.assertEqual(len(self.library.records('2866855')), 3)
        self.assertEqual(self.library.store.resume('2866855')[0], 104)
        self.library.store.finish_transfer(tid, next_cursor=104, elapsed_seconds=1, maxrows=10, received_rows=decoded)
        self.assertEqual(len(self.library.records('2866855')), 4)
        with self.library.store.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM pending_blocks').fetchone()[0],0)
        with self.assertRaises(RuntimeError):
            self.library.store.finish_transfer(tid, next_cursor=104, elapsed_seconds=1, maxrows=10, received_rows=decoded)

    def test_final_publish_failure_rolls_back_observations_cursor_and_cleanup(self):
        tid, decoded = self.pending()
        with self.library.store.transaction() as c:
            c.execute("CREATE TRIGGER fail_publish BEFORE DELETE ON pending_blocks BEGIN SELECT RAISE(ABORT,'disk full'); END")
        with self.assertRaises(sqlite3.IntegrityError):
            self.library.store.finish_transfer(tid, next_cursor=104, elapsed_seconds=1, maxrows=10, received_rows=decoded)
        self.assertEqual(self.library.store.device('2866855')['next_cursor'], 103)
        self.assertEqual(self.library.store.resume('2866855')[0], 104)
        self.assertEqual(len(self.library.records('2866855')), 3)
        with self.library.store.transaction() as c: c.execute('DROP TRIGGER fail_publish')
        self.library.store.finish_transfer(tid, next_cursor=104, elapsed_seconds=1, maxrows=10, received_rows=decoded)
        self.assertEqual(self.library.database_info()['records'], 4)

    def test_block_commit_precedes_device_ack(self):
        fake = FakeBLE([record(30)], 104, interrupt=True)
        original = fake.module.BleakClient
        owner = self
        checked = []
        class Client(original):
            async def write_gatt_char(self, uuid, packet, response):
                frame = protocol.FrameStream().feed(packet)[0]
                if frame.kind == 4 and frame.payload == b'\xff\xff' and self.phase == 1:
                    # Independent read-only connection sees the durable commit.
                    checked.append(Store(owner.library.database_path).resume('2866855')[0])
                return await super().write_gatt_char(uuid, packet, response)
        fake.module.BleakClient = Client
        with self.assertRaises(asyncio.CancelledError): self.download(fake)
        self.assertEqual(checked, [104])

    def test_failed_block_transaction_is_not_acknowledged_or_resumed(self):
        with self.library.store.transaction() as c:
            c.execute("CREATE TRIGGER fail_block BEFORE INSERT ON pending_blocks BEGIN SELECT RAISE(ABORT,'disk full'); END")
        fake = FakeBLE([record(30)], 104)
        with self.assertRaises(sqlite3.IntegrityError): self.download(fake)
        self.assertEqual(self.library.store.resume('2866855')[0],103)
        # Schema ACK only, no record ACK after failed commit.
        self.assertEqual(len([f for f in fake.writes if f.kind == 4 and f.payload == b'\xff\xff']),2)

    def test_backup_with_pending_restores_and_resumes_without_other_files(self):
        self.pending()
        backup = self.library.backup(self.root / 'backup.sqlite')
        restored_root = self.root / 'restored'
        storage.restore(backup, restored_root)
        restored = Library(restored_root)
        self.assertEqual(restored.status()[0]['pending_records'],1)
        self.assertEqual(self.download(FakeBLE([record(35)],105),restored),0)
        self.assertEqual(len(restored.records('2866855')),5)
        self.assertEqual(restored.status()[0]['pending_records'],0)
        self.assertEqual(self.library.status()[0]['database_records'],3)
        with self.assertRaises(FileExistsError): storage.restore(backup, restored_root)
        with self.assertRaises(FileExistsError): self.library.backup(backup)

    def test_backup_reads_a_live_committed_wal_snapshot(self):
        with self.library.store.connect(writable=True) as c:
            c.execute('PRAGMA journal_mode=WAL')
            with c: c.execute("UPDATE devices SET alias='field unit'")
            backup = self.library.backup(self.root / 'wal.sqlite')
            with Store(backup).connect() as copied:
                self.assertEqual(copied.execute('SELECT alias FROM devices').fetchone()[0], 'field unit')

    def test_backup_ignores_unrelated_legacy_csv(self):
        (self.library.root / 'bad.csv').write_text('broken')
        with patch.object(self.library, 'status', side_effect=AssertionError('no reconciliation')):
            self.library.backup(self.root / 'backup.sqlite')

    def test_export_identity_duplicates_and_date_boundaries(self):
        destination = self.root / 'day.csv'
        self.assertEqual(self.library.export('2866855',destination,since='2026-10-22',until='2026-10-22'),3)
        with destination.open(encoding='utf-8-sig') as fd: saved=list(csv.DictReader(fd))
        self.assertEqual([r['absolute_cursor'] for r in saved],['100','101','102'])
        self.assertEqual(saved[1]['timestamp_device_local'],saved[2]['timestamp_device_local'])
        self.assertEqual(saved[0]['history_generation'],'1')
        destination.unlink()
        self.assertEqual(self.library.database_info()['records'],3)

    def test_integrity_check_detects_changed_measurement_and_raw_bytes(self):
        with self.library.store.transaction() as c: c.execute('UPDATE observations SET temperature_c=999 WHERE absolute_cursor=100')
        with self.assertRaisesRegex(RuntimeError,'测量字段'): self.library.database_info()
        with self.assertRaises(RuntimeError): self.library.backup(self.root / 'bad.sqlite')
        self.assertFalse((self.root/'bad.sqlite').exists())

    def test_pending_checksum_mutation_is_refused(self):
        self.pending()
        with self.library.store.transaction() as c: c.execute("UPDATE pending_blocks SET sha256='wrong'")
        with self.assertRaisesRegex(RuntimeError,'摘要'): self.library.store.resume('2866855')

    def test_pending_session_cursor_mutation_is_refused(self):
        tid,_=self.pending()
        with self.library.store.transaction() as c: c.execute('UPDATE transfers SET requested_cursor=999 WHERE id=?',(tid,))
        with self.assertRaisesRegex(RuntimeError,'不连续'): self.library.store.resume('2866855')

    def test_wrong_final_cursor_and_received_payload_remain_unpublished(self):
        tid,decoded=self.pending()
        for final,payload in ((999,decoded),(104,rows((35,)))):
            with self.assertRaises(RuntimeError):
                self.library.store.finish_transfer(tid,next_cursor=final,elapsed_seconds=1,maxrows=10,received_rows=payload)
        self.assertEqual(self.library.status()[0]['pending_records'],1)

    def test_repeated_noop_sync_does_not_accumulate_transfer_rows(self):
        before=self.library.store.summary()['transfers']
        for _ in range(3): self.download(FakeBLE([],103))
        self.assertEqual(self.library.store.summary()['transfers'],before)

    def test_directory_lock_blocks_second_writer_but_not_readers(self):
        with storage.writer_lock(self.library.root):
            self.assertEqual(len(self.library.records('2866855')),3)
            with self.assertRaisesRegex(RuntimeError,'Another download'):
                self.download(FakeBLE([record(30)],104))
        self.assertFalse(list(self.library.root.glob('*.lock')))

    def test_future_or_unrecognized_database_never_overwritten(self):
        for version,app_id in ((99,APPLICATION_ID),(2,0)):
            path=self.root/f'bad-{version}.sqlite'
            with closing(sqlite3.connect(path)) as c, c:
                c.execute(f'PRAGMA user_version={version}'); c.execute(f'PRAGMA application_id={app_id}')
            before=path.read_bytes()
            with self.assertRaises(RuntimeError): Store(path,create=True)
            self.assertEqual(path.read_bytes(),before)

    def test_process_kill_rolls_back_uncommitted_block(self):
        tid=self.library.store.begin_transfer('2866855',103,1)
        code='''import sys,time,hashlib
from pathlib import Path
from kestrel_drop.database import Store
s=Store(Path(sys.argv[1]))
with s.transaction() as c:
 raw=bytes.fromhex(sys.argv[3]); c.execute('INSERT INTO pending_blocks VALUES(?,?,?,?)',(int(sys.argv[2]),0,raw,hashlib.sha256(raw).hexdigest()))
 print('inside',flush=True); time.sleep(30)
'''
        process=subprocess.Popen([sys.executable,'-B','-c',code,str(self.library.database_path),str(tid),record(30).hex()],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            self.assertTrue(select.select([process.stdout],[],[],5)[0]); self.assertEqual(process.stdout.readline().strip(),'inside')
        finally: process.kill(); process.communicate(timeout=5)
        self.assertEqual(self.library.store.resume('2866855')[0],103)
        self.assertEqual(self.library.database_info()['records'],3)

    def test_process_kill_after_durable_block_resumes_at_boundary(self):
        tid=self.library.store.begin_transfer('2866855',103,1)
        code='''import sys,time
from pathlib import Path
from kestrel_drop.database import Store
from kestrel_drop.protocol import decode_record
s=Store(Path(sys.argv[1])); s.append_block(int(sys.argv[2]),[decode_record(bytes.fromhex(sys.argv[3]),block=0,position=0)],[{'block':0,'records':1}])
print('durable',flush=True); time.sleep(30)
'''
        process=subprocess.Popen([sys.executable,'-B','-c',code,str(self.library.database_path),str(tid),record(30).hex()],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            self.assertTrue(select.select([process.stdout],[],[],5)[0]); self.assertEqual(process.stdout.readline().strip(),'durable')
        finally: process.kill(); process.communicate(timeout=5)
        self.assertEqual(self.library.store.resume('2866855')[0],104)
        self.download(FakeBLE([record(35)],105))
        self.assertEqual(self.library.database_info()['records'],5)

    def test_uncommitted_source_write_does_not_enter_backup(self):
        with self.library.store.connect(writable=True) as c:
            c.execute('BEGIN IMMEDIATE')
            c.execute("UPDATE devices SET alias='not committed'")
            backup = self.library.backup(self.root/'snapshot.sqlite')
            with Store(backup).connect() as target:
                self.assertEqual(target.execute('SELECT alias FROM devices').fetchone()[0], '')
            c.rollback()

    def test_two_devices_preserve_identity_at_same_cursor(self):
        other=fixture(self.root,'other.csv',serial='123')
        self.library.import_archive(other)
        self.assertEqual(self.library.database_info()['records'],6)
        self.assertEqual(len(self.library.records('123')),3)
        self.assertEqual(len(self.library.records('2866855')),3)

    def test_sqlite_busy_rolls_back_without_acknowledging_block(self):
        # A writer outside the app lock still cannot interleave SQLite commits.
        tid=self.library.store.begin_transfer('2866855',103,1)
        with self.library.store.connect(writable=True) as c:
            c.execute('BEGIN IMMEDIATE')
            real=sqlite3.connect
            with patch('kestrel_drop.database.sqlite3.connect', wraps=real) as connect:
                def immediate(*args, **kwargs):
                    kwargs['timeout']=0
                    return real(*args, **kwargs)
                connect.side_effect=immediate
                with self.assertRaises(sqlite3.OperationalError):
                    self.library.store.append_block(tid,rows((30,)),[{'block':0,'records':1}])
            c.rollback()
        self.assertEqual(self.library.store.resume('2866855')[0],103)

    def test_process_kill_during_final_transaction_preserves_old_publication(self):
        tid,decoded=self.pending()
        code='''import sys,time
from pathlib import Path
from kestrel_drop.database import Store
from kestrel_drop.protocol import decode_record
s=Store(Path(sys.argv[1])); original=s._insert
def insert(*args):
 original(*args); print('inserted-uncommitted',flush=True); time.sleep(30)
s._insert=insert
s.finish_transfer(int(sys.argv[2]),next_cursor=104,elapsed_seconds=1,maxrows=100,received_rows=[decode_record(bytes.fromhex(sys.argv[3]),block=0,position=0)])
'''
        process=subprocess.Popen([sys.executable,'-B','-c',code,str(self.library.database_path),str(tid),record(30).hex()],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            self.assertTrue(select.select([process.stdout],[],[],5)[0])
            self.assertEqual(process.stdout.readline().strip(),'inserted-uncommitted')
        finally: process.kill(); process.communicate(timeout=5)
        self.assertEqual(self.library.store.device('2866855')['next_cursor'],103)
        self.assertEqual(self.library.database_info()['records'],3)
        self.assertEqual(self.library.store.resume('2866855')[0],104)
        self.library.store.finish_transfer(tid,next_cursor=104,elapsed_seconds=1,maxrows=10,received_rows=decoded)
        self.assertEqual(self.library.database_info()['records'],4)



class MigrationTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.source=fixture(self.root)
        protocol.adopt_archive(self.source,Path(str(self.source)+'.json'))
        self.state=protocol._state_loaded(protocol._archive_paths(self.source))
        self.original={str(p.relative_to(self.root)):p.read_bytes() for p in self.root.iterdir() if p.is_file()}

    def add_pending(self, *, trusted=True):
        paths=protocol._archive_paths(self.source)
        protocol._new_pending(paths,self.state)
        protocol.BlockSpool(paths['pending'],self.state['fieldnames']).append(rows((30,)),[{'block':0,'records':1}])
        if not trusted:
            journal=json.loads(paths['journal'].read_text()); journal.pop('spool_checkpoint'); journal.pop('transfer_sessions')
            paths['journal'].write_text(json.dumps(journal))

    def test_legacy_migration_freezes_sources_and_is_idempotent(self):
        result=storage.migrate(self.root)
        self.assertEqual((result['records'],result['integrity']),(3,'ok'))
        frozen=Path(result['legacy'])/'source'
        for name,content in self.original.items():
            self.assertEqual((frozen/name).read_bytes(),content)
            self.assertFalse((self.root/name).exists())
        self.assertEqual(storage.migrate(self.root)['records'],3)
        self.assertEqual(len(list((self.root/'legacy').glob('migration-*'))),1)
        self.assertEqual([r['record_hex'] for r in Library(self.root).records('2866855')],[r['record_hex'] for r in rows()])

    def test_trusted_pending_migrates_and_is_resumeable(self):
        self.add_pending()
        storage.migrate(self.root)
        library=Library(self.root)
        self.assertEqual(library.store.resume('2866855')[0],104)
        fake=FakeBLE([record(35)],105)
        async def resolve(_): return object()
        with fake.patches(),patch.object(protocol,'resolve_ble_device',resolve),redirect_stdout(io.StringIO()):
            asyncio.run(library.download('2866855',execute=True))
        self.assertEqual(library.database_info()['records'],5)

    def test_untrusted_pending_aborts_without_changing_source(self):
        self.add_pending(trusted=False)
        before={p:p.read_bytes() for p in self.root.iterdir() if p.is_file()}
        with self.assertRaisesRegex(RuntimeError,'Legacy pending'): storage.migrate(self.root)
        for path,content in before.items(): self.assertEqual(path.read_bytes(),content)
        self.assertFalse((self.root/'library.sqlite').exists())

    def test_bad_csv_aborts_without_publishing_database(self):
        self.source.write_text(self.source.read_text().replace('14:20:00','14:21:00'))
        before=self.source.read_bytes()
        with self.assertRaises(RuntimeError): storage.migrate(self.root)
        self.assertEqual(self.source.read_bytes(),before)
        self.assertFalse((self.root/'library.sqlite').exists())

    def test_fault_before_database_switch_preserves_sources_and_retry_works(self):
        real_replace=os.replace
        def replace(source,target):
            if Path(target).resolve()==(self.root/'library.sqlite').resolve(): raise OSError('simulated interruption')
            return real_replace(source,target)
        with patch.object(storage.os,'replace',replace):
            with self.assertRaises(OSError): storage.migrate(self.root)
        for name,content in self.original.items(): self.assertEqual((self.root/name).read_bytes(),content)
        self.assertEqual(storage.migrate(self.root)['records'],3)

    def test_fault_after_switch_cleanup_retries_without_reimport(self):
        with patch.object(storage,'_retire',side_effect=OSError('cleanup interrupted')):
            with self.assertRaises(OSError): storage.migrate(self.root)
        self.assertEqual(Library(self.root).database_info()['records'],3)
        self.assertEqual(storage.migrate(self.root)['records'],3)
        self.assertFalse(self.source.exists())

    def test_reading_or_downloading_legacy_data_requires_explicit_migration(self):
        library=Library(self.root)
        with self.assertRaisesRegex(RuntimeError,'migrate'): library.status()
        fake=FakeBLE([],0)
        with fake.patches():
            with self.assertRaisesRegex(RuntimeError,'migrate'): asyncio.run(library.download('2866855',execute=True))
        self.assertFalse((self.root/'library.sqlite').exists())

    def test_settings_priority_and_persistence(self):
        config=self.root/'settings.json'
        with patch.dict(os.environ,{'KESTREL_CONFIG_FILE':str(config)}):
            os.environ.pop('KESTREL_DATA_DIR',None)
            chosen=self.root/'chosen'
            storage.configure(chosen)
            self.assertEqual(storage.default_root(),chosen.resolve())
            with patch.dict(os.environ,{'KESTREL_DATA_DIR':str(self.root/'override')}):
                self.assertEqual(storage.default_root(),self.root/'override')

    def make_v1(self, *, database_only=False):
        path=self.root/'library.sqlite'
        with closing(sqlite3.connect(path)) as c, c:
            c.execute('PRAGMA user_version=1')
            c.execute('CREATE TABLE devices(serial TEXT,model TEXT,first_cursor INTEGER,next_cursor INTEGER,row_count INTEGER)')
            c.execute("INSERT INTO devices VALUES('2866855','Kestrel DROP 3',100,103,3)")
            c.execute('''CREATE TABLE observations(serial TEXT,absolute_cursor INTEGER,record_raw BLOB,row_json TEXT,
                timestamp_device_local TEXT,temperature_c REAL,relative_humidity_pct REAL,station_pressure_hpa REAL,
                heat_index_c REAL,dew_point_c REAL,density_altitude_m_raw REAL,density_altitude_m_official_integer_estimate INTEGER,
                unknown_byte INTEGER,block INTEGER,position_in_block INTEGER)''')
            names,old_rows=protocol._rows_checked(self.source)
            from kestrel_drop.database import MEASUREMENTS
            for i,row in enumerate(old_rows):
                d=protocol.decode_record(bytes.fromhex(row['record_hex']),block=int(row['block']),position=int(row['position_in_block']))
                c.execute('INSERT INTO observations VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    ('2866855',100+i,bytes.fromhex(row['record_hex']),json.dumps(row),d['timestamp_device_local'],
                     *(d[k] for k in MEASUREMENTS),d['unknown_byte'],d['block'],d['position_in_block']))
        if database_only:
            for p in list(self.root.iterdir()):
                if p != path: p.unlink()
        return path

    def test_v1_database_and_csv_migrate_with_byte_identical_backup(self):
        path=self.make_v1()
        with closing(sqlite3.connect(path)) as c, c: before=c.execute('SELECT absolute_cursor,record_raw FROM observations ORDER BY absolute_cursor').fetchall()
        result=storage.migrate(self.root)
        with closing(sqlite3.connect(Path(result['legacy'])/'library-v1.sqlite')) as c:
            self.assertEqual(c.execute('SELECT absolute_cursor,record_raw FROM observations ORDER BY absolute_cursor').fetchall(),before)
        with Store(path).connect() as c:
            self.assertEqual([tuple(r) for r in c.execute('SELECT absolute_cursor,record_raw FROM observations ORDER BY absolute_cursor')],before)

    def test_v1_database_only_backup_can_be_migrated_and_resumed(self):
        self.make_v1(database_only=True)
        self.assertEqual(storage.migrate(self.root)['records'],3)
        self.assertEqual(Library(self.root).store.resume('2866855')[0],103)

    def test_v1_measurement_conflict_aborts_without_switch(self):
        path=self.make_v1()
        with closing(sqlite3.connect(path)) as c, c: c.execute('UPDATE observations SET temperature_c=999 WHERE absolute_cursor=100')
        with self.assertRaisesRegex(RuntimeError,'冲突'): storage.migrate(self.root)
        with closing(sqlite3.connect(path)) as c, c:
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],1)
        self.assertTrue(self.source.exists())

    def test_completed_legacy_journal_is_recovered_on_copy_only(self):
        self.add_pending()
        paths=protocol._archive_paths(self.source)
        journal=json.loads(paths['journal'].read_text())
        journal['completed']={'next_cursor':104,'validation':{'verification':'complete'}}
        paths['journal'].write_text(json.dumps(journal))
        original=self.source.read_bytes()
        result=storage.migrate(self.root)
        self.assertEqual(result['records'],4)
        self.assertEqual((Path(result['legacy'])/'source'/'source.csv').read_bytes(),original)

    def test_migration_does_not_delete_source_changed_after_publication(self):
        real_retire=storage._retire
        def retire(root,snapshot,manifest):
            self.source.write_text('modified externally')
            return real_retire(root,snapshot,manifest)
        with patch.object(storage,'_retire',retire): result=storage.migrate(self.root)
        self.assertIn('source.csv',result['retained_changed_files'])
        self.assertEqual(self.source.read_text(),'modified externally')
        self.assertEqual(Library(self.root).database_info()['records'],3)

    def test_legacy_writer_lock_prevents_migration(self):
        with protocol._archive_lock(self.source):
            with self.assertRaisesRegex(RuntimeError,'Another download'): storage.migrate(self.root)
        self.assertFalse((self.root/'library.sqlite').exists())

    def test_completed_initial_attempt_can_be_migrated_without_ble(self):
        # No formal archive exists yet; completion metadata establishes cursors.
        initial=self.root/'.downloads'/'2866855'/'attempt'/'history.csv'
        initial.parent.mkdir(parents=True)
        shutil.move(str(self.source),initial)
        shutil.move(str(self.source)+'.json',str(initial)+'.json')
        protocol._archive_paths(self.source)['state'].unlink()
        self.assertEqual(storage.migrate(self.root)['records'],3)
        self.assertEqual(Library(self.root).store.resume('2866855')[0],103)

    def test_unknown_initial_partial_is_frozen_without_guessed_records(self):
        for p in list(self.root.iterdir()): p.unlink()
        initial=self.root/'.downloads'/'2866855'/'attempt'/'history.csv.partial.csv'
        initial.parent.mkdir(parents=True)
        initial.write_bytes(b'unknown raw evidence')
        result=storage.migrate(self.root)
        self.assertEqual(result['records'],0)
        self.assertEqual((Path(result['legacy'])/'source'/initial.relative_to(self.root)).read_bytes(),b'unknown raw evidence')

    def kill_migration(self, after):
        code='''import os,sys,time
from pathlib import Path
from kestrel_drop.storage import migrate
root=Path(sys.argv[1]).resolve(); original=os.replace
def replace(source,target):
 if Path(target)==root/'library.sqlite':
  if sys.argv[2]=='after': original(source,target)
  print('switch-boundary',flush=True); time.sleep(30)
 return original(source,target)
os.replace=replace
migrate(root)
'''
        process=subprocess.Popen([sys.executable,'-B','-c',code,str(self.root),'after' if after else 'before'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try:
            self.assertTrue(select.select([process.stdout],[],[],5)[0])
            self.assertEqual(process.stdout.readline().strip(),'switch-boundary')
        finally: process.kill(); process.communicate(timeout=5)
        self.assertEqual(storage.migrate(self.root)['records'],3)
        self.assertEqual(len(Library(self.root).records('2866855')),3)

    def test_kill_before_database_switch_retries_safely(self): self.kill_migration(False)
    def test_kill_after_database_switch_retries_cleanup_safely(self): self.kill_migration(True)


    def test_database_changed_after_snapshot_is_not_overwritten(self):
        path=self.make_v1()
        original=Store.backup
        def backup(store,destination):
            result=original(store,destination)
            with closing(sqlite3.connect(path)) as c, c:
                c.execute("UPDATE devices SET model='Kestrel DROP 3 modified'")
            return result
        with patch.object(Store,'backup',backup):
            with self.assertRaisesRegex(RuntimeError,'发生变化'): storage.migrate(self.root)
        with closing(sqlite3.connect(path)) as c:
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],1)
            self.assertEqual(c.execute('SELECT model FROM devices').fetchone()[0],'Kestrel DROP 3 modified')
        self.assertTrue(self.source.exists())

    def test_live_legacy_database_reader_prevents_switch(self):
        path=self.make_v1()
        with closing(sqlite3.connect(path)) as reader:
            reader.execute('BEGIN')
            reader.execute('SELECT * FROM devices').fetchall()
            with self.assertRaises((RuntimeError,sqlite3.OperationalError)): storage.migrate(self.root)
        with closing(sqlite3.connect(path)) as c:
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],1)
        self.assertTrue(self.source.exists())



class FinalAckTests(unittest.TestCase):
    setUp = DatabaseTests.setUp
    download = DatabaseTests.download
    def test_final_commit_is_visible_before_final_ack(self):
        fake=FakeBLE([record(30)],104)
        original=fake.module.BleakClient
        owner=self
        checked=[]
        class Client(original):
            async def write_gatt_char(self,uuid,packet,response):
                frame=protocol.FrameStream().feed(packet)[0]
                if frame.kind==4 and frame.payload==b'\x12\x00':
                    info=Library(owner.library.root).status()[0]
                    checked.append((info['next_cursor'],info['database_records'],info['pending_records']))
                return await super().write_gatt_char(uuid,packet,response)
        fake.module.BleakClient=Client
        self.download(fake)
        self.assertEqual(checked,[(104,4,0)])

    def test_failed_final_publication_is_not_acknowledged(self):
        with self.library.store.transaction() as c:
            c.execute("CREATE TRIGGER fail_final BEFORE INSERT ON observations WHEN NEW.absolute_cursor=103 BEGIN SELECT RAISE(ABORT,'disk full'); END")
        fake=FakeBLE([record(30)],104)
        with self.assertRaises(sqlite3.IntegrityError): self.download(fake)
        self.assertFalse(any(f.kind==4 and f.payload==b'\x12\x00' for f in fake.writes))
        self.assertEqual(self.library.store.resume('2866855')[0],104)
        self.assertEqual(len(self.library.records('2866855')),3)

    def test_initial_interruption_backup_restarts_zero_and_discards_staging_only_after_success(self):
        empty=Library(self.root/'initial')
        with self.assertRaises(asyncio.CancelledError): self.download(FakeBLE([record(20)],101,interrupt=True),empty)
        backup=empty.backup(self.root/'initial.sqlite')
        storage.restore(backup,self.root/'initial-restored')
        restored=Library(self.root/'initial-restored')
        fake=FakeBLE([record(20),record(25)],102)
        self.download(fake,restored)
        query=next(f for f in fake.writes if f.kind==0 and f.payload[:2]==b'\x03\x00')
        self.assertEqual(int.from_bytes(query.payload[2:6],'little'),0)
        self.assertEqual(restored.database_info()['records'],2)
        self.assertEqual(restored.status()[0]['pending_records'],0)

    def test_final_ack_send_failure_keeps_committed_records_and_retry_is_idempotent(self):
        fake=FakeBLE([record(30)],104)
        original=fake.module.BleakClient
        class Client(original):
            async def write_gatt_char(self,uuid,packet,response):
                frame=protocol.FrameStream().feed(packet)[0]
                if frame.kind==4 and frame.payload==b'\x12\x00':
                    raise fake.errors.BleakError('disconnected after publication')
                return await super().write_gatt_char(uuid,packet,response)
        fake.module.BleakClient=Client
        with self.assertRaisesRegex(RuntimeError,'蓝牙操作失败'): self.download(fake)
        self.assertEqual(self.library.database_info()['records'],4)
        self.assertEqual(self.library.status()[0]['pending_records'],0)
        self.download(FakeBLE([],104))
        self.assertEqual(self.library.database_info()['records'],4)
        self.assertEqual(self.library.download_results['2866855']['status'],'up_to_date')
