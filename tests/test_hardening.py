"""Bounded full validation, snapshot compatibility and durable result semantics."""
from contextlib import closing, redirect_stdout, redirect_stderr
import hashlib
import io
import json
from pathlib import Path
import sqlite3
import time
import sys
import tempfile
import tracemalloc
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from kestrel_drop import manager, protocol, storage
from kestrel_drop.database import Store
from kestrel_drop.errors import StorageBusy, describe
from kestrel_drop.layouts import profile
from kestrel_drop.ui_actions import retry_download_arguments
from kestrel_drop.worker import Job, validate_event
from test_manager import FakeBLE, RAW
from test_database import fixture, rows
from test_ui_workflow import Screen
from kestrel_drop.tui import Terminal


class StorageHardeningTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.library=manager.Library(self.root/'archive')
        self.library.import_archive(fixture(self.root))
        self.store=self.library.store

    def test_check_has_bounded_memory_and_finds_corruption_after_batch_boundary(self):
        row=profile('D3').decode(RAW,block=0,position=0)
        with self.store.transaction() as c:
            c.execute('DELETE FROM observations')
            history=c.execute('SELECT id FROM histories').fetchone()[0]
            self.store._insert(c,history,0,((RAW,row) for _ in range(20000)))
            c.execute('UPDATE histories SET first_cursor=0,next_cursor=20000')
        tracemalloc.start()
        try:
            result=self.store.check()
            peak=tracemalloc.get_traced_memory()[1]
        finally:tracemalloc.stop()
        self.assertEqual(result['records'],20000)
        self.assertLess(peak,12*1024*1024)
        with self.store.transaction() as c:
            c.execute('UPDATE observations SET temperature_c=999 WHERE absolute_cursor=19999')
        with self.assertRaisesRegex(RuntimeError,'原始字节'):self.store.check()
        with self.assertRaises(RuntimeError):self.library.backup(self.root/'invalid.sqlite')
        self.assertFalse((self.root/'invalid.sqlite').exists())
        self.assertFalse(list(self.root.glob('.kestrel-backup-*')))

    def test_restored_read_is_pure_then_writable_open_enables_wal(self):
        backup=self.library.backup(self.root/'snapshot.sqlite')
        source_hash=hashlib.sha256(backup.read_bytes()).hexdigest()
        destination=self.root/'restored';storage.restore(backup,destination)
        path=destination/'library.sqlite'
        with Store(path).connect() as c:self.assertEqual(c.execute('PRAGMA journal_mode').fetchone()[0],'delete')
        before=path.read_bytes();Store(path).check();self.assertEqual(path.read_bytes(),before)
        writable=Store(path,create=True)
        with writable.connect() as reader:
            self.assertEqual(reader.execute('PRAGMA journal_mode').fetchone()[0],'wal')
            reader.execute('BEGIN');self.assertEqual(reader.execute('SELECT count(*) FROM devices').fetchone()[0],1)
            writable.register_device('2','D1')
            self.assertEqual(reader.execute('SELECT count(*) FROM devices').fetchone()[0],1)
        self.assertEqual(len(writable.status()),2)
        self.assertEqual(hashlib.sha256(backup.read_bytes()).hexdigest(),source_hash)
        writable.backup(self.root/'new-snapshot.sqlite')
        with Store(self.root/'new-snapshot.sqlite').connect() as c:
            self.assertEqual(c.execute('PRAGMA journal_mode').fetchone()[0],'delete')

    def test_check_rejects_missing_current_schema_objects_but_accepts_old_snapshot(self):
        for sql,name in [('DROP VIEW device_overview','device_overview'),
                         ('DROP TABLE device_actions','device_actions'),
                         ('DROP INDEX observations_time','observations_time'),
                         ('DROP INDEX transfers_history','transfers_history')]:
            path=self.root/(name+'.sqlite');self.store.backup(path)
            with closing(sqlite3.connect(path)) as c,c:c.execute(sql)
            broken=Store(path)
            with self.assertRaisesRegex(RuntimeError,name):broken.check()
            with self.assertRaisesRegex(RuntimeError,name):Store(path,create=True)
        path=self.root/'old.sqlite';self.store.backup(path)
        with closing(sqlite3.connect(path)) as c,c:
            c.execute('DROP VIEW device_overview');c.execute('DROP TABLE device_actions')
            c.execute('PRAGMA user_version=3')
            c.execute("UPDATE transfers SET decoder_version='d3-21-v1'")
        # Fixture's heat-index fits both decoder revisions.
        before=path.read_bytes();self.assertEqual(Store(path).check()['records'],3)
        self.assertEqual(path.read_bytes(),before)

    def test_check_rejects_wrong_index_keys_and_missing_column(self):
        with self.store.transaction() as c:
            c.execute('DROP INDEX observations_time')
            c.execute('CREATE INDEX observations_time ON observations(history_id,block)')
        with self.assertRaisesRegex(RuntimeError,'observations_time'):self.store.check()
        path=self.root/'columns.sqlite';fresh=Store(path,create=True)
        with fresh.transaction() as c:c.execute('ALTER TABLE devices RENAME COLUMN alias TO accidental')
        with self.assertRaisesRegex(RuntimeError,'devices|device_overview'):fresh.check()

    def test_superseded_evidence_is_separate_and_checked_until_success(self):
        self.store.ensure_device('2','D3');decoded=rows((20,))
        old=self.store.begin_transfer('2',0,1);self.store.append_block(old,decoded,[{'block':0,'records':1}])
        current=self.store.begin_transfer('2',0,1);self.store.append_block(current,decoded,[{'block':0,'records':1}])
        status=self.store.status('2')[0]
        self.assertEqual((status['pending_records'],status['superseded_records'],status['resumable_records']),(1,1,0))
        self.assertEqual(self.store.resume('2'),(0,[]))
        self.store.check()
        with self.store.transaction() as c:c.execute("UPDATE pending_blocks SET sha256='bad' WHERE transfer_id=?",(old,))
        with self.assertRaisesRegex(RuntimeError,'摘要'):self.store.check()
        with self.store.transaction() as c:
            payload=c.execute('SELECT payload FROM pending_blocks WHERE transfer_id=?',(old,)).fetchone()[0]
            c.execute('UPDATE pending_blocks SET sha256=? WHERE transfer_id=?',(hashlib.sha256(payload).hexdigest(),old))
        self.store.finish_transfer(current,next_cursor=101,elapsed_seconds=0,maxrows=10,received_rows=decoded)
        status=self.store.status('2')[0]
        self.assertEqual((status['pending_records'],status['superseded_records']),(0,0))
        self.assertEqual(status['committed_records'],1)

    def test_no_new_records_does_not_hide_superseded_initial_evidence(self):
        self.store.ensure_device('2','D3');decoded=rows((20,))
        old=self.store.begin_transfer('2',0,1);self.store.append_block(old,decoded,[{'block':0,'records':1}])
        self.store.begin_transfer('2',0,1)
        fake=FakeBLE([],0,serial='2')
        async def resolve(_):return object()
        import asyncio
        with fake.patches(),patch.object(protocol,'resolve_ble_device',resolve),redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO()):
            code=asyncio.run(self.library.download('2',execute=True))
        outcome=self.library.download_results['2']
        self.assertEqual((code,outcome['status'],outcome['pending_records'],outcome['superseded_records']),(3,'pending',0,1))
        self.assertEqual(self.store.resume('2'),(0,[]))
        self.store.check()

    def test_busy_is_typed_without_changing_lock_exclusion(self):
        with storage.writer_lock(self.library.root):
            with self.assertRaises(StorageBusy) as caught:
                with storage.writer_lock(self.library.root):pass
            self.assertEqual(describe(caught.exception)['code'],'busy')
            self.assertTrue(describe(caught.exception)['retryable'])
            self.assertEqual(self.store.check()['records'],3)


class ProtocolAndResultTests(unittest.TestCase):
    def test_native_model_is_explicit_and_legacy_dialect_named(self):
        with self.assertRaises(TypeError):protocol.BleSession(None)
        for model in ('D1','D2','D3'):
            self.assertTrue(protocol.BleSession(None,model=model).require_schema)
        self.assertFalse(protocol.LegacyBleSession(None).require_schema)

    def test_final_ack_warning_flows_through_cli_worker_ui_and_is_not_retried(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);library=manager.Library(root/'archive');library.import_archive(fixture(root))
            fake=FakeBLE([bytes.fromhex(rows((30,))[0]['record_hex'])],104)
            base=fake.module.BleakClient
            class Client(base):
                async def write_gatt_char(self,uuid,raw,response):
                    for frame in protocol.FrameStream().feed(raw):
                        if frame.kind==4 and frame.payload==b'\x12\0':raise fake.errors.BleakError('lost final ACK')
                    return await super().write_gatt_char(uuid,raw,response)
            fake.module.BleakClient=Client
            async def resolve(_):return object()
            output=io.StringIO()
            with fake.patches(),patch.object(protocol,'resolve_ble_device',resolve),redirect_stdout(output),redirect_stderr(io.StringIO()):
                code=manager.main(['--data-dir',str(library.root),'--events-json','download','--serial','2866855','--execute'])
            stream=[validate_event(json.loads(line)) for line in output.getvalue().splitlines()]
            self.assertEqual(code,0);self.assertFalse(any(e['event']=='error' for e in stream))
            outcome=next(e for e in stream if e['event']=='completed')
            self.assertEqual((outcome['status'],outcome['total_records'],outcome['pending_records']),('saved_warning',4,0))
            result=next(e['data'] for e in stream if e['event']=='result')
            self.assertEqual(result['incomplete'],[])
            self.assertIsNone(retry_download_arguments(['download','--serials','2866855','--execute'],result['downloads'],[]))
            screen=Screen();terminal=Terminal(screen,library)
            terminal.job=SimpleNamespace(outcomes=[]);terminal.action='download';terminal.receive_event(outcome)
            terminal.job=None
            terminal.result_view={'outcomes':[outcome],'failures':[],'code':0,'action':'download','arguments':[],'selected':0}
            terminal.draw()
            self.assertIn('结束确认',' '.join(screen.lines))
            self.assertEqual(library.database_info()['records'],4)
            with self.assertRaises(ValueError):validate_event({**outcome,'warning':{'message':0}})

    def test_worker_sigint_cleans_session_and_preserves_durable_block(self):
        # Real worker -> CLI -> asyncio -> fake GATT -> SQLite, interrupted after commit.
        script="""import asyncio,json,sys
from pathlib import Path
from unittest.mock import patch
sys.path[:0]=[sys.argv[3],str(Path(sys.argv[3])/'tests')]
from test_manager import FakeBLE,RAW
from kestrel_drop import manager,protocol
fake=FakeBLE([RAW],101)
base=fake.module.BleakClient
class Client(base):
 async def __aexit__(self,*args):
  Path(sys.argv[2]).write_text(json.dumps({'notifications_stopped':fake.stopped}))
 async def write_gatt_char(self,uuid,raw,response):
  for frame in protocol.FrameStream().feed(raw):
   if frame.kind==4 and frame.payload==b'\\xff\\xff' and self.phase==1:
    await asyncio.sleep(30)
  return await super().write_gatt_char(uuid,raw,response)
fake.module.BleakClient=Client
async def resolve(_):return object()
with fake.patches(),patch.object(protocol,'resolve_ble_device',resolve):
 sys.exit(manager.main(['--data-dir',sys.argv[1],'--events-json','download','--serial','2866855','--execute']))
"""
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);data=root/'data';marker=root/'cleanup.json'
            prefix=[sys.executable,'-u','-B','-c',script,str(data),str(marker),str(Path(__file__).resolve().parents[1])]
            with patch('kestrel_drop.worker.cli_command',return_value=prefix):job=Job(data,['download'])
            try:
                deadline=time.monotonic()+10
                while time.monotonic()<deadline:
                    kind,event=job.events.get(timeout=5)
                    if kind=='event' and event.get('event')=='progress':break
                else:self.fail('Child never committed a block')
                job.cancel();self.assertEqual(job.process.wait(timeout=5),130)
                job.thread.join(timeout=5);self.assertFalse(job.thread.is_alive())
                self.assertTrue(job.finished);self.assertTrue(job.process.stdout.closed);self.assertTrue(job.process.stderr.closed)
                self.assertTrue(json.loads(marker.read_text())['notifications_stopped'])
                info=manager.Library(data).database_info()
                self.assertEqual(info['records'],0);self.assertEqual(info['devices'][0]['pending_records'],1)
            finally:job.close()


class TransportCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_io_cannot_swallow_external_cancellation(self):
        import asyncio
        # Python 3.11 wait_for may return an already-completed inner task while
        # swallowing cancellation of its caller. Direct timeout scopes must not.
        for operation in ('send','next'):
            with self.subTest(operation=operation):
                task=None
                async def completed_io(*args,**kwargs):
                    asyncio.get_running_loop().call_soon(task.cancel)
                    return protocol.Frame(4,b'\xff\xff',b'')
                client=SimpleNamespace(write_gatt_char=completed_io)
                session=protocol.BleSession(client,model='D1')
                session.queue=SimpleNamespace(get=completed_io)
                async def transfer():
                    if operation=='send':await session.send(4,b'\xff\xff')
                    else:await session.next()
                    await asyncio.sleep(0)
                task=asyncio.create_task(transfer())
                with self.assertRaises(asyncio.CancelledError):await task

    async def test_gatt_deadline_cancels_inner_io_and_runs_cleanup(self):
        import asyncio
        from kestrel_drop.devices import gatt_wait
        closed=[]
        async def blocked():
            try:await asyncio.sleep(30)
            finally:closed.append(True)
        with self.assertRaises(TimeoutError):await gatt_wait(blocked(),timeout=.001)
        self.assertEqual(closed,[True])
