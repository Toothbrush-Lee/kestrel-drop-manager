"""Library, first-download durability and CLI integration without BLE hardware."""
import asyncio
from contextlib import redirect_stdout, redirect_stderr
import csv
import io
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType
import unittest
from unittest.mock import patch

import kestrel_drop.manager as manager
import kestrel_drop.protocol as protocol
from kestrel_drop.tui import Job, clip

RAW = bytes.fromhex('00140e160aea07f506990dfa0500d000c227360300')


def record(minute):
    raw = bytearray(RAW)
    raw[1] = minute
    return bytes(raw)


class FakeBLE:
    def __init__(self, rows, cursor, *, interrupt=False, serial='2866855'):
        self.rows, self.cursor = rows, cursor
        self.interrupt, self.serial = interrupt, serial
        self.writes = []
        self.stopped = False
        owner = self

        class Client:
            def __init__(self, *args, **kwargs):
                self.phase = 0

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                pass

            async def read_gatt_char(self, uuid):
                return owner.serial.encode() if uuid == protocol.SERIAL_UUID else b'Kestrel DROP 3'

            async def start_notify(self, uuid, callback):
                self.callback = callback

            async def stop_notify(self, uuid):
                owner.stopped = True

            def transmit(self, kind, payload):
                self.callback(None, protocol.encode_frame(kind, payload))

            async def write_gatt_char(self, uuid, raw, response):
                frame = protocol.decode_frame(raw[1:-1])
                owner.writes.append(frame)
                if frame.kind == 0 and frame.payload[:2] == b'\x03\x00':
                    self.transmit(4, b'\x03\x00')
                    self.transmit(3, len(owner.rows).to_bytes(4, 'little'))
                elif frame.kind == 0 and frame.payload[:2] == b'\x05\x00':
                    self.transmit(4, b'\x05\x00')
                    self.transmit(1, b'schema')
                elif frame.kind == 4 and frame.payload == b'\x05\x00':
                    if self.phase == 0:
                        self.transmit(3, b'\x00\x00' + b''.join(owner.rows))
                    else:
                        if owner.interrupt:
                            raise asyncio.CancelledError()
                        self.transmit(0, b'\x12\x00' + owner.cursor.to_bytes(4, 'little'))
                    self.phase += 1

        self.module = ModuleType('bleak')
        self.module.BleakClient = Client
        self.errors = ModuleType('bleak.exc')
        self.errors.BleakError = type('BleakError', (Exception,), {})

    def patches(self):
        return patch.dict(sys.modules, {'bleak': self.module, 'bleak.exc': self.errors})


class LibraryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.library = manager.Library(self.root / 'library')
        self.out = io.StringIO()
        self.stdout = redirect_stdout(self.out)
        self.stderr = redirect_stderr(self.out)
        self.stdout.__enter__()
        self.stderr.__enter__()
        self.addCleanup(self.stdout.__exit__, None, None, None)
        self.addCleanup(self.stderr.__exit__, None, None, None)

    def download(self, fake, library=None):
        async def resolve(_):
            return object()
        with fake.patches(), patch.object(protocol, 'resolve_ble_device', resolve):
            return asyncio.run((library or self.library).download('2866855', execute=True))

    def fixture(self):
        source = self.root / 'source.csv'
        rows = [protocol.decode_record(record(m), block=0, position=i) for i, m in enumerate((20, 25, 25))]
        protocol.write_csv(source, rows)
        meta = {'serial': '2866855', 'model': 'Kestrel DROP 3', 'rows': 3,
                'first_record_time': rows[0]['timestamp_device_local'],
                'last_record_time': rows[-1]['timestamp_device_local'],
                'validation': {'next_cursor': 103, 'inferred_first_cursor': 100}}
        Path(str(source) + '.json').write_text(json.dumps(meta))
        return source

    def test_first_download_then_incremental_is_one_archive(self):
        self.assertEqual(self.download(FakeBLE([record(20), record(25)], 102)), 0)
        initial = self.library.database_path
        self.assertEqual(self.library.status()[0]['next_cursor'], 102)
        fake = FakeBLE([record(30)], 103)
        self.assertEqual(self.download(fake), 0)
        query = next(f for f in fake.writes if f.kind == 0 and f.payload[:2] == b'\x03\x00')
        self.assertEqual(int.from_bytes(query.payload[2:6], 'little'), 102)
        self.assertEqual(self.library.database_path, initial)
        self.assertEqual(len(self.library.records('2866855')), 3)
        self.assertFalse(list(self.library.root.rglob('*.partial.csv')))

    def test_cancel_first_download_preserves_blocks_and_retry_restarts(self):
        fake = FakeBLE([record(20)], 101, interrupt=True)
        with self.assertRaises(asyncio.CancelledError): self.download(fake)
        self.assertTrue(fake.stopped)
        self.assertFalse(self.library.archives())
        self.assertEqual(self.library.status()[0]['initial_attempts'], 1)
        self.assertEqual(self.library.status()[0]['pending_records'], 1)
        retry = FakeBLE([record(20), record(25)], 102)
        self.download(retry)
        query = next(f for f in retry.writes if f.kind == 0 and f.payload[:2] == b'\x03\x00')
        self.assertEqual(int.from_bytes(query.payload[2:6], 'little'), 0)
        self.assertEqual(len(self.library.records('2866855')), 2)
        with self.library.store.connect() as c:
            self.assertEqual(c.execute('SELECT count(*) FROM pending_blocks').fetchone()[0], 0)

    def test_cancel_incremental_then_resume_absolute_cursor(self):
        self.library.import_archive(self.fixture())
        with self.assertRaises(asyncio.CancelledError):
            self.download(FakeBLE([record(30)], 104, interrupt=True))
        status = self.library.status()[0]
        self.assertEqual((status['committed_records'], status['pending_records'], status['resume_cursor']), (3, 1, 104))
        self.download(FakeBLE([record(35)], 105))
        self.assertEqual(len(self.library.records('2866855')), 5)

    def test_completed_first_download_is_immediately_queryable(self):
        self.download(FakeBLE([record(20)], 101))
        reopened = manager.Library(self.library.root)
        self.assertEqual(len(reopened.records('2866855')), 1)
        self.assertEqual(reopened.status()[0]['next_cursor'], 101)

    def test_wrong_device_never_receives_commands(self):
        fake = FakeBLE([record(20)], 101, serial='0000')
        with self.assertRaisesRegex(RuntimeError, 'Serial mismatch'):
            self.download(fake)
        self.assertEqual(fake.writes, [])
        self.assertFalse(self.library.archives())

    def test_d1_and_d2_models_rejected_before_any_proprietary_write(self):
        for model in (b'Kestrel DROP 1',b'Kestrel DROP 2'):
            with self.subTest(model=model):
                fake=FakeBLE([record(20)],101)
                original=fake.module.BleakClient
                class Client(original):
                    async def read_gatt_char(self,uuid):
                        return b'2866855' if uuid==protocol.SERIAL_UUID else model
                fake.module.BleakClient=Client
                with self.assertRaisesRegex(RuntimeError,'unsupported model'): self.download(fake)
                self.assertEqual(fake.writes,[])
                self.assertFalse(self.library.archives())

    def test_empty_device_does_not_publish_empty_archive(self):
        self.download(FakeBLE([], 0))
        self.assertFalse(self.library.archives())

    def test_import_preserves_source_and_protects_existing_archive(self):
        source = self.fixture()
        original = source.read_bytes()
        output = self.library.import_archive(source)
        self.assertEqual(source.read_bytes(), original)
        self.assertEqual(output, self.library.database_path)
        self.assertFalse(list(self.library.root.rglob('*.csv')))
        self.assertEqual(len(self.library.records('2866855')), 3)
        with self.assertRaises(FileExistsError):
            self.library.import_archive(source)

    def test_invalid_import_never_publishes(self):
        source = self.fixture()
        source.write_text(source.read_text().replace('14:20:00', '14:21:00'))
        with self.assertRaises(RuntimeError):
            self.library.import_archive(source)
        self.assertFalse(self.library.archives())
        # Bad imported evidence cannot poison initial-download recovery.
        self.download(FakeBLE([record(20)], 101))
        self.assertEqual(self.library.status()[0]['committed_records'], 1)

    def test_incremental_ble_disconnect_becomes_actionable_error(self):
        self.library.import_archive(self.fixture())
        fake = FakeBLE([], 0)
        with fake.patches(), patch.object(protocol, 'resolve_ble_device', side_effect=fake.errors.BleakError('connection rejected')):
            with self.assertRaisesRegex(RuntimeError, '蓝牙操作失败.*connection rejected'):
                asyncio.run(self.library.download('2866855', execute=True))

    def test_scan_filters_unrelated_devices_and_marks_unnamed_candidates(self):
        from types import SimpleNamespace as Item
        fake = FakeBLE([], 0)
        async def scan(**kwargs):
            def advertisement(name, address, services, rssi):
                return Item(name=name, address=address), Item(local_name=name, service_uuids=services, rssi=rssi)
            return {'a': advertisement('D3 - 2866855', 'one', [], -40),
                    'b': advertisement('', 'two', [manager.DROP_SERVICE], -60),
                    'c': advertisement('Headphones', 'three', [], -20)}
        fake.module.BleakScanner = Item(discover=scan)
        with fake.patches():
            found = asyncio.run(manager.discover(1))
        self.assertEqual(len(found), 2)
        self.assertEqual(found[0]['serial'], '2866855')
        self.assertFalse(found[1]['supported'])
        self.assertIsNone(found[1]['serial'])

    def test_export_time_filter_preserves_duplicates_and_refuses_overwrite(self):
        self.library.import_archive(self.fixture())
        output = self.root / 'export.csv'
        self.assertEqual(self.library.export('2866855', output, since='2026-10-22 14:25:00'), 2)
        with output.open(encoding='utf-8-sig') as fd:
            self.assertEqual(len(list(csv.DictReader(fd))), 2)
        with self.assertRaises(FileExistsError):
            self.library.export('2866855', output)
        with self.assertRaises(ValueError):
            self.library.export('2866855', self.root / 'bad.csv', since='2026-10-23', until='2026-10-22')

    def test_status_reads_while_writer_lock_held(self):
        from kestrel_drop.storage import writer_lock
        self.library.import_archive(self.fixture())
        with writer_lock(self.library.root):
            self.assertEqual(self.library.status()[0]['database_records'], 3)

    def test_dry_run_never_creates_library(self):
        self.assertEqual(asyncio.run(self.library.download('2866855')), 0)
        self.assertFalse(self.library.root.exists())

    def test_explicit_directory_and_configure_can_recover_bad_settings(self):
        settings=self.root/'settings.json'
        settings.write_text('broken JSON')
        with patch.dict('os.environ',{'KESTREL_CONFIG_FILE':str(settings)}):
            self.assertEqual(manager.main(['--data-dir',str(self.library.root),'list']),0)
            self.assertEqual(manager.main(['configure',str(self.root/'chosen')]),0)
            self.assertEqual(json.loads(settings.read_text())['data_dir'],str((self.root/'chosen').resolve()))

    def test_export_write_failure_does_not_publish_partial_file(self):
        self.library.import_archive(self.fixture())
        destination=self.root/'export.csv'
        with patch('kestrel_drop.manager.os.fsync',side_effect=OSError('disk full')):
            with self.assertRaises(OSError): self.library.export('2866855',destination)
        self.assertFalse(destination.exists())
        self.assertFalse(list(self.root.glob('.kestrel-export-*')))

    def test_serial_path_traversal_rejected(self):
        for value in ('../source', '', '1/2', '１２３', '-1'):
            with self.assertRaises(ValueError):
                asyncio.run(self.library.download(value, execute=True))

    def test_cli_batch_continues_after_failure(self):
        calls = []
        async def download(_, serial, **kwargs):
            calls.append(serial)
            if serial == '111':
                raise RuntimeError('unavailable')
            return 0
        with patch.object(manager.Library, 'targets', return_value=['111', '222']), patch.object(manager.Library, 'download', download), patch.object(manager.devices, 'scan_snapshot', return_value={}):
            self.assertEqual(manager.main(['download', '--all', '--execute']), 2)
        self.assertEqual(calls, ['111', '222'])

    def test_tui_background_job_reads_json_and_exits(self):
        job = Job(self.library.root, ['list'])
        self.addCleanup(job.close)
        payloads = []
        while True:
            kind, value = job.events.get(timeout=10)
            if kind == 'exit':
                self.assertEqual(value, 0)
                break
            if kind == 'event' and value['event'] == 'result':
                payloads.append(value['data'])
        self.assertEqual(payloads, [[]])

    def test_tui_width_clipping_handles_chinese(self):
        self.assertEqual(clip('设备 ABC', 5), '设备 ')
        self.assertEqual(clip('设备 ABC', 6), '设备 A')


if __name__ == '__main__':
    unittest.main()
