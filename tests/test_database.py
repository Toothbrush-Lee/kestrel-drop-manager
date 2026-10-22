"""SQLite transactions, CSV replay, backups and independent offline queries."""
import asyncio
from contextlib import redirect_stderr, redirect_stdout
import csv
import io
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from kestrel_drop.database import Store
from kestrel_drop.manager import Library
from kestrel_drop import protocol
from test_manager import FakeBLE, record


class DatabaseTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.library = Library(self.root / 'archive')
        self.source = self.root / 'source.csv'
        rows = [protocol.decode_record(record(minute), block=0, position=i)
                for i, minute in enumerate((20, 25, 25))]
        protocol.write_csv(self.source, rows)
        self.metadata = Path(str(self.source) + '.json')
        self.metadata.write_text(json.dumps({
            'serial': '2866855', 'model': 'Kestrel DROP 3', 'rows': 3,
            'first_record_time': rows[0]['timestamp_device_local'],
            'last_record_time': rows[-1]['timestamp_device_local'],
            'validation': {'next_cursor': 103, 'inferred_first_cursor': 100}}))
        self.output = self.library.import_archive(self.source)

    def download(self, fake):
        async def resolve(_):
            return object()
        with fake.patches(), patch.object(protocol, 'resolve_ble_device', resolve), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            return asyncio.run(self.library.download('2866855', execute=True))

    def append_committed_csv(self):
        paths = protocol._archive_paths(self.output)
        state = protocol._state_loaded(paths)
        journal = protocol._new_pending(paths, state)
        protocol.BlockSpool(paths['pending'], state['fieldnames']).append(
            [protocol.decode_record(record(30), block=0, position=0)], [])
        journal['completed'] = {'next_cursor': 104, 'validation': {'verification': 'complete'}}
        protocol._json_atomic(paths['journal'], journal)
        protocol.commit_pending(paths, state)

    def test_automatic_migration_is_typed_and_idempotent(self):
        for _ in range(3):
            self.library.status()
        summary = self.library.database_info()
        self.assertEqual((summary['records'], summary['batches']), (3, 1))
        with self.library.store.connect() as connection:
            rows = connection.execute('SELECT * FROM observations ORDER BY absolute_cursor').fetchall()
        self.assertEqual([r['absolute_cursor'] for r in rows], [100, 101, 102])
        self.assertIsInstance(rows[0]['temperature_c'], float)
        self.assertIsInstance(rows[0]['record_raw'], bytes)
        self.assertEqual(rows[1]['timestamp_device_local'], rows[2]['timestamp_device_local'])

    def test_transaction_failure_rolls_back_rows_checkpoint_and_batch(self):
        self.append_committed_csv()
        with self.library.store.connect() as connection, connection:
            connection.execute('''CREATE TRIGGER fail_new_record BEFORE INSERT ON observations
                WHEN NEW.absolute_cursor=103 BEGIN SELECT RAISE(ABORT, 'simulated disk failure'); END''')
        with self.assertRaises(sqlite3.IntegrityError):
            self.library.index_archive('2866855')
        self.assertEqual(self.library.store.device('2866855')['next_cursor'], 103)
        summary = self.library.store.summary()
        self.assertEqual((summary['records'], summary['batches']), (3, 1))
        with self.library.store.connect() as connection, connection:
            connection.execute('DROP TRIGGER fail_new_record')
        self.assertEqual(self.library.index_archive('2866855')['database_added_records'], 1)

    def test_csv_commit_then_database_failure_recovers_on_next_launch(self):
        real_ingest = self.library.store.ingest
        calls = []
        def fail_after_download(*args):
            calls.append(True)
            if len(calls) == 2:
                raise sqlite3.OperationalError('disk full')
            return real_ingest(*args)
        with patch.object(self.library.store, 'ingest', side_effect=fail_after_download):
            with self.assertRaises(sqlite3.OperationalError):
                self.download(FakeBLE([record(30)], 104))
        self.assertEqual(protocol.archive_info(self.output)['committed_records'], 4)
        self.assertEqual(self.library.store.summary()['records'], 3)
        reopened = Library(self.library.root)
        self.assertEqual(len(reopened.records('2866855')), 4)
        self.assertEqual(reopened.database_info()['batches'], 2)

    def test_pending_data_is_not_published_to_database(self):
        with self.assertRaises(asyncio.CancelledError):
            self.download(FakeBLE([record(30)], 104, interrupt=True))
        info = self.library.status()[0]
        self.assertEqual((info['pending_records'], info['database_records']), (1, 3))

    def test_conflicting_existing_record_is_not_overwritten(self):
        with self.library.store.connect() as connection, connection:
            connection.execute("UPDATE observations SET row_json='{}' WHERE absolute_cursor=100")
        with self.assertRaisesRegex(RuntimeError, '记录冲突'):
            self.library.index_archive('2866855')
        with self.library.store.connect() as connection:
            self.assertEqual(connection.execute('SELECT row_json FROM observations WHERE absolute_cursor=100').fetchone()[0], '{}')

    def test_typed_column_conflict_is_detected(self):
        with self.library.store.connect() as connection, connection:
            connection.execute('UPDATE observations SET temperature_c=999 WHERE absolute_cursor=100')
        with self.assertRaisesRegex(RuntimeError, '测量字段冲突'):
            self.library.index_archive('2866855')

    def test_bad_numeric_value_never_enters_database(self):
        paths = protocol._archive_paths(self.output)
        state = protocol._state_loaded(paths)
        names, rows = protocol._rows_checked(self.output)
        rows[0]['temperature_c'] = 'nan'
        with self.assertRaisesRegex(RuntimeError, 'temperature_c'):
            self.library.store.ingest(self.output, state, names, rows)
        self.assertEqual(self.library.store.summary()['records'], 3)

    def test_independent_backup_restores_queries_without_csv(self):
        backup = self.library.backup(self.root / 'backup.sqlite')
        restored_root = self.root / 'restored'
        restored_root.mkdir()
        shutil.copyfile(backup, restored_root / 'library.sqlite')
        restored = Library(restored_root)
        self.assertEqual([row['absolute_cursor'] for row in restored.records('2866855')], [100, 101, 102])
        self.assertIn('仅有数据库副本', restored.status()[0]['note'])
        self.assertEqual(restored.export('2866855', self.root / 'restored.csv'), 3)
        with self.assertRaisesRegex(RuntimeError, '恢复整个 archive'):
            asyncio.run(restored.download('2866855', execute=True))
        with self.assertRaises(FileExistsError):
            self.library.backup(backup)

    def test_end_date_includes_entire_day_and_exports_identity(self):
        destination = self.root / 'day.csv'
        self.assertEqual(self.library.export('2866855', destination, since='2026-10-22', until='2026-10-22'), 3)
        with destination.open(encoding='utf-8-sig') as fd:
            rows = list(csv.DictReader(fd))
        self.assertEqual([row['absolute_cursor'] for row in rows], ['100', '101', '102'])
        self.assertEqual(rows[0]['serial'], '2866855')

    def test_same_absolute_cursor_on_two_devices_is_allowed(self):
        metadata = json.loads(self.metadata.read_text())
        metadata['serial'] = '1234567'
        self.metadata.write_text(json.dumps(metadata))
        self.library.import_archive(self.source)
        self.assertEqual(self.library.database_info()['records'], 6)
        self.assertEqual(len(self.library.store.query('1234567')), 3)

    def test_logical_missing_record_is_reported(self):
        with self.library.store.connect() as connection, connection:
            connection.execute('DELETE FROM observations WHERE absolute_cursor=101')
        with self.assertRaisesRegex(RuntimeError, '记录数或游标'):
            self.library.store.summary()

    def test_unknown_schema_version_is_not_reset(self):
        other = self.root / 'future.sqlite'
        connection = sqlite3.connect(other)
        connection.execute('PRAGMA user_version=999')
        connection.close()
        with self.assertRaisesRegex(RuntimeError, '版本不受支持'):
            Store(other)
        connection = sqlite3.connect(other)
        self.assertEqual(connection.execute('PRAGMA user_version').fetchone()[0], 999)
        connection.close()


if __name__ == '__main__':
    unittest.main()
