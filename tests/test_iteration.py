"""0.5 fault boundaries, persistent evidence and structured worker contract."""
import asyncio
from contextlib import redirect_stderr, redirect_stdout
import csv
import io
import json
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from kestrel_drop import events, manager, protocol
from kestrel_drop.errors import BluetoothFailure, TransferError
from kestrel_drop.tui import Terminal, Job
from test_manager import FakeBLE, record


class StreamBoundaryTests(unittest.IsolatedAsyncioTestCase):
    async def transfer(self, frames, expected=2):
        self.persisted, self.sent = [], []
        owner = self
        class Client:
            async def write_gatt_char(self, uuid, packet, response):
                owner.sent.append(protocol.decode_frame(packet[1:-1]))
        session = protocol.BleSession(Client())
        async def next_frame(timeout=25):
            if not frames:
                raise BluetoothFailure('simulated disconnect')
            return frames.pop(0)
        session.next = next_frame
        def persist(rows, blocks):
            self.persisted.extend(item['block'] for item in blocks)
        return await session.download_from_cursor(100, expected, 100, on_block=persist)

    def block(self, index, minute=20):
        return protocol.Frame(3, index.to_bytes(2, 'little') + record(minute), b'')

    def acks(self, operation=5):
        return [f for f in self.sent if f.kind == 4 and f.payload == operation.to_bytes(2, 'little')]

    async def test_missing_block_stops_before_persisting_or_acknowledging_gap(self):
        with self.assertRaises(TransferError):
            await self.transfer([self.block(0), self.block(2, 30)])
        self.assertEqual(self.persisted, [0])
        self.assertEqual(len(self.acks()), 1)

    async def test_nonzero_first_block_is_not_a_proven_resume_boundary(self):
        with self.assertRaises(TransferError):
            await self.transfer([self.block(1)])
        self.assertFalse(self.persisted)
        self.assertFalse(self.acks())

    async def test_duplicate_block_is_not_written_or_acknowledged_twice(self):
        with self.assertRaises(TransferError):
            await self.transfer([self.block(0), self.block(0)])
        self.assertEqual(self.persisted, [0])
        self.assertEqual(len(self.acks()), 1)

    async def test_time_goes_backwards_before_spooling_second_block(self):
        with self.assertRaises(TransferError):
            await self.transfer([self.block(0, 30), self.block(1, 20)])
        self.assertEqual(self.persisted, [0])

    async def test_empty_record_block_is_a_typed_failure(self):
        with self.assertRaises(TransferError):
            await self.transfer([protocol.Frame(3, b'\x00\x00', b'')])
        self.assertFalse(self.persisted)

    async def test_bad_record_date_is_a_typed_failure(self):
        raw = bytearray(record(20)); raw[4] = 99
        with self.assertRaises(TransferError):
            await self.transfer([protocol.Frame(3, b'\x00\x00' + bytes(raw), b'')])
        self.assertFalse(self.persisted)

    async def test_incomplete_final_message_is_not_acknowledged_as_completed(self):
        final = protocol.Frame(0, b'\x12\x00' + (101).to_bytes(4, 'little'), b'')
        with self.assertRaises(TransferError):
            await self.transfer([self.block(0), final], expected=2)
        self.assertFalse(self.acks(18))

    async def test_wrong_final_cursor_is_not_acknowledged(self):
        final = protocol.Frame(0, b'\x12\x00' + (999).to_bytes(4, 'little'), b'')
        with self.assertRaises(TransferError):
            await self.transfer([self.block(0), final], expected=1)
        self.assertFalse(self.acks(18))

    async def test_crc_error_is_fatal_before_any_later_good_frame_is_spooled(self):
        saved, sent = [], []
        bad = bytearray(protocol.encode_frame(3, b'\x01\x00' + record(25)))
        bad[-2] ^= 1
        class Client:
            async def write_gatt_char(self, uuid, packet, response):
                frame = protocol.decode_frame(packet[1:-1]); sent.append(frame)
                if frame.kind == 4:
                    session.callback(None, bytes(bad) + protocol.encode_frame(3, b'\x01\x00' + record(25)))
        session = protocol.BleSession(Client())
        session.callback(None, protocol.encode_frame(3, b'\x00\x00' + record(20)))
        with self.assertRaises(TransferError):
            await session.download_from_cursor(100, 2, 10, on_block=lambda rows, blocks: saved.extend(rows))
        self.assertEqual(len(saved), 1)
        self.assertEqual(len([f for f in sent if f.kind == 4]), 1)


class ArchiveFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.library = manager.Library(self.root / 'archive')
        source = self.root / 'source.csv'
        row = protocol.decode_record(record(20), block=0, position=0)
        protocol.write_csv(source, [row])
        Path(str(source) + '.json').write_text(json.dumps({
            'serial': '2866855', 'model': 'Kestrel DROP 3', 'rows': 1,
            'first_record_time': row['timestamp_device_local'], 'last_record_time': row['timestamp_device_local'],
            'validation': {'next_cursor': 101, 'inferred_first_cursor': 100}}))
        self.output = self.root / 'legacy.csv'
        import shutil
        shutil.copyfile(source, self.output)
        protocol.adopt_archive(self.output, Path(str(source) + '.json'))
        self.source = source
        self.paths = protocol._archive_paths(self.output)
        self.state = protocol._state_loaded(self.paths)

    def spool(self):
        protocol._new_pending(self.paths, self.state)
        return protocol.BlockSpool(self.paths['pending'], self.state['fieldnames'])

    def append(self, spool):
        spool.append([protocol.decode_record(record(25), block=0, position=0)], [{'block': 0, 'records': 1}])

    def run_cli(self, arguments, fake=None):
        output = io.StringIO()
        async def resolve(args):
            events.emit('phase', serial=args.serial, phase='scanning')
            return object()
        with redirect_stdout(output), redirect_stderr(io.StringIO()):
            if fake:
                with fake.patches(), patch.object(protocol, 'resolve_ble_device', resolve):
                    code = manager.main(['--data-dir', str(self.library.root), '--events-json', *arguments])
            else:
                code = manager.main(['--data-dir', str(self.library.root), '--events-json', *arguments])
        return code, [json.loads(line) for line in output.getvalue().splitlines()]


class CheckpointTests(ArchiveFixture):
    def test_kill_between_empty_spool_and_journal_creation_recovers(self):
        with self.paths['pending'].open('w', newline='') as fd:
            csv.DictWriter(fd, fieldnames=self.state['fieldnames']).writeheader()
        journal, rows = protocol._pending_loaded(self.paths, self.state)
        self.assertEqual(rows, [])
        self.assertEqual(journal['spool_checkpoint']['resume_cursor'], 101)

    def test_missing_empty_spool_can_be_reconstructed_from_exact_header_checkpoint(self):
        protocol._new_pending(self.paths, self.state)
        self.paths['pending'].unlink()
        _, rows = protocol._pending_loaded(self.paths, self.state)
        self.assertEqual(rows, [])

    def test_checkpoint_is_durable_before_device_receives_block_ack(self):
        fake = FakeBLE([record(25)], 102, interrupt=True)
        original = fake.module.BleakClient
        verified = []
        owner = self
        class Client(original):
            async def write_gatt_char(self, uuid, packet, response):
                frame = protocol.decode_frame(packet[1:-1])
                if frame.kind == 4 and frame.payload == b'\x05\x00' and self.phase == 1:
                    journal = json.loads(owner.paths['journal'].read_text())
                    checkpoint = journal['spool_checkpoint']
                    verified.append(checkpoint['sha256'] == protocol._sha256(owner.paths['pending']))
                    owner.assertEqual(checkpoint['resume_cursor'], 102)
                    owner.assertEqual(journal['transfer_sessions'][0]['last_block'], 0)
                return await super().write_gatt_char(uuid, packet, response)
        fake.module.BleakClient = Client
        async def resolve(_): return object()
        with fake.patches(), patch.object(protocol, 'resolve_ble_device', resolve), redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
            with self.assertRaises(asyncio.CancelledError):
                asyncio.run(protocol.sync_live(type('Args', (), dict(output=str(self.output), serial='2866855', execute=True, ble_id=None, scan_timeout=1, maxrows=100, progress_factory=protocol.TransferProgress))()))
        self.assertEqual(verified, [True])

    def test_checkpoint_write_failure_preserves_tail_and_resumes_only_verified_prefix(self):
        spool = self.spool()
        with patch.object(protocol, '_json_atomic', side_effect=OSError('crash before checkpoint publication')):
            with self.assertRaises(OSError): self.append(spool)
        with redirect_stderr(io.StringIO()):
            journal, rows = protocol._pending_loaded(self.paths, self.state)
        self.assertEqual(rows, [])
        self.assertEqual(journal['spool_checkpoint']['resume_cursor'], 101)
        evidence = list(self.output.parent.glob('*.unconfirmed-*.bin'))
        self.assertEqual(len(evidence), 1)
        self.assertIn(record(25).hex().encode(), evidence[0].read_bytes())
        self.assertEqual(protocol._sha256(self.output), self.state['csv_sha256'])

    def test_mutation_inside_verified_prefix_is_refused_and_preserved(self):
        self.append(self.spool())
        content = bytearray(self.paths['pending'].read_bytes()); content[0] ^= 1
        self.paths['pending'].write_bytes(content)
        with self.assertRaisesRegex(RuntimeError, 'checkpoint failed integrity'):
            protocol._pending_loaded(self.paths, self.state)
        self.assertEqual(self.paths['pending'].read_bytes(), bytes(content))

    def test_legacy_unfinished_pending_is_not_given_a_guessed_resume_cursor(self):
        self.append(self.spool())
        journal = json.loads(self.paths['journal'].read_text())
        journal.pop('spool_checkpoint'); journal.pop('transfer_sessions')
        protocol._json_atomic(self.paths['journal'], journal)
        with self.assertRaisesRegex(RuntimeError, 'Legacy pending'):
            protocol._pending_loaded(self.paths, self.state)
        self.assertTrue(self.paths['pending'].exists())

    def test_session_cursor_tampering_is_detected(self):
        self.append(self.spool())
        journal = json.loads(self.paths['journal'].read_text())
        journal['transfer_sessions'][0]['requested_cursor'] = 999
        protocol._json_atomic(self.paths['journal'], journal)
        with self.assertRaisesRegex(RuntimeError, 'session boundaries'):
            protocol._pending_loaded(self.paths, self.state)

    def cleanup_crash(self, which):
        self.append(self.spool())
        journal = json.loads(self.paths['journal'].read_text())
        journal['completed'] = {'next_cursor': 102, 'validation': {'verification': 'complete'}}
        protocol._json_atomic(self.paths['journal'], journal)
        original = Path.unlink
        def unlink(path, *args, **kwargs):
            if path == self.paths[which]: raise OSError('crash during cleanup')
            return original(path, *args, **kwargs)
        with patch.object(Path, 'unlink', unlink):
            with self.assertRaises(OSError): protocol.commit_pending(self.paths, self.state)
        info = protocol.archive_info(self.output)
        self.assertEqual((info['committed_records'], info['next_cursor']), (2, 102))
        self.assertFalse(self.paths['journal'].exists())
        self.assertFalse(self.paths['pending'].exists())

    def test_crash_after_state_commit_before_pending_cleanup_recovers(self): self.cleanup_crash('pending')
    def test_crash_between_pending_and_journal_cleanup_recovers(self): self.cleanup_crash('journal')

    def test_forcibly_killed_writer_leaves_a_valid_resume_boundary(self):
        code = '''import sys,time
from pathlib import Path
from kestrel_drop import protocol as p
output=Path(sys.argv[1])
with p._archive_lock(output):
 paths=p._archive_paths(output); state=p._state_loaded(paths)
 p._new_pending(paths,state)
 raw=bytearray.fromhex('00140e160aea07f506990dfa0500d000c227360300'); raw[1]=25
 p.BlockSpool(paths['pending'],state['fieldnames']).append([p.decode_record(bytes(raw),block=0,position=0)],[{'block':0,'records':1}])
 print('durable',flush=True)
 time.sleep(30)
'''
        process = subprocess.Popen([sys.executable, '-B', '-c', code, str(self.output)], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertTrue(select.select([process.stdout], [], [], 5)[0])
            self.assertEqual(process.stdout.readline().strip(), 'durable')
        finally:
            process.kill(); process.communicate(timeout=5)
        info = protocol.archive_info(self.output)
        self.assertEqual((info['pending_records'], info['resume_cursor'], info['committed_records']), (1, 102, 1))


class EventTests(ArchiveFixture):
    def setUp(self):
        super().setUp()
        self.library.import_archive(self.source)

    def test_success_stream_has_typed_phases_progress_and_complete_result(self):
        code, stream = self.run_cli(['download', '--serial', '2866855', '--execute'], FakeBLE([record(25)], 102))
        self.assertEqual(code, 0)
        self.assertTrue(all(item['version'] == 1 for item in stream))
        phases = [item['phase'] for item in stream if item['event'] == 'phase']
        for phase in ('scanning', 'connecting', 'identifying', 'querying', 'receiving', 'verifying', 'saving'):
            self.assertIn(phase, phases)
        self.assertTrue(any(item['event'] == 'progress' for item in stream))
        completed = next(item for item in stream if item['event'] == 'completed')
        self.assertEqual((completed['added_records'], completed['total_records'], completed['pending_records']), (1, 2, 0))
        self.assertEqual(stream[-1], {'version': 1, 'event': 'finished', 'code': 0})

    def test_wrong_device_exposes_non_retryable_action(self):
        fake = FakeBLE([record(25)], 102, serial='000')
        code, stream = self.run_cli(['download', '--serial', '2866855', '--execute'], fake)
        failure = next(item for item in stream if item['event'] == 'error')
        self.assertEqual((code, failure['code'], failure['retryable']), (2, 'device_mismatch', False))
        self.assertIn('序列号', failure['action'])
        self.assertEqual(fake.writes, [])

    def test_zero_new_records_reports_up_to_date_without_inventing_additions(self):
        code, stream = self.run_cli(['download', '--serial', '2866855', '--execute'], FakeBLE([], 101))
        result = next(item for item in stream if item['event'] == 'completed')
        self.assertEqual((code, result['status'], result['added_records']), (0, 'up_to_date', 0))

    def test_unconfirmed_completion_is_actionable_and_not_success(self):
        transfer = self.library.store.begin_transfer('2866855', 101, 1)
        self.library.store.append_block(transfer, [protocol.decode_record(record(25), block=0, position=0)], [{'block': 0, 'records': 1}])
        code, stream = self.run_cli(['download', '--serial', '2866855', '--execute'], FakeBLE([], 102))
        failure = next(item for item in stream if item['event'] == 'error')
        self.assertEqual((code, failure['code']), (2, 'completion_pending'))
        result = next(item for item in stream if item['event'] == 'completed')
        self.assertEqual((result['status'], result['pending_records'], result['added_records']), ('pending', 1, 0))

    def test_plain_cli_retains_single_json_result_for_read_commands(self):
        output = io.StringIO()
        with redirect_stdout(output):
            code = manager.main(['--data-dir', str(self.library.root), 'records', '2866855'])
        self.assertEqual(code, 0)
        self.assertEqual(len(json.loads(output.getvalue())), 1)

    def test_worker_rejects_unstructured_stdout_as_protocol_failure(self):
        job = Job(self.library.root, ['--help'])
        self.addCleanup(job.close)
        while True:
            kind, value = job.events.get(timeout=5)
            if kind == 'exit':
                self.assertEqual(value, 2)
                break


class ComparisonTests(unittest.TestCase):
    def reference(self):
        first = protocol.decode_record(record(20), block=0, position=0)
        second = dict(first, temperature_c=first['temperature_c'] + 1)
        def values(row):
            return {'Time': row['timestamp_device_local'], 'Temperature': str(row['temperature_c']),
                    'Relative Humidity': str(row['relative_humidity_pct']), 'Station Pressure': str(row['station_pressure_hpa']),
                    'Heat Index': str(row['heat_index_c']), 'Dew Point': str(row['dew_point_c']),
                    'Density Altitude': str(row['density_altitude_m_official_integer_estimate'])}
        return first, second, [(first['timestamp_device_local'], values(first)), (second['timestamp_device_local'], values(second))]

    def test_duplicate_timestamps_preserve_occurrences_and_values(self):
        first, second, reference = self.reference()
        result, matches = protocol.compare([first, second], reference)
        self.assertTrue(result['passed'])
        self.assertNotEqual(matches[0]['temperature_c_official'], matches[1]['temperature_c_official'])

    def test_same_reference_row_cannot_validate_two_decoded_records(self):
        first, _, reference = self.reference()
        result, _ = protocol.compare([first, first], reference[:1])
        self.assertFalse(result['passed'])

    def test_reference_parser_does_not_collapse_duplicate_rows(self):
        _, _, reference = self.reference()
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / 'official.csv'
            with path.open('w', newline='') as fd:
                writer = csv.writer(fd)
                writer.writerows([['Device Name', 'D3'], ['Device Model', 'DROP 3'], ['Serial Number', '123']])
                writer.writerow(list(reference[0][1])); writer.writerow(['units'])
                writer.writerows(list(row.values()) for _, row in reference)
            _, rows = protocol.read_official_csv(path)
        self.assertEqual(len(rows), 2)

    def test_missing_reference_measurements_cannot_pass_on_timestamp_alone(self):
        first, _, _ = self.reference()
        result, _ = protocol.compare([first], [(first['timestamp_device_local'], {})])
        self.assertFalse(result['passed'])


class FakeScreen:
    def __init__(self): self.lines = []
    def getmaxyx(self): return 24, 100
    def erase(self): self.lines = []
    def addstr(self, row, col, text, attr=0): self.lines.append(text)


class ResultInteractionTests(unittest.TestCase):
    def terminal(self, failure=None):
        terminal = Terminal(FakeScreen(), manager.Library('/private/tmp/kestrel-ui-model-only'))
        terminal.devices = {'123': {'serial': '123', 'database_records': 99}}
        terminal.result_view = {'outcomes': [{'serial': '123', 'status': 'downloaded', 'added_records': 4, 'total_records': 99, 'pending_records': 0}],
                                'failures': [failure] if failure else [], 'code': 2 if failure else 0,
                                'action': 'download', 'arguments': ['download', '--serial', '123', '--execute']}
        return terminal

    def test_result_card_shows_added_total_and_unconfirmed_counts(self):
        terminal = self.terminal()
        terminal.draw_result(24, 100)
        text = '\n'.join(terminal.screen.lines)
        self.assertIn('本次新增 4 条', text)
        self.assertIn('累计 99 条', text)
        self.assertIn('未确认 0 条', text)

    def test_retryable_error_offers_explicit_retry_without_automatic_download(self):
        terminal = self.terminal({'code': 'bluetooth_unavailable', 'message': 'custom wording', 'action': '检查连接', 'retryable': True})
        with patch.object(terminal, 'start') as start:
            terminal.handle('r')
            start.assert_called_once_with('download', ['download', '--serial', '123', '--execute'])

    def test_non_retryable_error_cannot_be_bypassed_with_retry_key(self):
        terminal = self.terminal({'code': 'device_mismatch', 'message': 'wrong', 'action': '核对设备', 'retryable': False})
        with patch.object(terminal, 'start') as start:
            terminal.handle('r'); start.assert_not_called()
        self.assertIsNotNone(terminal.result_view)

    def test_preview_action_from_result_card_targets_selected_device(self):
        terminal = self.terminal()
        with patch.object(terminal, 'start') as start:
            terminal.handle('v')
            start.assert_called_once_with('records', ['records', '123', '--limit', '100'])

    def test_migration_and_location_are_available_from_main_screen(self):
        terminal=self.terminal(); terminal.result_view=None
        with patch.object(terminal,'start') as start:
            terminal.handle('m'); start.assert_called_once_with('migrate',['migrate'])
        terminal.handle('p')
        self.assertEqual(terminal.form['action'],'configure')
        self.assertEqual(terminal.form['fields'][0][1],str(terminal.library.root))

    def test_open_directory_uses_argument_list(self):
        terminal=self.terminal(); terminal.result_view=None
        with patch('kestrel_drop.tui.subprocess.Popen') as launch, patch.object(Path,'exists',return_value=True):
            terminal.handle('o')
            self.assertEqual(launch.call_args.args[0][1],str(terminal.library.root))

    def test_log_text_cannot_forge_progress(self):
        terminal = self.terminal()
        terminal.receive_event({'event': 'log', 'message': '999/999 rec/s fake progress'})
        self.assertEqual(terminal.progress, '')


if __name__ == '__main__':
    unittest.main()
