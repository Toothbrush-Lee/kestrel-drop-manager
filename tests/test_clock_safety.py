"""Clock verification is not sampling-grid verification; preserve original evidence."""
import asyncio
from contextlib import redirect_stdout, redirect_stderr
from datetime import datetime
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from kestrel_drop import control, manager, batch_settings
from kestrel_drop.timing import analyze, format_report
from kestrel_drop.ui_actions import setting_change, batch_setting_change
from kestrel_drop.tui import Terminal
from test_official import FakeControl
from test_database import fixture
from test_ui_workflow import Screen


def samples(times):
    return [{'timestamp_device_local':t,'device_sequence':i+1} for i,t in enumerate(times)]


class SamplingDiagnosticsTests(unittest.TestCase):
    def test_interval_can_be_correct_while_grid_is_shifted(self):
        report=analyze(samples(['2026-10-27 00:00:47','2026-10-27 00:05:47','2026-10-27 00:10:47']))
        self.assertTrue(report['recent_intervals_match']);self.assertFalse(report['recent_grid_aligned'])
        self.assertEqual(report['recent_phase_counts'],[{'offset_seconds':47,'records':3}])
    def test_midnight_grid_is_stable_and_backwards_times_remain_in_sequence(self):
        times=['2026-10-26 23:55:00','2026-10-27 00:00:00','2026-10-27 00:00:00','2026-02-12 16:55:00']
        data=samples(times);original=[dict(r) for r in data];report=analyze(iter(data))
        self.assertTrue(report['recent_grid_aligned']);self.assertFalse(report['recent_intervals_match'])
        self.assertEqual(report['duplicate_time_pairs'],1);self.assertEqual(report['backward_time_pairs'],1)
        self.assertEqual(report['recent_transitions'][-1]['sequence'],4);self.assertEqual(data,original)
        self.assertNotIn('missing_records',report)
    def test_recent_window_does_not_hide_history_transition(self):
        report=analyze(samples(['2026-02-12 16:55:00','2026-10-26 23:55:47','2026-10-27 00:00:47','2026-10-27 00:05:47']),recent=2)
        self.assertTrue(report['recent_intervals_match']);self.assertEqual(report['irregular_pairs'],1)
        self.assertEqual(report['phase_counts'][0]['offset_seconds'],47)
    def test_invalid_parameters_and_empty_records(self):
        for interval,recent in ((0,100),(300,0),(True,100),(300,10001)):
            with self.assertRaises(ValueError):analyze([],interval,recent)
        report=analyze([]);self.assertEqual(report['records'],0);self.assertFalse(report['recent_grid_aligned'])
    def test_timing_cli_is_read_only_and_returns_actions(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp);library=manager.Library(root/'archive');library.import_archive(fixture(root))
            action=library.store.begin_action('2866855',{'time':'now'},{'device_time':'2026-10-26 12:00:00'})
            library.store.finish_action(action,after={'device_time':'2026-10-27 00:00:00'})
            before=library.database_path.read_bytes();output=io.StringIO()
            with patch.object(manager.devices,'resolve',side_effect=AssertionError('no BLE')),redirect_stdout(output):
                code=manager.main(['--data-dir',str(library.root),'timing','2866855'])
            result=json.loads(output.getvalue());self.assertEqual(code,0)
            self.assertEqual(len(result['devices'][0]['clock_actions']),1)
            self.assertEqual(library.database_path.read_bytes(),before)
            self.assertIn('只读',' '.join(format_report(result)))
    def test_tui_diagnostic_form_and_risk_confirmation(self):
        t=Terminal(Screen(),manager.Library('/tmp/kestrel-ui-no-io'))
        t.devices={'1':{'serial':'1'}}
        t.handle('t');self.assertEqual(t.form['action'],'timing')
        with patch.object(t,'start') as start:
            t.submit();start.assert_called_once_with('timing',['timing','1','--interval','300'])
        state={'serial':'1','device_time':'2026-10-27 00:00:00','settings':{'interval_seconds':300}}
        plan=setting_change('set_time','now',state)
        self.assertIn('采样相位',plan['impact']);self.assertIn('--allow-schedule-shift',plan['arguments'])
        self.assertIn('--allow-schedule-shift',batch_setting_change('batch_set_time','now',[state])['arguments'])


class ClockAndSettingsSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_clock_requires_explicit_ack_before_bluetooth_or_data_creation(self):
        with tempfile.TemporaryDirectory() as tmp:
            library=manager.Library(Path(tmp)/'archive')
            with patch.object(manager.devices,'resolve') as resolve:
                with self.assertRaisesRegex(ValueError,'allow-schedule-shift'):
                    await library.device_operation('1',changes={'time':'now'},execute=True)
                with self.assertRaisesRegex(ValueError,'allow-schedule-shift'):
                    await batch_settings.configure(library,{'1':None},{'time':'now'},execute=True)
            resolve.assert_not_called();self.assertFalse(library.root.exists())
    async def test_direct_apply_cannot_mutate_settings_before_clock_ack(self):
        fake=FakeControl()
        with fake.patches():
            before=await control.inspect(fake.client)
            with self.assertRaises(ValueError):await control.apply(fake.client,{'time':'now','interval_seconds':600},before)
        self.assertFalse(fake.writes)
    async def test_same_settings_skip_write_but_still_read_back(self):
        fake=FakeControl()
        with fake.patches():
            before=await control.inspect(fake.client)
            after=await control.apply(fake.client,{'interval_seconds':300,'wrap':True},before)
        self.assertFalse(fake.writes);self.assertTrue(after['settings_write_skipped'])
        self.assertEqual(after['settings_written_fields'],[])
    async def test_only_selected_setting_is_written_with_official_ignore_values(self):
        fake=FakeControl()
        with fake.patches():
            before=await control.inspect(fake.client)
            after=await control.apply(fake.client,{'wrap':False},before)
        raw=fake.writes[0][1]
        self.assertEqual(raw[:3],b'\xff'*3);self.assertEqual(raw[3],0)
        self.assertEqual(raw[4:7],b'\xff'*3);self.assertEqual(raw[7:9],b'\x01\x80')
        self.assertEqual(after['settings']['interval_seconds'],300)
        self.assertEqual(after['settings']['setup_complete'],before['settings']['setup_complete'])
        self.assertEqual(after['settings_written_fields'],['wrap'])
    async def test_nearly_synchronized_clock_is_not_rewritten(self):
        class Clock(datetime):
            @classmethod
            def now(cls,tz=None):return cls(2026,10,26,12,0,1)
        fake=FakeControl()
        with fake.patches(),patch.object(control,'datetime',Clock):
            before=await control.inspect(fake.client)
            after=await control.apply(fake.client,{'time':'now'},before,allow_schedule_shift=True)
        self.assertTrue(after['time_write_skipped'])
        self.assertFalse(any(f.kind==0 and f.payload[:2]==b'\x0f\0' for f in fake.mass_writes))
        self.assertFalse(after['sampling_risk']['schedule_alignment_verified'])
    async def test_large_clock_change_records_correction_without_any_interval_write(self):
        fake=FakeControl()
        with fake.patches():
            before=await control.inspect(fake.client)
            after=await control.apply(fake.client,{'time':'2026-10-26 12:00:47'},before,allow_schedule_shift=True)
        self.assertEqual(after['clock_correction_seconds'],47)
        self.assertEqual(after['clock_before_write_local'],'2026-10-26 12:00:00')
        self.assertFalse(after['time_write_skipped']);self.assertFalse(fake.writes)
        self.assertFalse(after['sampling_risk']['schedule_alignment_verified'])
        self.assertEqual(after['settings']['interval_seconds'],300)
