from datetime import datetime,timezone
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch,AsyncMock

from kestrel_drop import control,followup_clock,manager,followup
from test_official import FakeControl
from kestrel_drop.tui import Terminal
from test_ui_workflow import Screen

class FollowupClockTests(unittest.IsolatedAsyncioTestCase):
    def setup_library(self,tmp):
        library=manager.Library(tmp);store=library._writable_store();store.register_device('123','D2')
        store.update_metadata([('123',{'management_code':'D2-123','participant_code':'P007'})])
        return store,{'serial':'123','model':'Kestrel DROP 2','ble_id':'a'}
    def test_thresholds_are_symmetric_and_inclusive(self):
        for seconds,level in [(0,'normal'),(29,'normal'),(30,'drift'),(3599,'drift'),(3600,'manual'),(43200,'half_day'),(86400,'day')]:
            self.assertEqual(followup_clock.severity(seconds),level);self.assertEqual(followup_clock.severity(-seconds),level)
    async def test_read_audit_keeps_subject_raw_time_and_signed_offset(self):
        with tempfile.TemporaryDirectory() as tmp:
            store,identity=self.setup_library(tmp);fake=FakeControl()
            with fake.patches(),patch.object(followup_clock,'computer_now',return_value=datetime(2026,10,26,12,0,45,tzinfo=timezone.utc)):
                report=await followup_clock.check(fake.client,store,identity,phase='before_download')
            self.assertEqual(report['severity'],'drift');self.assertEqual(report['offset_seconds'],-45)
            audit=store.actions('123')[0]
            self.assertEqual(audit['before']['participant_code'],'P007');self.assertEqual(audit['after']['device_time_wire_hex'],report['device_time_wire_hex'])
            self.assertFalse(any(f.payload[:2]==b'\x0f\0' for f in fake.mass_writes))
    async def test_read_failure_and_slow_round_trip_are_registered_and_never_corrected(self):
        with tempfile.TemporaryDirectory() as tmp:
            store,identity=self.setup_library(tmp)
            with patch.object(control,'clock',side_effect=RuntimeError('read failed')):
                report=await followup_clock.check(None,store,identity,phase='before_download')
            self.assertEqual(report['severity'],'read_failed');self.assertEqual(store.actions('123')[0]['status'],'failed_or_unknown')
            timer=iter([0,8])
            with patch.object(control,'clock',return_value=('2026-10-26 12:00:00','00')),patch.object(followup_clock,'time',SimpleNamespace(monotonic=lambda:next(timer))),patch.object(followup_clock,'computer_now',return_value=datetime(2026,10,26,12,0,0,tzinfo=timezone.utc)):
                report=await followup_clock.check(None,store,identity,phase='after_download')
            self.assertEqual(report['severity'],'unreliable_read')
            with patch.object(control,'apply',new_callable=AsyncMock) as apply:
                result=await followup_clock.correct(None,store,identity,report)
                self.assertEqual(result['status'],'held');apply.assert_not_called()
    async def test_minor_correction_is_fresh_verified_and_writes_only_once(self):
        class Clock(datetime):
            @classmethod
            def now(cls,tz=None):return cls(2026,10,26,12,0,45)
        with tempfile.TemporaryDirectory() as tmp:
            store,identity=self.setup_library(tmp);fake=FakeControl()
            with fake.patches(),patch.object(control,'datetime',Clock):
                result=await followup_clock.correct(fake.client,store,identity,{'severity':'drift','offset_seconds':-45})
            self.assertEqual(result['status'],'verified');self.assertEqual(result['after']['time_source'],'computer_at_send')
            self.assertEqual(len([f for f in fake.mass_writes if f.kind==0 and f.payload[:2]==b'\x0f\0']),1)
            self.assertEqual(store.actions('123')[0]['status'],'verified')
    async def test_half_day_offset_and_unresolved_write_are_held(self):
        with tempfile.TemporaryDirectory() as tmp:
            store,identity=self.setup_library(tmp)
            with patch.object(control,'apply',new_callable=AsyncMock) as apply:
                result=await followup_clock.correct(None,store,identity,{'severity':'half_day'})
                self.assertEqual(result['status'],'held');apply.assert_not_called()
            action=store.begin_action('123',{'followup_auto_clock':True},{})
            store.finish_action(action,error='lost ACK')
            with patch.object(control,'apply',new_callable=AsyncMock) as apply:
                result=await followup_clock.correct(None,store,identity,{'severity':'drift'})
                self.assertEqual(result['status'],'held_unknown');apply.assert_not_called()
            manual=store.begin_action('123',{'time':'now'},{})
            store.finish_action(manual,after={'device_time':'2026-10-26 12:00:00'})
            with patch.object(control,'apply',new_callable=AsyncMock,return_value={'device_time':'2026-10-26 12:00:00'}):
                result=await followup_clock.correct(None,store,identity,{'severity':'drift'})
                self.assertEqual(result['status'],'verified')
    async def test_fresh_send_guard_blocks_a_clock_jump_and_audits_unknown(self):
        class Clock(datetime):
            @classmethod
            def now(cls,tz=None):return cls(2026,10,26,13,0,0)
        with tempfile.TemporaryDirectory() as tmp:
            store,identity=self.setup_library(tmp);fake=FakeControl()
            with fake.patches(),patch.object(control,'datetime',Clock):
                result=await followup_clock.correct(fake.client,store,identity,{'severity':'drift'})
            self.assertEqual(result['status'],'unknown');self.assertFalse(any(f.kind==0 and f.payload[:2]==b'\x0f\0' for f in fake.mass_writes))
            self.assertEqual(store.actions('123')[0]['status'],'failed_or_unknown')
    async def test_auto_clock_requires_explicit_phase_ack_before_any_io(self):
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ValueError):await followup.run(manager.Library(Path(tmp)/'none'),sync_clock=True)
            self.assertFalse((Path(tmp)/'none').exists())

class DownloadClockOrderTests(unittest.IsolatedAsyncioTestCase):
    async def test_download_precedes_correction_and_clock_errors_do_not_replay_download(self):
        for pending,post_error in ((False,False),(True,False),(False,True)):
            with tempfile.TemporaryDirectory() as tmp:
                library=manager.Library(tmp);library._writable_store().register_device('123','D2')
                fake=FakeControl();order=[]
                async def check(*args,phase):
                    order.append(phase)
                    if post_error and phase=='after_download':raise RuntimeError('audit failed')
                    return {'severity':'drift','offset_seconds':-45}
                async def sync(*args,**kw):order.append('download');return 3 if pending else 0
                async def correct(*args):order.append('correct');return {'status':'verified'}
                with fake.patches(),patch.object(manager.protocol,'resolve_ble_device',return_value=SimpleNamespace(address='a')),patch.object(library,'_sync_client',side_effect=sync),patch.object(followup_clock,'check',side_effect=check),patch.object(followup_clock,'correct',side_effect=correct):
                    code=await library.download('123',execute=True,clock_policy={'sync':True,'allow_schedule_shift':True})
                self.assertEqual(order[:2],['before_download','download'])
                if pending:
                    self.assertEqual(order,['before_download','download']);self.assertEqual(code,3)
                    self.assertEqual(library.download_results['123']['status'],'pending')
                elif post_error:
                    self.assertNotIn('correct',order);self.assertEqual(code,0)
                    self.assertEqual(library.download_results['123']['status'],'saved_warning')
                else:self.assertEqual(order,['before_download','download','after_download','correct'])

class ClockUiTests(unittest.TestCase):
    def test_followup_auto_clock_toggle_sets_phase_ack_only_when_selected(self):
        for mode in ('on','off','bad'):
            terminal=Terminal(Screen(24,100),manager.Library('/tmp/followup-clock-ui-model'))
            terminal.handle('f');self.assertEqual(terminal.form['fields'][2][1],'off');terminal.form['fields'][2][1]=mode
            with patch.object(terminal,'start',return_value=True) as start:
                terminal.submit()
                if mode=='bad':start.assert_not_called();self.assertTrue(terminal.form['error'])
                else:self.assertEqual('--allow-schedule-shift' in start.call_args.args[1],mode=='on')

    def test_immediate_warning_stays_visible_during_download(self):
        terminal=Terminal(Screen(24,100),manager.Library('/tmp/followup-clock-ui-model'))
        terminal.followup={'parallel':6};terminal.job=SimpleNamespace()
        terminal.receive_event({'event':'phase','serial':'123','phase':'clock_warning','message':'偏差至少半天'})
        terminal.draw()
        self.assertEqual(terminal.devices['123']['clock_warning'],'偏差至少半天')
        self.assertTrue(any('时钟告警 1' in line for line in terminal.screen.lines))
