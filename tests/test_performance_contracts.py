"""Behavioral contracts for performance changes; no real BLE or archive writes."""
from contextlib import contextmanager, redirect_stdout, redirect_stderr
import csv
import curses
import io
from pathlib import Path
import queue
import tempfile
from types import ModuleType, SimpleNamespace as Item
import sys
import unittest
from unittest.mock import AsyncMock, Mock, patch

from kestrel_drop import control, devices, manager
from kestrel_drop.database import FIELDS
from kestrel_drop.tui import Terminal
from kestrel_drop.ui_text import wrap_lines
from test_database import fixture, rows
from test_official import FakeControl
from test_ui_workflow import Screen


class BatchLocatorTests(unittest.IsolatedAsyncioTestCase):
    def scanner(self, addresses):
        self.visited=[];self.timeout=None
        module=ModuleType('bleak');exc=ModuleType('bleak.exc')
        exc.BleakError=type('BleakError',(Exception,),{})
        async def locate(predicate,timeout):
            self.timeout=timeout
            for address in addresses:
                self.visited.append(address)
                device=Item(address=address,name='D1 - 123')
                adv=Item(local_name=device.name,rssi=-50,service_uuids=[])
                if predicate(device,adv):return device
            return None
        self.full=AsyncMock(return_value={})
        module.BleakScanner=Item(find_device_by_filter=locate,discover=self.full)
        return patch.dict(sys.modules,{'bleak':module,'bleak.exc':exc})
    async def test_waits_for_every_address_and_stops_at_last(self):
        with self.scanner(['other','A','b','never']):
            found=await devices.scan_snapshot(20,addresses=['a','B'])
        self.assertEqual(self.visited,['other','A','b']);self.assertEqual(set(found),{'other','a','b'})
        self.full.assert_not_awaited();self.assertEqual(self.timeout,20)
    async def test_missing_peer_keeps_observations_for_normal_failure_handling(self):
        with self.scanner(['a','other']):found=await devices.scan_snapshot(20,addresses=['a','missing'])
        self.assertEqual(set(found),{'a','other'});self.assertEqual(self.visited,['a','other'])
        self.full.assert_not_awaited()
    async def test_standard_scan_still_uses_full_window(self):
        with self.scanner(['a']):await devices.scan_snapshot(15)
        self.full.assert_awaited_once_with(timeout=15,return_adv=True);self.assertEqual(self.visited,[])
    async def test_duplicate_case_variants_do_not_require_duplicate_advertisements(self):
        with self.scanner(['a','never']):await devices.scan_snapshot(20,addresses=['A','a'])
        self.assertEqual(self.visited,['a'])
    async def test_cancel_never_falls_back_to_second_scanner(self):
        import asyncio
        with self.scanner([]),patch('bleak.BleakScanner.find_device_by_filter',side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):await devices.scan_snapshot(20,addresses=['a'])
        self.full.assert_not_awaited()
    def test_cli_only_shortens_shared_lookup_when_all_targets_have_addresses(self):
        for addresses,expected in ((['a','b'],['a','b']),(['a','-'],None)):
            with tempfile.TemporaryDirectory() as directory,patch.object(manager.Library,'download',new=AsyncMock(return_value=0)),patch.object(devices,'scan_snapshot',new=AsyncMock(return_value={})) as scan,redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO()):
                code=manager.main(['--data-dir',directory,'download','--serials','1','2','--ble-ids',*addresses,'--execute'])
            self.assertEqual(code,0);scan.assert_awaited_once_with(20,addresses=expected)


class ScopedStatusAndExportTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.library=manager.Library(self.root/'archive')
        self.library.import_archive(fixture(self.root))
        self.store=self.library.store;self.store.ensure_device('2','D3')
    def test_scoped_status_is_identical_to_filtering_full_status(self):
        self.assertEqual(self.store.status('2866855'),[i for i in self.store.status() if i['serial']=='2866855'])
        self.assertEqual(self.store.status('999'),[])
    def test_scoped_status_never_decodes_another_history(self):
        tid=self.store.begin_transfer('2',0,1)
        decoded=rows((20,));self.store.append_block(tid,decoded,[{'block':0,'records':1}])
        self.store.finish_transfer(tid,next_cursor=1,elapsed_seconds=1,maxrows=20000,received_rows=decoded)
        original=self.store._pending_state
        def pending(connection,history):
            if history['serial']=='2':raise AssertionError('other history decoded')
            return original(connection,history)
        with patch.object(self.store,'_pending_state',side_effect=pending):
            self.store.status('2866855');self.library.complete_download('2866855',previous=3)
    def test_corrupt_pending_of_target_remains_visible(self):
        tid=self.store.begin_transfer('2866855',103,1)
        self.store.append_block(tid,rows((30,)),[{'block':0,'records':1}])
        with self.store.transaction() as connection:connection.execute("UPDATE pending_blocks SET sha256='broken' WHERE transfer_id=?",(tid,))
        self.assertIn('error',self.store.status('2866855')[0])
        with self.assertRaises(RuntimeError):self.library.complete_download('2866855')
    def test_first_attempt_pending_count_stays_equal(self):
        tid=self.store.begin_transfer('2',0,1)
        self.store.append_block(tid,rows((30,)),[{'block':0,'records':1}])
        status=self.store.status('2')[0]
        self.assertEqual(status,next(i for i in self.store.status() if i['serial']=='2'))
        self.assertEqual((status['pending_records'],status['resume_cursor']),(1,0))
    def test_stream_and_list_keep_order_filter_and_raw_fields(self):
        for filters in ({},{'since':'2026-10-22 14:25:00'},{'until':'2026-10-22 14:25:00','end_exclusive':True}):
            with self.subTest(filters=filters):self.assertEqual(list(self.store.iter_query('2866855',**filters)),self.store.query('2866855',**filters))
        self.assertEqual([r['absolute_cursor'] for r in self.store.query('2866855',limit=2)],[101,102])
    def test_export_bytes_and_count_match_previous_csv_format(self):
        expected=io.StringIO(newline='')
        writer=csv.DictWriter(expected,fieldnames=['serial','history_generation','absolute_cursor','device_sequence',*FIELDS])
        writer.writeheader();writer.writerows(self.store.query('2866855'))
        output=self.root/'export.csv'
        self.assertEqual(self.library.export('2866855',output),3)
        self.assertEqual(output.read_bytes(),expected.getvalue().encode('utf-8-sig'))
    def test_snapshot_is_stable_across_fetch_batches_and_concurrent_append(self):
        self.store.import_rows('3','D3',rows((20,))*1500,1500,{})
        iterator=self.store.iter_query('3');self.assertEqual(next(iterator)['absolute_cursor'],0)
        tid=self.store.begin_transfer('3',1500,1)
        decoded=rows((30,));self.store.append_block(tid,decoded,[{'block':0,'records':1}])
        self.store.finish_transfer(tid,next_cursor=1501,elapsed_seconds=1,maxrows=20000,received_rows=decoded)
        self.assertEqual(len(list(iterator)),1499)
        self.assertEqual(len(self.store.query('3')),1501)
    @contextmanager
    def track_connections(self):
        self.active=0;original=self.store.connect
        @contextmanager
        def connect(**kwargs):
            with original(**kwargs) as connection:
                self.active+=1
                try:yield connection
                finally:self.active-=1
        with patch.object(self.store,'connect',connect):yield
    def test_early_stream_close_releases_read_connection(self):
        with self.track_connections():
            iterator=self.store.iter_query('2866855');next(iterator)
            self.assertEqual(self.active,1);iterator.close();self.assertEqual(self.active,0)
    def test_export_error_and_cancel_close_stream_and_remove_partial_file(self):
        original=csv.DictWriter.writerow
        for exception in (OSError('disk failure'),KeyboardInterrupt()):
            counter=0
            def write(writer,row):
                nonlocal counter
                counter+=1
                if counter==3:raise exception
                return original(writer,row)
            destination=self.root/'exports'/'failed.csv'
            with self.track_connections(),patch.object(csv.DictWriter,'writerow',write),self.assertRaises(type(exception)):
                self.library.export('2866855',destination)
            self.assertEqual(self.active,0);self.assertFalse(destination.exists())
            self.assertEqual(list(destination.parent.iterdir()),[])


class TextAndRedrawTests(unittest.TestCase):
    def terminal(self):return Terminal(Screen(),manager.Library('/tmp/kestrel-perf-ui-model-only'))
    def test_text_cache_handles_resize_in_place_update_and_new_content(self):
        t=self.terminal();t.text_view=['中文'*100]
        with patch('kestrel_drop.tui.wrap_lines',wraps=wrap_lines) as wrap:
            first=t.wrapped_text();t.wrapped_text();self.assertEqual(wrap.call_count,1)
            t.text_offset=1;t.draw();self.assertEqual(wrap.call_count,1)
            t.screen.size=(18,68);self.assertNotEqual(t.wrapped_text(),first);self.assertEqual(wrap.call_count,2)
            t.text_view[0]='changed';self.assertEqual(t.wrapped_text(),['changed']);self.assertEqual(wrap.call_count,3)
    def test_reopening_log_includes_new_entries(self):
        t=self.terminal();t.logs.append('before');t.handle('l');t.wrapped_text();t.handle('\x1b')
        t.logs.append('after');t.handle('l');self.assertIn('after',t.wrapped_text())
    def test_opaque_overlay_does_not_render_hidden_dashboard(self):
        t=self.terminal();t.handle('?')
        with patch.object(t,'draw_dashboard') as dashboard:t.draw();dashboard.assert_not_called()
    def run_loop(self,events,setup=None):
        t=self.terminal();screen=t.screen;screen.reads=0;screen.refresh_count=0
        screen.timeout=lambda _:None
        def refresh():screen.refresh_count+=1
        screen.refresh=refresh
        def read():
            event=events[screen.reads];screen.reads+=1
            if callable(event):event=event(t)
            if event is None:raise curses.error('no input')
            return event
        screen.get_wch=read
        if setup:setup(t)
        with patch.object(t,'start',return_value=False):t.loop()
        return t
    def test_idle_timeouts_do_not_redraw(self):
        t=self.run_loop([None]*10+['q']);self.assertEqual(t.screen.refresh_count,1)
    def test_keyboard_and_resize_still_redraw(self):
        def resize(t):t.screen.size=(18,68)
        t=self.run_loop([None,curses.KEY_DOWN,resize,None,'q'])
        self.assertEqual(t.screen.refresh_count,3)
    def test_backend_event_marks_frame_dirty(self):
        t=self.terminal();t.needs_redraw=False
        t.receive_event({'event':'log','message':'new'});self.assertTrue(t.needs_redraw)
    def test_running_job_animation_is_throttled_and_quit_still_cancels(self):
        holder={}
        def setup(t):
            holder['t']=t
            def cancel():t.job=None
            t.job=Item(events=queue.Queue(),cancel=Mock(side_effect=cancel))
            holder['job']=t.job
        with patch('kestrel_drop.tui.time.monotonic',side_effect=lambda:holder['t'].screen.reads*0.1):
            t=self.run_loop([None]*10+['q'],setup=setup)
        self.assertEqual(t.screen.refresh_count,4);holder['job'].cancel.assert_called_once()


class ConfigurationReadReuseTests(unittest.IsolatedAsyncioTestCase):
    async def test_stable_fields_reused_but_dynamic_values_fresh_and_clock_verified(self):
        fake=FakeControl()
        with fake.patches(),patch.object(fake.client,'read_gatt_char',wraps=fake.client.read_gatt_char) as read:
            before=await control.inspect(fake.client)
            fake.values[control.BATTERY_UUID]=b'\x50'
            after=await control.apply(fake.client,{'time':'2026-10-27 01:00:00'},before)
        names=[call.args[0] for call in read.await_args_list]
        self.assertEqual(names.count(control.FIRMWARE_UUID),1);self.assertEqual(names.count(control.HARDWARE_UUID),1)
        self.assertEqual(names.count(control.BATTERY_UUID),2);self.assertEqual(after['battery_pct'],80)
        commands=[int.from_bytes(frame.payload[:2],'little') for frame in fake.mass_writes if frame.kind==0]
        self.assertEqual(commands.count(14),2);self.assertEqual(commands.count(15),1)
        self.assertEqual(after['device_time'],'2026-10-27 01:00:00')
    async def test_failed_stable_field_is_retried_not_cached_as_success(self):
        fake=FakeControl();original=fake.client.read_gatt_char;fail=True
        async def read(uuid):
            nonlocal fail
            if uuid==control.FIRMWARE_UUID and fail:
                fail=False;raise fake.exc.BleakError('temporary failure')
            return await original(uuid)
        with fake.patches(),patch.object(fake.client,'read_gatt_char',side_effect=read):
            before=await control.inspect(fake.client);self.assertIn('firmware',before['read_errors'])
            after=await control.apply(fake.client,{'interval_seconds':600},before)
        self.assertEqual(after['firmware'],'1.56');self.assertNotIn('firmware',after['read_errors'])
    async def test_device_operation_reports_stage_times_without_changing_audit_evidence(self):
        fake=FakeControl()
        with tempfile.TemporaryDirectory() as directory:
            library=manager.Library(directory)
            with fake.patches(),patch.object(devices,'resolve',return_value=Item(address='a')):
                result=await library.device_operation('123',ble_id='a',changes={'interval_seconds':600},execute=True)
            self.assertEqual(set(result['timings_seconds']),{'locating','connecting','identity','read_before','write_verify','audit','total'})
            self.assertTrue(all(value>=0 for value in result['timings_seconds'].values()))
            self.assertEqual(library.store.actions('123')[0]['status'],'verified')
            self.assertEqual(library.store.actions('123')[0]['after'],result['after'])
