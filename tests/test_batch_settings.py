"""Explicit selection, per-device settings isolation and audited cancellation."""
import asyncio
from contextlib import redirect_stdout, redirect_stderr
import curses
import io
import json
from pathlib import Path
import queue
import tempfile
from types import SimpleNamespace as Item
import unittest
from unittest.mock import AsyncMock, patch

from kestrel_drop import batch_settings, control, events, manager
from kestrel_drop.tui import Terminal
from kestrel_drop.worker import Job, validate_event, validate_result
from test_official import FakeControl
from test_ui_workflow import Screen


class SelectionTests(unittest.TestCase):
    def terminal(self):
        t=Terminal(Screen(),manager.Library('/tmp/kestrel-batch-ui-model-only'))
        t.devices={str(i):{'serial':str(i),'ble_id':f'a{i}','device_type':'D2','nearby':True} for i in range(100)}
        return t
    def test_enter_downloads_all_marks_regardless_of_cursor(self):
        t=self.terminal();t.handle(' ');t.handle(curses.KEY_DOWN);t.handle(' ');t.handle(curses.KEY_END)
        with patch.object(t,'start') as start:
            t.handle('\n');start.assert_called_once_with('download',['download','--serials','0','1','--ble-ids','a0','a1','--execute'])
    def test_empty_selection_never_widens_to_everyone(self):
        t=self.terminal();t.handle('A');t.handle('u')
        with patch.object(t,'start') as start:t.handle('a');start.assert_not_called()
        self.assertIn('尚未勾选',t.message)
    def test_select_all_then_deselect_one(self):
        t=self.terminal();t.handle('A');t.handle(' ')
        with patch.object(t,'start') as start:t.handle('a')
        args=start.call_args.args[1];self.assertNotIn('0',args);self.assertEqual(len(t.marked),99)
    def test_invalid_mark_blocks_entire_operation(self):
        t=self.terminal();t.marked={'0','missing'}
        with patch.object(t,'start') as start:t.handle('\n');start.assert_not_called()
        self.assertIn('missing',t.message)
    def test_successful_scan_clears_old_marks(self):
        t=self.terminal();t.marked={'0'}
        q=queue.Queue();q.put(('exit',0))
        t.job=Item(events=q,payload=[],cancelled=False,arguments=['scan'],outcomes=[],failures=[]);t.action='scan'
        t.poll();self.assertFalse(t.marked)
    def test_batch_time_review_requires_confirmation_and_keeps_targets(self):
        t=self.terminal();t.marked={'0','1'}
        with patch.object(t,'start',return_value=True) as start:
            t.handle('g');t.handle('3');t.handle('\n');start.assert_not_called()
            self.assertEqual([i['serial'] for i in t.review['targets']],['0','1'])
            t.marked={'99'};t.handle('\n')
            start.assert_called_once_with('set-batch',['set-batch','--serials','0','1','--ble-ids','a0','a1','--time','now','--execute'])
        self.assertIsNone(t.batch_settings)
    def test_preview_escape_preserves_form_without_writing(self):
        t=self.terminal();t.marked={'0','1'}
        with patch.object(t,'start') as start:
            t.handle('g');t.handle('1');t.handle('\n');t.handle('\x1b');start.assert_not_called()
        self.assertIsNotNone(t.form);self.assertIsNone(t.review)
    def test_hundred_targets_fit_and_can_reach_end_in_every_overlay(self):
        for height,width in ((18,68),(24,100)):
            t=self.terminal();t.screen=Screen(height,width);t.handle('A');t.handle('g');t.handle(curses.KEY_END);t.draw()
            self.assertEqual(t.batch_settings['offset'],99)
            t.handle('3');t.draw();t.handle('\n');t.handle(curses.KEY_END);t.draw()
            self.assertEqual(t.review['offset'],99)
            self.assertTrue(any('99' in line for line in t.screen.lines))
    def test_settings_never_offer_automatic_retry_even_if_flag_is_wrong(self):
        t=self.terminal();t.result_view={'action':'set-batch','arguments':['set-batch'],'outcomes':[],
          'failures':[{'retryable':True}],'code':2}
        with patch.object(t,'start') as start:t.handle('r');start.assert_not_called()
    def test_configured_worker_event_updates_result_without_download_counts(self):
        t=self.terminal();outcome={'version':1,'event':'configured','serial':'0','status':'verified','action_id':1}
        q=queue.Queue();q.put(('event',outcome));q.put(('exit',0))
        t.job=Item(events=q,payload=None,cancelled=False,arguments=['set-batch'],outcomes=[],failures=[]);t.action='set-batch'
        validate_event(outcome)
        with patch.object(t,'start'):t.poll()
        t.draw();self.assertEqual(t.result_view['outcomes'][0]['status'],'verified')
        self.assertNotIn('新增',' '.join(t.screen.lines))


class BatchConfigurationTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)/'archive';self.library=manager.Library(self.root)
    async def test_dry_run_never_creates_data_or_touches_ble(self):
        with patch.object(self.library,'device_operation',wraps=self.library.device_operation) as operation,patch.object(manager.devices,'resolve') as resolve:
            result=await batch_settings.configure(self.library,{'123':'a','456':'b'},{'time':'now'})
        resolve.assert_not_called();self.assertFalse(self.root.exists());self.assertEqual(operation.await_count,2)
        self.assertEqual([i['status'] for i in result['configurations']],['dry_run','dry_run'])
    async def test_invalid_value_rejected_before_any_operation(self):
        with patch.object(self.library,'device_operation') as operation:
            with self.assertRaises(ValueError):await batch_settings.configure(self.library,{'123':'a'},{'interval_seconds':123},execute=True)
        operation.assert_not_called();self.assertFalse(self.root.exists())
    async def test_invalid_targets_never_create_data(self):
        for serials,addresses in (([],None),(['1','2'],['a']),(['1','2'],['A','a']),(['1','1'],['a','b'])):
            with self.subTest(serials=serials,addresses=addresses),self.assertRaises(ValueError):batch_settings.targets(serials,addresses)
        self.assertFalse(self.root.exists())
    async def test_real_control_two_devices_keep_unrequested_bytes_and_audits(self):
        peers={'a':FakeControl(serial='123'),'b':FakeControl(serial='456')}
        # Save original client factories before installing a routing factory.
        factories={a:p.module.BleakClient for a,p in peers.items()}
        first=peers['a'];second_raw=bytearray(peers['b'].values[control.SETTINGS_UUID]);second_raw[4]=60
        peers['b'].values[control.SETTINGS_UUID]=bytes(second_raw)
        before={a:p.values[control.SETTINGS_UUID] for a,p in peers.items()}
        async def resolve(args):return Item(address=args.ble_id)
        with first.patches(),patch('bleak.BleakClient',side_effect=lambda device,**kw:factories[device.address](device,**kw)),patch.object(manager.devices,'resolve',side_effect=resolve):
            result=await batch_settings.configure(self.library,{'123':'a','456':'b'},{'interval_seconds':600},execute=True)
        self.assertEqual([i['status'] for i in result['configurations']],['verified','verified'])
        for address,peer in peers.items():
            self.assertEqual(peer.values[control.SETTINGS_UUID][3:],before[address][3:])
        for serial in ('123','456'):
            self.assertEqual(self.library.store.actions(serial)[0]['status'],'verified')
    async def test_one_readback_failure_does_not_block_other_device(self):
        peers={'a':FakeControl(serial='123',reject=True),'b':FakeControl(serial='456')}
        factories={a:p.module.BleakClient for a,p in peers.items()}
        async def resolve(args):return Item(address=args.ble_id)
        with peers['a'].patches(),patch('bleak.BleakClient',side_effect=lambda device,**kw:factories[device.address](device,**kw)),patch.object(manager.devices,'resolve',side_effect=resolve):
            result=await batch_settings.configure(self.library,{'123':'a','456':'b'},{'interval_seconds':600},execute=True)
        self.assertEqual([i['status'] for i in result['configurations']],['failed','verified'])
        self.assertFalse(result['incomplete'][0]['retryable'])
        self.assertEqual(self.library.store.actions('123')[0]['status'],'failed_or_unknown')
    async def test_cancel_stops_remaining_and_preserves_unknown_audit(self):
        fake=FakeControl()
        async def cancel(*args,**kwargs):raise asyncio.CancelledError()
        output=io.StringIO()
        with fake.patches(),patch.object(manager.devices,'resolve',return_value=Item(address='a')),patch.object(control,'apply',side_effect=cancel),events.stream(output):
            with self.assertRaises(asyncio.CancelledError):await batch_settings.configure(self.library,{'123':'a','456':'b'},{'time':'now'},execute=True)
        result=next(json.loads(line)['data'] for line in output.getvalue().splitlines() if json.loads(line)['event']=='result')
        self.assertEqual([i['status'] for i in result['configurations']],['cancelled_unknown','not_attempted'])
        self.assertEqual(self.library.store.actions('123')[0]['status'],'failed_or_unknown')
        self.assertEqual(self.library.store.actions('456'),[])
    async def test_real_clock_writes_each_device_current_time_and_audits(self):
        from datetime import datetime
        stamps=iter([datetime(2026,10,27,1,0,1),datetime(2026,10,27,1,0,8)])
        class Clock(datetime):
            @classmethod
            def now(cls, tz=None):return next(stamps)
        peers={'a':FakeControl(serial='123'),'b':FakeControl(serial='456')}
        factories={a:p.module.BleakClient for a,p in peers.items()}
        async def resolve(args):return Item(address=args.ble_id)
        with peers['a'].patches(),patch('bleak.BleakClient',side_effect=lambda device,**kw:factories[device.address](device,**kw)),patch.object(manager.devices,'resolve',side_effect=resolve),patch.object(control,'datetime',Clock):
            result=await batch_settings.configure(self.library,{'123':'a','456':'b'},{'time':'now'},execute=True)
        self.assertEqual([i['time_write_target_local'] for i in result['configurations']],['2026-10-27 01:00:01','2026-10-27 01:00:08'])
        for serial,address in [('123','a'),('456','b')]:
            self.assertEqual(self.library.store.actions(serial)[0]['status'],'verified')
            self.assertTrue(any(int.from_bytes(frame.payload[:2],'little')==15 for frame in peers[address].mass_writes if frame.kind==0))

    async def test_batch_time_now_is_resolved_at_each_write(self):
        # Pass now through unchanged to existing per-device clock implementation.
        calls=[]
        async def operation(serial,**kwargs):
            calls.append((serial,kwargs['changes']['time']))
            return {'action_id':len(calls),'after':{'time_write_target_local':f'2026-10-27 01:00:0{len(calls)}'}}
        with patch.object(self.library,'device_operation',side_effect=operation):
            result=await batch_settings.configure(self.library,{'123':'a','456':'b'},{'time':'now'},execute=True)
        self.assertEqual(calls,[('123','now'),('456','now')])
        self.assertNotEqual(result['configurations'][0]['time_write_target_local'],result['configurations'][1]['time_write_target_local'])


class BatchCLIAndTransportTests(unittest.TestCase):
    def test_cli_dry_run_and_json_worker(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)/'not-created'
            job=Job(root,['set-batch','--serials','123','456','--time','now'])
            job.thread.join(timeout=10);self.assertFalse(job.thread.is_alive());self.assertFalse(job.protocol_error)
            events_=[]
            while not job.events.empty():events_.append(job.events.get_nowait())
            self.assertIn(('exit',0),events_);self.assertFalse(root.exists())
            result=next(value['data'] for kind,value in events_ if kind=='event' and value['event']=='result')
            validate_result('set-batch',result);self.assertEqual(len(result['configurations']),2)
    def test_cli_failed_batch_returns_two_and_preserves_results(self):
        data={'configurations':[{'serial':'123','status':'failed'},{'serial':'456','status':'verified'}],'incomplete':[{'serial':'123'}]}
        with patch('kestrel_drop.batch_settings.configure',new=AsyncMock(return_value=data)),redirect_stdout(io.StringIO()) as output,redirect_stderr(io.StringIO()):
            code=manager.main(['set-batch','--serials','123','456','--time','now','--execute'])
        self.assertEqual(code,2);self.assertEqual(json.loads(output.getvalue()),data)
