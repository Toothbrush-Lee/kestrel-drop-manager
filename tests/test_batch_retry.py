"""Manual retries narrow the original batch, re-read state and never replay success."""
from contextlib import redirect_stdout
from datetime import datetime
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace as Item
import unittest
from unittest.mock import patch

from kestrel_drop import batch_settings,control,manager
from kestrel_drop.errors import BluetoothFailure
from kestrel_drop.tui import Terminal
from kestrel_drop.ui_actions import retry_batch_settings_plan
from test_official import FakeControl
from test_ui_workflow import Screen


def arguments(*flags):
    return ['set-batch','--serials','123','456','789','--ble-ids','a','-','c',*(flags or ('--interval','600')),'--timeout','17','--execute']


class RetryPlanningTests(unittest.TestCase):
    def test_success_is_excluded_and_failed_unknown_and_missing_are_included(self):
        args=arguments();saved=list(args)
        for status in ('failed','cancelled_unknown','not_attempted'):
            plan=retry_batch_settings_plan(args,[{'serial':'123','status':'verified'},{'serial':'456','status':status}],{})
            self.assertEqual([t['serial'] for t in plan['targets']],['456','789'])
            self.assertEqual(plan['arguments'],['set-batch','--serials','456','789','--ble-ids','-','c','--interval','600','--timeout','17','--execute'])
        self.assertEqual(args,saved)
    def test_all_success_returns_no_plan_and_does_not_recreate_empty_batch(self):
        self.assertIsNone(retry_batch_settings_plan(arguments(),[{'serial':s,'status':'verified'} for s in ('123','456','789')],{}))
    def test_confirmation_keeps_original_addresses_not_changed_dashboard_selection(self):
        plan=retry_batch_settings_plan(arguments(),[{'serial':'123','status':'verified'}],{'456':{'ble_id':'wrong','participant_code':'0007'}})
        self.assertIsNone(plan['targets'][0]['ble_id']);self.assertEqual(plan['targets'][0]['participant_code'],'0007')
    def test_now_and_fixed_clock_keep_explicit_safety_ack_and_targets(self):
        for value in ('now','2026-10-30 12:00:00'):
            plan=retry_batch_settings_plan(arguments('--time',value,'--allow-schedule-shift'),[{'serial':'123','status':'verified'}],{})
            self.assertIn('--allow-schedule-shift',plan['arguments']);self.assertEqual(plan['arguments'][plan['arguments'].index('--time')+1],value)
            self.assertIn('采样相位' if value=='now' else '可能已过时',plan['impact'])
    def test_multi_field_request_and_option_order_are_preserved(self):
        args=arguments('--interval','600','--wrap','off','--update-seconds','60')
        plan=retry_batch_settings_plan(args,[],{})
        self.assertEqual(plan['arguments'],args)
        self.assertIn('滚动覆盖：off',plan['new'])
    def test_invalid_scope_or_results_cannot_start_retry(self):
        bad=[['set-batch'],arguments()[:-1],arguments('--interval','0'),arguments('--time','now'),arguments('--name','All'),arguments('--interval','600','--interval','300')]
        for args in bad:
            with self.subTest(args=args),self.assertRaises(ValueError):retry_batch_settings_plan(args,[],{})
        for results in ([{'serial':'999','status':'verified'}],[{'serial':'123','status':'unknown'}],None):
            with self.assertRaises(ValueError):retry_batch_settings_plan(arguments(),results,{})
    def test_success_always_wins_over_a_duplicate_error_report(self):
        plan=retry_batch_settings_plan(arguments(),[{'serial':'123','status':'verified'},{'serial':'123','status':'failed'}],{})
        self.assertNotIn('123',[item['serial'] for item in plan['targets']])


class RetryUiTests(unittest.TestCase):
    def terminal(self,height=24,width=100):
        t=Terminal(Screen(height,width),manager.Library('/tmp/kestrel-retry-model-only'))
        t.devices={s:{'serial':s,'ble_id':a,'participant_code':'0007'} for s,a in [('123','a'),('456','changed'),('789','c')]}
        t.marked={'123','456','789'}
        t.result_view={'action':'set-batch','arguments':arguments(),'outcomes':[{'serial':'123','status':'verified'},{'serial':'456','status':'failed','error':{'code':'configuration_unknown','message':'lost response','retryable':False}}],
                       'failures':[{'serial':'456','retryable':False}],'code':2,'selected':0}
        return t
    def test_r_previews_only_incomplete_even_if_error_is_nonretryable(self):
        t=self.terminal();old=t.result_view
        t.draw();self.assertIn('本次目标 3 台 · 未成功 2 台',' '.join(t.screen.lines))
        with patch.object(t,'start') as start:t.handle('r');start.assert_not_called()
        self.assertEqual([i['serial'] for i in t.review['targets']],['456','789'])
        self.assertIs(t.result_view,old);self.assertEqual(t.marked,{'123','456','789'})
        t.handle('\x1b');self.assertIsNone(t.review);self.assertIs(t.result_view,old)
    def test_enter_confirms_exact_targets_and_reduces_marks_only_after_start(self):
        t=self.terminal();t.handle('r');t.marked={'123'}
        expected=t.review['arguments'][:]
        with patch.object(t,'start',return_value=True) as start:t.handle('\n')
        start.assert_called_once_with('set-batch',expected)
        self.assertIsNone(t.review);self.assertIsNone(t.result_view);self.assertEqual(t.marked,{'456','789'})
    def test_spawn_failure_keeps_original_results_and_review(self):
        t=self.terminal();old=t.result_view;t.handle('r')
        with patch.object(t,'start',return_value=False):t.handle('\n')
        self.assertIs(t.result_view,old);self.assertIsNotNone(t.review);self.assertEqual(t.marked,{'123','456','789'})
    def test_all_success_has_visible_notice_and_no_retry_option(self):
        t=self.terminal();t.result_view['outcomes']=[{'serial':s,'status':'verified'} for s in t.devices]
        with patch.object(t,'start') as start:t.handle('r');start.assert_not_called()
        t.draw();self.assertIn('无需重试',' '.join(t.screen.lines));self.assertIsNone(t.review)
        self.assertFalse(any('r 仅重试' in line for line in t.screen.lines))
    def test_successes_from_earlier_retry_round_cannot_reappear(self):
        t=self.terminal();t.handle('r');args=t.review['arguments']
        t.review=None;t.result_view={'action':'set-batch','arguments':args,'outcomes':[{'serial':'456','status':'verified'},{'serial':'789','status':'failed'}],'failures':[],'code':2}
        t.handle('r');self.assertEqual([i['serial'] for i in t.review['targets']],['789'])
    def test_preview_and_footer_fit_small_window_with_fixed_time_warning(self):
        for size in ((18,68),(24,100)):
            t=self.terminal(*size);t.result_view['arguments']=arguments('--time','2026-10-30 12:00:00','--allow-schedule-shift')
            t.draw();self.assertTrue(any('r 仅重试未成功项' in line for line in t.screen.lines))
            t.handle('r');t.draw();self.assertIn('固定校时目标',' '.join(t.screen.lines));self.assertIn('2026-10-30 12:00:00',' '.join(t.screen.lines))
    def test_no_automatic_launch_while_refresh_worker_is_active(self):
        t=self.terminal();t.job=Item()
        with patch.object(t,'start') as start:t.handle('r');start.assert_not_called()
        self.assertIsNone(t.review);self.assertIn('等待',t.result_view['notice'])
        t.job=None;t.handle('r')
        self.assertIsNotNone(t.review);self.assertNotIn('notice',t.result_view)


class RetryHardwareSimulationTests(unittest.IsolatedAsyncioTestCase):
    async def test_lost_write_response_is_reconciled_without_rewrite_and_success_not_connected(self):
        peers={'a':FakeControl(serial='123'),'b':FakeControl(serial='456',fail_after=1),'c':FakeControl(serial='789')}
        factories={address:peer.module.BleakClient for address,peer in peers.items()};calls=[]
        async def resolve(args):
            calls.append(args.serial)
            if args.serial=='789' and calls.count('789')==1:raise BluetoothFailure('not seen')
            return Item(address=args.ble_id)
        with tempfile.TemporaryDirectory() as temporary:
            library=manager.Library(Path(temporary)/'archive')
            with peers['a'].patches(),patch('bleak.BleakClient',side_effect=lambda device,**kw:factories[device.address](device,**kw)),patch.object(manager.devices,'resolve',side_effect=resolve),redirect_stdout(io.StringIO()):
                initial=await batch_settings.configure(library,{'123':'a','456':'b','789':'c'},{'interval_seconds':600},execute=True)
                self.assertEqual([r['status'] for r in initial['configurations']],['verified','failed','failed'])
                plan=retry_batch_settings_plan(['set-batch','--serials','123','456','789','--ble-ids','a','b','c','--interval','600','--execute'],initial['configurations'],{})
                peers['b'].fail_after=0
                final=await batch_settings.configure(library,{i['serial']:i['ble_id'] for i in plan['targets']},{'interval_seconds':600},execute=True)
            self.assertEqual(calls,['123','456','789','456','789'])
            self.assertEqual([r['status'] for r in final['configurations']],['verified','verified'])
            self.assertTrue(final['configurations'][0]['settings_write_skipped'])
            self.assertEqual([len(peers[a].writes) for a in ('a','b','c')],[1,1,1])
            audits=library.store.actions('456');self.assertEqual([a['status'] for a in audits],['verified','failed_or_unknown'])
            self.assertTrue(audits[0]['after']['settings_write_skipped'])
    async def test_reread_failure_never_sends_settings_and_near_current_clock_is_not_rewritten(self):
        fake=FakeControl();library=None
        class Clock(datetime):
            @classmethod
            def now(cls,tz=None):return cls(2026,10,26,12,0,0)
        with tempfile.TemporaryDirectory() as temporary:
            library=manager.Library(temporary)
            with fake.patches(),patch.object(manager.devices,'resolve',return_value=Item(address='a')),patch.object(control,'datetime',Clock),redirect_stdout(io.StringIO()):
                result=await batch_settings.configure(library,{'123':'a'},{'time':'now'},execute=True,allow_schedule_shift=True)
            self.assertTrue(result['configurations'][0]['time_write_skipped']);self.assertFalse(fake.writes)
            self.assertFalse(any(frame.kind==0 and frame.payload[:2]==b'\x0f\0' for frame in fake.mass_writes))
            with fake.patches(),patch.object(manager.devices,'resolve',return_value=Item(address='a')),patch.object(control,'inspect',return_value={'settings':None,'read_errors':{'settings':'failed'}}),redirect_stdout(io.StringIO()):
                result=await batch_settings.configure(library,{'123':'a'},{'interval_seconds':600},execute=True)
            self.assertEqual(result['configurations'][0]['status'],'failed');self.assertFalse(fake.writes)
