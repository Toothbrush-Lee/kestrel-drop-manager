import asyncio
from contextlib import contextmanager
import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from kestrel_drop import followup,manager
from kestrel_drop.tui import Terminal
from test_ui_workflow import Screen

class Library:
    def __init__(self,root):
        self.root=Path(root);self.download_results={};self.calls=[];self.active=0;self.peak=0
        self.store=SimpleNamespace(actions=lambda *args,**kw:[])
    def status(self):return [{'serial':str(i),'management_code':'D1-'+str(i),'participant_code':'P'+str(i)} for i in range(1,9)]
    @contextmanager
    def collection(self,*args,**kw):yield
    async def download(self,serial,**kwargs):
        self.calls.append(serial);self.active+=1;self.peak=max(self.peak,self.active)
        try:
            await asyncio.sleep(.01)
            self.download_results[serial]={'serial':serial,'status':'up_to_date','added_records':0,'total_records':1,'pending_records':0}
            return 0
        finally:self.active-=1

class FollowupTests(unittest.IsolatedAsyncioTestCase):
    def observation(self,i):return {'serial':str(i),'ble_id':'a'+str(i),'history_supported':True}
    async def test_arrivals_are_deduplicated_unknowns_skipped_and_concurrency_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            library=Library(tmp);scans=0
            async def scan(_):
                nonlocal scans
                scans+=1;await asyncio.sleep(.02)
                return [self.observation(i) for i in (range(1,5) if scans==1 else range(1,10))]
            with patch.object(followup.devices,'scan_snapshot',side_effect=scan),patch.object(followup.devices,'candidates',side_effect=lambda s:s),patch.object(followup.events,'result'):
                await followup.run(library,rounds=4,execute=True)
            self.assertEqual(sorted(library.calls),[str(i) for i in range(1,9)])
            self.assertGreater(library.peak,1);self.assertLessEqual(library.peak,6)
            journal=json.loads(next((Path(tmp)/'followups').glob('*.json')).read_text())
            self.assertEqual(journal['devices']['1']['participant_code'],'P1')
            self.assertTrue(all(d['status']=='saved' for d in journal['devices'].values()))
    async def test_retry_backoff_and_failure_cap_do_not_hammer_device(self):
        with tempfile.TemporaryDirectory() as tmp:
            library=Library(tmp)
            async def fail(serial,**kw):library.calls.append(serial);return 3
            library.download_results['1']={'serial':'1','status':'pending'}
            with patch.object(library,'download',side_effect=fail),patch.object(followup.devices,'scan_snapshot',return_value=[self.observation(1)]),patch.object(followup.devices,'candidates',side_effect=lambda s:s),patch.object(followup.events,'result'):
                await followup.run(library,rounds=8,execute=True)
            self.assertEqual(library.calls,['1'])
    async def test_transient_failure_stops_after_three_attempts_and_conflict_is_never_scheduled(self):
        with tempfile.TemporaryDirectory() as tmp:
            library=Library(tmp);clock=[0]
            async def scan(_):
                clock[0]+=100
                await asyncio.sleep(0)
                return [self.observation(1),{**self.observation(2),'identity_conflict':True}]
            async def fail(serial,**kw):library.calls.append(serial);return 3
            library.download_results['1']={'serial':'1','status':'pending'}
            with patch.object(library,'download',side_effect=fail),patch.object(followup.devices,'scan_snapshot',side_effect=scan),patch.object(followup.devices,'candidates',side_effect=lambda s:s),patch.object(followup.time,'monotonic',side_effect=lambda:clock[0]),patch.object(followup.events,'result'):
                await followup.run(library,rounds=8,execute=True)
            self.assertEqual(library.calls,['1','1','1'])
            journal=json.loads(next((Path(tmp)/'followups').glob('*.json')).read_text())
            self.assertEqual(journal['devices']['1']['status'],'blocked')

    async def test_cancel_cleans_active_tasks_and_preserves_session(self):
        with tempfile.TemporaryDirectory() as tmp:
            library=Library(tmp);entered=asyncio.Event();cleaned=asyncio.Event();scans=0
            async def download(serial,**kw):
                entered.set()
                try:await asyncio.Event().wait()
                finally:cleaned.set()
            async def scan(_):
                nonlocal scans
                scans+=1
                if scans>1:await entered.wait();raise asyncio.CancelledError()
                return [self.observation(1)]
            with patch.object(library,'download',side_effect=download),patch.object(followup.devices,'scan_snapshot',side_effect=scan),patch.object(followup.devices,'candidates',side_effect=lambda s:s),patch.object(followup.events,'result'):
                with self.assertRaises(asyncio.CancelledError):await followup.run(library,execute=True)
            self.assertTrue(cleaned.is_set())
            journal=json.loads(next((Path(tmp)/'followups').glob('*.json')).read_text())
            self.assertEqual(journal['devices']['1']['status'],'interrupted')
    async def test_preview_does_not_touch_data_or_bluetooth(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)/'absent'
            result=await followup.run(manager.Library(root))
            self.assertEqual(result['parallel'],6);self.assertFalse(root.exists())
        for args in ((9,8,3),(6,float('nan'),3),(6,0,3)):
            with self.assertRaises(ValueError):followup.validate(*args)

class FollowupUiTests(unittest.TestCase):
    def test_start_confirmation_uses_six_connections_and_scan_results_stay_on_dashboard(self):
        terminal=Terminal(Screen(24,100),manager.Library('/tmp/followup-ui-no-write'))
        terminal.handle('f');self.assertEqual(terminal.form['fields'][0][1],'6')
        with patch.object(terminal,'start',return_value=True) as start:terminal.submit()
        self.assertEqual(start.call_args.args[0],'followup')
        terminal.action='followup';terminal.job=SimpleNamespace(payload=None)
        terminal.receive_event({'event':'result','data':{'followup':{'saved':2,'active':1,'parallel':6},'devices':[{'serial':'123','ble_id':'a'}]}})
        self.assertIn('123',terminal.devices);self.assertIsNone(terminal.result_view)
