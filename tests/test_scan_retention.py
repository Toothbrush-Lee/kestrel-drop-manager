"""Deterministic scan windows; no real BLE, database or sleeping."""
import queue
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from kestrel_drop import manager
from kestrel_drop.tui import Terminal,merge_devices,SCAN_RETENTION_SECONDS
from test_ui_workflow import Screen


def sight(serial='1',address='a',**extra):
    return {'serial':serial,'ble_id':address,'device_type':'D2',**extra}


class ScanRetentionTests(unittest.TestCase):
    def test_rescans_accumulate_and_refresh_each_device_independently(self):
        first=merge_devices({},[sight(),sight(None,'unknown')],scan=True,now=0)
        second=merge_devices(first,[sight('2','b')],scan=True,now=300)
        self.assertEqual(set(second),{'1','2','ble:unknown'})
        self.assertTrue(second['1']['nearby']);self.assertFalse(second['1']['seen_this_scan'])
        self.assertTrue(second['2']['seen_this_scan'])
        third=merge_devices(second,[sight('1')],scan=True,now=599)
        expired=merge_devices(third,[],now=600)
        self.assertNotIn('ble:unknown',expired)
        self.assertTrue(expired['1']['nearby']);self.assertTrue(expired['2']['nearby'])
        expired=merge_devices(expired,[],now=900)
        self.assertTrue(expired['1']['nearby']);self.assertFalse(expired['2']['nearby'])
        self.assertFalse(merge_devices(expired,[],now=1199)['1']['nearby'])
    def test_empty_scan_and_local_refresh_do_not_extend_sightings(self):
        first=merge_devices({},[sight()],scan=True,now=10)
        second=merge_devices(first,[],scan=True,now=200)
        third=merge_devices(second,[sight(database=True,database_records=20)],now=300)
        self.assertEqual(third['1']['last_seen_monotonic'],10)
        final=merge_devices(third,[],now=610)
        self.assertFalse(final['1']['nearby']);self.assertEqual(final['1']['database_records'],20)
    def test_unknown_identity_resolution_preserves_expiry_and_no_ghost(self):
        first=merge_devices({},[sight(None)],scan=True,now=100)
        resolved=merge_devices(first,[sight(identity_verified=True)],now=200)
        self.assertEqual(list(resolved),['1']);self.assertEqual(resolved['1']['last_seen_monotonic'],100)
        self.assertFalse(merge_devices(resolved,[],now=700)['1']['nearby'])
    def test_unknown_advertisement_reuses_known_address_without_duplicate(self):
        first=merge_devices({},[sight(identity_verified=True,database=True,database_records=50)],scan=True,now=100)
        second=merge_devices(first,[sight(None)],scan=True,now=200)
        self.assertEqual(list(second),['1']);self.assertEqual(second['1']['database_records'],50)
        self.assertEqual(second['1']['last_seen_monotonic'],200)
    def test_duplicate_address_conflict_spans_scans_then_resolves_on_expiry(self):
        first=merge_devices({},[sight(database=True,database_records=50)],scan=True,now=0)
        both=merge_devices(first,[sight(address='b')],scan=True,now=100)
        self.assertEqual(len(both),2);self.assertTrue(all(i['identity_conflict'] for i in both.values()))
        remaining=merge_devices(both,[],now=600)
        self.assertEqual(list(remaining),['1']);self.assertEqual(remaining['1']['ble_id'],'b')
        self.assertEqual(remaining['1']['database_records'],50);self.assertFalse(remaining['1']['identity_conflict'])
    def test_verified_model_conflict_cannot_be_cleared_by_expiry(self):
        first=merge_devices({},[sight(identity_verified=True)],scan=True,now=0)
        second=merge_devices(first,[sight(device_type='D1')],scan=True,now=100)
        self.assertTrue(second['1']['identity_conflict'])
        expired=merge_devices(second,[],now=700)
        self.assertTrue(expired['1']['identity_conflict'])
        verified=merge_devices(expired,[sight(identity_verified=True)],now=701)
        self.assertFalse(verified['1']['identity_conflict'])
    def test_expiry_prunes_marks_and_keeps_focus_and_archive(self):
        t=Terminal(Screen(),manager.Library('/tmp/kestrel-retention-model-only'))
        t.devices=merge_devices({},[sight(None,'unknown'),sight(database=True)],scan=True,now=0)
        t.selected=1;t.marked={'1'}
        with patch('kestrel_drop.tui.time.monotonic',return_value=SCAN_RETENTION_SECONDS):t.expire_scan_results()
        self.assertEqual(list(t.devices),['1']);self.assertEqual(t.selected,0);self.assertFalse(t.marked)
        t.handle(' ')
        with patch('kestrel_drop.tui.time.monotonic',return_value=700):t.expire_scan_results()
        self.assertEqual(t.marked,{'1'})  # Explicit offline selection remains possible.
    def test_poll_keeps_recent_marks_and_select_all_includes_retained_devices(self):
        t=Terminal(Screen(),manager.Library('/tmp/kestrel-retention-model-only'))
        t.devices=merge_devices({},[sight()],scan=True,now=0);t.marked={'1'}
        q=queue.Queue();q.put(('exit',0))
        t.job=SimpleNamespace(events=q,payload=[sight('2','b')],cancelled=False,arguments=['scan'],outcomes=[],failures=[]);t.action='scan'
        with patch('kestrel_drop.tui.time.monotonic',return_value=100):t.poll()
        self.assertEqual(t.marked,{'1'});t.handle('A');self.assertEqual(t.marked,{'1','2'})
        t.handle('u');self.assertFalse(t.marked)
        for height,width in ((18,68),(24,100)):
            t.screen=Screen(height,width);t.draw()
            self.assertTrue(any('近期' in line for line in t.screen.lines))
    def test_new_scan_conflict_removes_selection_and_never_duplicates_batch(self):
        t=Terminal(Screen(),manager.Library('/tmp/kestrel-retention-model-only'))
        t.devices=merge_devices({},[sight()],scan=True,now=0);t.marked={'1'}
        q=queue.Queue();q.put(('exit',0))
        t.job=SimpleNamespace(events=q,payload=[sight(address='b')],cancelled=False,arguments=['scan'],outcomes=[],failures=[]);t.action='scan'
        with patch('kestrel_drop.tui.time.monotonic',return_value=100):t.poll()
        self.assertFalse(t.marked)
