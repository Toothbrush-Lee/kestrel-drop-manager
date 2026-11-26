from types import SimpleNamespace
import unittest
import queue
import time
from kestrel_drop.ui_sort import ordered_devices
from kestrel_drop.tui import Terminal
from kestrel_drop.manager import Library
from test_ui_workflow import Screen

class DeviceSortTests(unittest.TestCase):
    def devices(self):
        return {'a':{'serial':'10','management_code':'D2-010','participant_code':'10-W'},
                'b':{'serial':'2','management_code':'D1-002','participant_code':'2-Z'},
                'c':{'serial':'3','management_code':'D1-010','participant_code':''},
                'd':{'serial':'4','management_code':'','participant_code':'2-A'},
                'e':{'serial':None,'ble_id':'unknown'}}
    def terminal(self):
        t=Terminal(Screen(24,100),Library('/tmp/kestrel-sort-ui-model'))
        t.devices={item.get('serial') or key:item for key,item in self.devices().items()};t.marked={'2','10'};return t
    def test_management_and_participant_natural_order_with_empty_values_last(self):
        self.assertEqual(list(ordered_devices(self.devices())),['b','c','a','d','e'])
        self.assertEqual(list(ordered_devices(self.devices(),'participant')),['d','b','a','c','e'])
    def test_switch_preserves_selected_identity_and_marks_while_worker_active(self):
        t=self.terminal();t.job=SimpleNamespace();t.selected=1
        t.handle('k');self.assertEqual(t.sort_mode,'participant')
        self.assertEqual(t.selected_device()['serial'],'2');self.assertEqual(t.marked,{'2','10'})
        t.handle('k');self.assertEqual(t.sort_mode,'management');self.assertEqual(t.selected_device()['serial'],'2')
        t.draw();self.assertTrue(any('排序：设备编号' in line for line in t.screen.lines))
    def test_new_scan_and_metadata_changes_keep_sort_and_current_device(self):
        t=self.terminal();t.handle('k');serial=t.selected_device()['serial']
        t.devices=t.merge_sorted_devices([{'serial':'5','management_code':'D3-001','participant_code':'1-Q','ble_id':'new'}],scan=True)
        self.assertEqual(next(iter(t.devices)),'5');self.assertEqual(t.selected_device()['serial'],serial)
        t.devices=t.merge_sorted_devices([{'serial':'10','participant_code':'0-A'}])
        self.assertEqual(next(iter(t.devices)),'10');self.assertEqual(t.selected_device()['serial'],serial)
    def test_leading_zero_codes_and_conflict_rows_are_not_merged_or_changed(self):
        devices={'1':{'serial':'1','participant_code':'01-A'},'2':{'serial':'2','participant_code':'1-A'},'2@x':{'serial':'2','participant_code':'1-A','ble_id':'x','identity_conflict':True}}
        result=ordered_devices(devices,'participant')
        self.assertEqual(len(result),3);self.assertEqual(result['1']['participant_code'],'01-A')
        self.assertTrue(result['2@x']['identity_conflict'])
    def test_result_and_form_keys_do_not_change_dashboard_order(self):
        t=self.terminal();t.open_form('test',[('field','')],'metadata');t.handle('k')
        self.assertEqual(t.sort_mode,'management');self.assertEqual(t.form['fields'][0][1],'k')

    def test_scan_keeps_exact_selected_row_when_serial_has_address_conflict(self):
        t=self.terminal();now=time.monotonic()
        t.devices={'2':{'serial':'2','ble_id':'a','nearby':True,'last_seen_monotonic':now},'2@b':{'serial':'2','ble_id':'b','nearby':True,'last_seen_monotonic':now}}
        t.selected=1;q=queue.Queue();q.put(('exit',0))
        t.job=SimpleNamespace(events=q,cancelled=False,payload=[{'serial':'2','ble_id':'b'}],failures=[],outcomes=[],arguments=['scan'])
        t.action='scan';t.poll()
        self.assertEqual(t.selected_device()['ble_id'],'b')

    def test_management_numbers_are_global_across_model_prefixes(self):
        devices={'a':{'management_code':'D2-044'},'b':{'management_code':'D3-025'},'c':{'management_code':'D1-002'}}
        self.assertEqual(list(ordered_devices(devices)),['c','b','a'])
