"""Read-only browser contracts and optional real Streamlit application tests."""
from contextlib import redirect_stdout,redirect_stderr
from datetime import datetime
import csv
import importlib.util
import io
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from kestrel_drop import manager
from kestrel_drop.browse import Browser,filters,launch
from test_database import fixture,rows
from test_manager import record
from kestrel_drop import protocol


class BrowserTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.library=manager.Library(self.root/'data')
        self.library.import_archive(fixture(self.root))
        self.browser=Browser(self.library.root)
        self.start='2026-10-22 14:20:00';self.end='2026-10-22 14:25:00'
    def test_catalog_page_and_csv_do_not_change_database(self):
        before=self.library.database_path.read_bytes()
        self.assertEqual(self.browser.catalog()[0]['records'],3)
        result=self.browser.read('2866855',self.start,self.end,page_size=2)
        self.assertEqual((result['records'],len(result['rows']),result['pages']),(3,2,2))
        self.assertEqual([r['device_sequence'] for r in result['rows']],[101,102])
        body,count=self.browser.csv('2866855',self.start,self.end)
        exported=list(csv.DictReader(io.StringIO(body.decode('utf-8-sig'))))
        self.assertEqual((count,len(exported)),(3,3))
        self.assertEqual(self.library.database_path.read_bytes(),before)
        self.assertFalse(list(self.library.root.glob('*.csv')))
    def test_inclusive_second_boundary_keeps_duplicate_timestamps_and_sequences(self):
        data=self.browser.read('2866855',self.end,self.end)
        self.assertEqual(data['records'],2)
        self.assertEqual([r['device_sequence'] for r in data['points']],[102,103])
        self.assertTrue(all(r['timestamp_device_local']==self.end for r in data['rows']))
    def test_empty_range_and_page_clamping(self):
        result=self.browser.read('2866855','2026-10-23 00:00:00','2026-10-24 00:00:00',page=999)
        self.assertEqual((result['records'],result['page'],result['points']),(0,1,[]))
        result=self.browser.read('2866855',self.start,self.end,page=999,page_size=2)
        self.assertEqual((result['page'],len(result['rows'])),(2,1))
    def test_validation_blocks_bad_filters_and_unbounded_requests(self):
        for serial,start,end in [('1 OR 1=1',self.start,self.end),('1',self.end,self.start),
                                 ('1','2026-10-22T00:00:00Z',self.end),('1',None,self.end)]:
            with self.assertRaises((ValueError,TypeError)):filters(serial,start,end)
        for values in ({'page':0},{'page_size':1000000},{'chart_limit':100000}):
            with self.assertRaises(ValueError):self.browser.read('2866855',self.start,self.end,**values)
        with self.assertRaises(FileNotFoundError):self.browser.read('999',self.start,self.end)
    def test_chart_sampling_is_bounded_and_preserves_endpoints(self):
        self.library.store.import_rows('3','D3',rows((20,))*101,101,{})
        result=self.browser.read('3',self.start,self.end,chart_limit=10,page_size=20)
        self.assertLessEqual(len(result['points']),11)
        self.assertEqual((result['points'][0]['device_sequence'],result['points'][-1]['device_sequence']),(1,101))
        self.assertEqual(len(result['rows']),20)
        with self.assertRaises(ValueError):self.browser.csv('3',self.start,self.end,max_records=100)
        with self.assertRaises(ValueError):self.browser.csv('3',self.start,self.end,max_bytes=100)
    def test_read_only_snapshot_allows_writer_and_does_not_see_partial_new_state(self):
        with self.browser.snapshot() as reader:
            self.assertEqual(reader.execute('SELECT count(*) FROM devices').fetchone()[0],1)
            self.library.store.register_device('2','D1')
            self.assertEqual(reader.execute('SELECT count(*) FROM devices').fetchone()[0],1)
            with self.assertRaises(sqlite3.OperationalError):reader.execute('DELETE FROM devices')
        self.assertEqual(len(self.browser.catalog()),2)

    def test_multi_device_values_pages_export_and_empty_device_share_one_scope(self):
        self.library.store.import_rows('3','D3',rows((20,25)),202,{})
        self.library.store.register_device('2','D1')
        before=self.library.database_path.read_bytes()
        result=self.browser.read_many(['2866855','3','2'],self.start,self.end,page_size=2)
        self.assertEqual((result['records'],result['pages']),(5,3))
        summary={d['serial']:d for d in result['devices']}
        self.assertEqual(summary['2']['records'],0);self.assertIsNone(summary['2']['latest'])
        self.assertEqual(summary['3']['latest']['device_sequence'],202)
        self.assertEqual(summary['2866855']['latest']['device_sequence'],103)
        pages=[self.browser.read_many(['2866855','3','2'],self.start,self.end,page=p,page_size=2)['rows'] for p in (1,2,3)]
        self.assertEqual([(r['serial'],r['device_sequence']) for page in pages for r in page],[('2866855',101),('2866855',102),('2866855',103),('3',201),('3',202)])
        body,count=self.browser.csv_many(['2866855','3','2'],self.start,self.end)
        exported=list(csv.DictReader(io.StringIO(body.decode('utf-8-sig'))))
        self.assertEqual(count,5);self.assertEqual({r['serial'] for r in exported},{'2866855','3'})
        with self.assertRaises(ValueError):self.browser.csv_many(['2866855','3'],self.start,self.end,max_records=4)
        self.assertEqual(self.library.database_path.read_bytes(),before)

    def test_multi_chart_budget_does_not_hide_sparse_device_and_latest_uses_sequence(self):
        self.library.store.import_rows('3','D3',rows((25,20))*100,200,{})
        result=self.browser.read_many(['3','2866855'],self.start,self.end,chart_limit=10)
        self.assertLessEqual(len(result['points']),12)
        for serial,first,last in [('3',1,200),('2866855',101,103)]:
            points=[p for p in result['points'] if p['serial']==serial]
            self.assertEqual((points[0]['device_sequence'],points[-1]['device_sequence']),(first,last))
        dense=next(d for d in result['devices'] if d['serial']=='3')
        self.assertEqual(dense['latest']['timestamp_device_local'],self.start)
        self.assertEqual(dense['last_time'],self.end)

    def test_multi_scope_rejects_empty_duplicate_missing_or_injected_targets(self):
        for values in ([],['2866855','2866855'],['2866855','1 OR 1=1'],['1']*101,None):
            with self.subTest(values=values),self.assertRaises(ValueError):self.browser.read_many(values,self.start,self.end)
        with self.assertRaises(FileNotFoundError):self.browser.read_many(['2866855','999'],self.start,self.end)
    def test_missing_path_is_never_created(self):
        path=self.root/'wrong'
        with self.assertRaises(FileNotFoundError):Browser(path)
        self.assertFalse(path.exists())
    def test_launch_is_loopback_only_and_checks_optional_dependency(self):
        with patch('kestrel_drop.browse.importlib.util.find_spec',return_value=None):
            with self.assertRaisesRegex(ValueError,'extra viewer'):launch(self.library.root)
        with patch('kestrel_drop.browse.importlib.util.find_spec',return_value=object()),patch('kestrel_drop.browse.subprocess.call',return_value=0) as call,redirect_stdout(io.StringIO()):
            self.assertEqual(launch(self.library.root,open_browser=False),0)
        args=call.call_args.args[0]
        self.assertEqual(args[args.index('--server.address')+1],'127.0.0.1')
        self.assertEqual(args[args.index('--browser.gatherUsageStats')+1],'false')
        self.assertEqual(args[args.index('--client.toolbarMode')+1],'viewer')
        with redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO()):
            self.assertEqual(manager.main(['--events-json','browse']),2)


@unittest.skipUnless(importlib.util.find_spec('streamlit'),'Optional viewer dependencies not installed')
class ViewerAppTests(unittest.TestCase):
    def setUp(self):
        from streamlit.testing.v1 import AppTest
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.library=manager.Library(self.root/'data');self.library.import_archive(fixture(self.root))
        self.library.store.register_device('2','D1')
        source=f'from kestrel_drop.viewer import render\nrender({str(self.library.root)!r})'
        self.app=AppTest.from_string(source,default_timeout=20).run()
    def test_first_render_and_export_are_read_only(self):
        self.assertFalse(self.app.exception)
        before=self.library.database_path.read_bytes()
        self.app.multiselect(key='devices').set_value(['2866855']).run()
        self.assertFalse(self.app.exception)
        self.assertEqual(self.app.metric[0].value,'3')
        button=next(b for b in self.app.button if b.label=='准备导出筛选结果')
        button.click().run();self.assertFalse(self.app.exception)
        self.assertEqual(self.library.database_path.read_bytes(),before)
        self.assertEqual(self.app.session_state['csv'][2],3)
    def test_empty_device_and_empty_time_range(self):
        self.app.multiselect(key='devices').set_value(['2']).run()
        self.assertFalse(self.app.exception);self.assertIn('尚无正式观测',self.app.info[0].value)
        self.app.multiselect(key='devices').set_value(['2866855']).run()
        self.app.datetime_input(key='start_2866855').set_value(datetime(2026,10,23))
        self.app.datetime_input(key='end_2866855').set_value(datetime(2026,10,24))
        next(b for b in self.app.button if b.label=='应用筛选').click().run()
        self.assertFalse(self.app.exception);self.assertEqual(self.app.metric[0].value,'0')
    def test_invalid_range_shows_error_without_crashing(self):
        self.app.multiselect(key='devices').set_value(['2866855']).run()
        self.app.datetime_input(key='start_2866855').set_value(datetime(2026,10,24))
        next(b for b in self.app.button if b.label=='应用筛选').click().run()
        self.assertFalse(self.app.exception);self.assertTrue(self.app.error)

    def test_end_minute_keeps_seconds_and_pending_filter_does_not_relabel_results(self):
        raw=bytearray(record(25));raw[0]=47
        decoded=protocol.decode_record(bytes(raw),block=0,position=3)
        self.library.store.import_rows('3','D3',rows((20,25,25))+[decoded],104,{})
        self.app.run().multiselect(key='devices').set_value(['3']).run()
        self.app.datetime_input(key='start_3').set_value(datetime(2026,10,22,14,25))
        self.app.datetime_input(key='end_3').set_value(datetime(2026,10,22,14,25))
        next(b for b in self.app.button if b.label=='应用筛选').click().run()
        self.assertFalse(self.app.exception);self.assertEqual(self.app.metric[0].value,'3')
        self.assertEqual(self.app.session_state['selection'][2],datetime(2026,10,22,14,25,59))
        self.app.datetime_input(key='start_3').set_value(datetime(2026,10,24)).run()
        self.assertFalse(self.app.exception);self.assertEqual(self.app.metric[0].value,'3')

    def test_multi_selection_latest_values_chart_export_and_scope_change(self):
        self.library.store.import_rows('3','D3',rows((20,25)),202,{})
        self.library.metadata('3',management_code='D3-003',participant_code='P007')
        self.app.run().multiselect(key='devices').set_value(['2866855','3','2']).run()
        self.assertFalse(self.app.exception)
        self.assertEqual(self.app.metric[0].value,'5')
        self.assertEqual(self.app.metric[1].value,'2 / 3')
        latest=self.app.dataframe[0].value
        self.assertEqual(len(latest),3)
        self.assertEqual(latest.loc[latest.serial=='3','participant_code'].iloc[0],'P007')
        self.assertEqual(latest.loc[latest.serial=='3','device_sequence'].iloc[0],'202')
        self.assertEqual(latest.loc[latest.serial=='2','records'].iloc[0],0)
        next(b for b in self.app.button if b.label=='准备导出筛选结果').click().run()
        self.assertEqual(self.app.session_state['csv'][2],5)
        self.app.multiselect(key='devices').set_value(['2866855']).run()
        self.assertFalse(self.app.exception);self.assertEqual(self.app.metric[0].value,'3')
        self.assertNotIn('csv',self.app.session_state)

    def test_clear_and_select_all_and_refresh_invalidates_prepared_export(self):
        next(b for b in self.app.button if b.label=='准备导出筛选结果').click().run()
        next(b for b in self.app.button if b.label=='刷新数据').click().run()
        self.assertNotIn('csv',self.app.session_state)
        next(b for b in self.app.button if b.label=='清空选择').click().run()
        self.assertFalse(self.app.exception);self.assertIn('请先选择',self.app.info[0].value)
        next(b for b in self.app.button if b.label=='全选设备').click().run()
        self.assertFalse(self.app.exception);self.assertEqual(self.app.metric[1].value,'1 / 2')
