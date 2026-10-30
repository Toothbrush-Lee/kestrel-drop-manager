"""Local inventory, participant privacy boundaries, schema-4 migration and TUI editing."""
import csv
import io
import sqlite3
import tempfile
import unittest
import time
from contextlib import redirect_stdout,redirect_stderr
from pathlib import Path
from unittest.mock import patch

from kestrel_drop import manager,storage
from kestrel_drop.database import Store
from kestrel_drop.metadata import clean,label_plan,export_labels
from kestrel_drop.browse import Browser
from kestrel_drop.tui import Terminal,merge_devices
from test_database import fixture
from test_ui_workflow import Screen
from legacy_schema import remove_metadata


class MetadataTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.library=manager.Library(self.root/'data')
        self.library.import_archive(fixture(self.root));self.store=self.library.store
        self.store.register_device('2','D1')
    def sheet(self,rows):
        path=self.root/'labels.csv'
        with path.open('w',encoding='utf-8-sig',newline='') as f:
            w=csv.writer(f);w.writerow(['编号','管理编号','设备序列号','型号','受试者代号']);w.writerows(rows)
        return path
    def test_codes_are_text_unique_optional_and_audited_without_touching_observations(self):
        original=self.store.query('2866855')
        self.library.metadata('2866855',management_code='d3-001',participant_code='0007')
        info=self.library.metadata('2866855')
        self.assertEqual((info['management_code'],info['participant_code']),('D3-001','0007'))
        self.assertEqual(self.store.query('2866855'),original)
        self.library.metadata('2866855',participant_code='P-002')
        action=self.store.actions('2866855')[0]
        self.assertEqual(action['before']['participant_code'],'0007');self.assertEqual(action['after']['participant_code'],'P-002')
        self.assertIn('local_metadata',action['requested'])
        self.library.metadata('2866855',participant_code='P-002')
        self.assertEqual(len(self.store.actions('2866855')),2)
        self.library.metadata('2866855',participant_code='')
        self.assertEqual(self.library.metadata('2866855')['participant_code'],'')
        self.store.check()
    def test_same_participant_can_have_multiple_devices_but_management_label_is_unique(self):
        self.library.metadata('2',management_code='D1-001',participant_code='P01')
        self.library.metadata('2866855',participant_code='P01')
        with self.assertRaisesRegex(ValueError,'占用'):self.library.metadata('2866855',management_code='D1-001')
        self.assertEqual(self.library.metadata('2866855')['management_code'],'')
    def test_failed_batch_rolls_back_prior_update_and_audit(self):
        self.library.metadata('2',management_code='D1-001')
        with self.assertRaises(ValueError):self.store.update_metadata([('2',{'participant_code':'P02'}),('2866855',{'management_code':'D1-001'})])
        self.assertEqual(self.library.metadata('2')['participant_code'],'')
        self.assertEqual(len(self.store.actions('2')),1)
    def test_invalid_codes_and_unknown_device_are_rejected(self):
        for code in ('=SUM(A1)','张三','P\n1','../name','a'*65):
            with self.assertRaises(ValueError):clean(code,'participant_code')
        with self.assertRaises(ValueError):self.store.update_metadata([('999',{'participant_code':'P01'})])
        with self.assertRaises(ValueError):self.store.update_metadata([('2',{'alias':'x'})])
        with self.assertRaises(ValueError):self.store.update_metadata([('2',{}),('2',{})])
    def test_label_preview_is_readonly_and_execute_preserves_existing_participant(self):
        self.library.metadata('2',participant_code='0001')
        path=self.sheet([['D1-001-2','D1-001','2','Kestrel DROP 1','']])
        before=self.library.database_path.read_bytes()
        self.assertEqual(self.library.import_labels(path)['devices'],1)
        self.assertEqual(before,self.library.database_path.read_bytes())
        self.library.import_labels(path,execute=True)
        self.assertEqual(self.library.metadata('2')['participant_code'],'0001')
        self.assertEqual(self.library.metadata('2')['management_code'],'D1-001')
    def test_import_rejects_unknown_duplicate_model_and_combined_label_conflicts(self):
        for rows in ([['x','D1-001','999','Kestrel DROP 1','']],
                     [['D1-001-2','D1-001','2','Kestrel DROP 3','']],
                     [['wrong','D1-001','2','Kestrel DROP 1','']],
                     [['D1-001-2','D1-001','2','Kestrel DROP 1','']]*2):
            with self.assertRaises(ValueError):label_plan(self.store,self.sheet(rows))
    def test_export_includes_current_assignment_without_fabricating_observation_owner(self):
        self.library.metadata('2',management_code='D1-001',participant_code='0001')
        path=self.root/'result.csv';export_labels(self.store,path)
        with path.open(encoding='utf-8-sig') as f:items=list(csv.DictReader(f))
        item=next(r for r in items if r['设备序列号']=='2')
        self.assertEqual(item['编号'],'D1-001-2');self.assertEqual(item['受试者代号'],'0001')
        with self.assertRaises(FileExistsError):export_labels(self.store,path)
        self.assertNotIn('participant_code',self.store.query('2866855')[0])
        self.assertEqual(Browser(self.library.root).catalog()[0]['participant_code'],'0001')
    def downgrade(self):
        with self.store.transaction() as c:remove_metadata(c);c.execute('PRAGMA user_version=4')
    def test_schema4_readonly_then_upgrade_backs_up_and_preserves_records(self):
        self.downgrade();old=Store(self.library.database_path);original=old.query('2866855')
        before=self.library.database_path.read_bytes();self.assertEqual(old.status()[0]['management_code'],'');old.check()
        self.assertEqual(before,self.library.database_path.read_bytes())
        self.assertEqual(Browser(self.library.root).catalog()[0]['participant_code'],'')
        result=storage.migrate(self.library.root);self.assertEqual(result['schema_version'],5)
        backups=list((self.library.root/'backups').glob('schema4-before-upgrade-*.sqlite'))
        self.assertEqual(len(backups),1);self.assertEqual(Store(backups[0]).check()['schema_version'],4)
        self.assertEqual(Store(self.library.database_path).query('2866855'),original)
    def test_schema4_ddl_failure_rolls_back_and_keeps_readable_backup(self):
        self.downgrade()
        with self.store.transaction() as c:c.execute('CREATE INDEX devices_management_code ON devices(alias)')
        with self.assertRaises(sqlite3.OperationalError):Store(self.library.database_path,create=True)
        old=Store(self.library.database_path);self.assertEqual(old.check()['schema_version'],4)
        with old.connect() as c:self.assertNotIn('participant_code',{r[1] for r in c.execute('PRAGMA table_info(devices)')})
        self.assertTrue(list((self.library.root/'backups').glob('schema4-before-upgrade-*.sqlite')))
    def test_cli_json_contract_works_without_bluetooth(self):
        output=io.StringIO()
        with redirect_stdout(output),redirect_stderr(io.StringIO()):
            code=manager.main(['--data-dir',str(self.library.root),'--events-json','metadata','2','--participant-code','0001'])
        self.assertEqual(code,0);self.assertIn('0001',output.getvalue())
        self.assertIn('"event": "finished"',output.getvalue())
    def test_tui_edit_cancel_review_target_clear_and_save(self):
        t=Terminal(Screen(),self.library);t.devices={item['serial']:item for item in self.library.status()}
        target=t.selected_device()['serial'];t.handle('z');self.assertEqual(t.form['target'],target)
        t.form['fields'][0][1]='D1-001';t.form['fields'][1][1]='0001'
        with patch.object(t,'start',return_value=True) as start:
            t.submit();start.assert_not_called();t.draw()
            t.handle('\x1b');self.assertIsNotNone(t.form)
            t.submit();t.handle('\n');start.assert_called_once_with('metadata',['metadata',target,'--management-code','D1-001','--participant-code','0001','--expect-management-code','','--expect-participant-code',''])
        t.handle('z');t.form['fields'][1][1]='';t.submit()
        self.assertIn('未分配',t.review['new'])
        self.assertIsNone(t.device_state)
    def test_scan_does_not_overwrite_metadata_and_unknown_device_cannot_be_edited(self):
        current={'2':{'serial':'2','management_code':'D1-001','participant_code':'P01'}}
        result=merge_devices(current,[{'serial':'2','ble_id':'abc','name':'DROP'}],scan=True)
        self.assertEqual(result['2']['participant_code'],'P01')
        t=Terminal(Screen(),self.library);t.devices=current;t.handle('z')
        self.assertIsNone(t.form);self.assertIn('核验登记',t.message)

    def test_real_worker_saves_then_refreshes_tui_and_metadata_survives_backup(self):
        t=Terminal(Screen(),self.library);t.devices={item['serial']:item for item in self.library.status()}
        t.start('metadata',['metadata','2','--participant-code','0007'])
        deadline=time.monotonic()+10
        while t.job and time.monotonic()<deadline:
            job=t.job;job.thread.join(.05);t.poll()
        if t.job:
            t.job.cancel();self.fail('本地worker未按时完成')
        self.assertEqual(t.devices['2']['participant_code'],'0007')
        self.assertIsNone(t.result_view)
        path=self.root/'backup.sqlite';self.store.backup(path)
        restored=Store(path);self.assertEqual(restored.device('2')['participant_code'],'0007')
        self.assertIn('local_metadata',restored.actions('2')[0]['requested'])
    def test_device_page_edits_its_own_local_metadata_without_setting_stale_flag(self):
        t=Terminal(Screen(),self.library);t.devices={item['serial']:item for item in self.library.status()}
        t.device_state={'serial':'2866855'};t.handle('z')
        self.assertEqual(t.form['target'],'2866855')
        t.form['fields'][0][1]='D3-001';t.submit()
        with patch.object(t,'start',return_value=True):t.handle('\n')
        self.assertNotIn('read_stale',t.device_state)
    def test_wrong_model_label_and_missing_read_are_clear_errors(self):
        with self.assertRaisesRegex(ValueError,'前缀'):self.library.metadata('2',management_code='D3-002')
        with self.assertRaises(FileNotFoundError):self.library.metadata('999')
    def test_unique_index_contract_rejects_nonunique_replacement(self):
        with self.store.transaction() as c:
            c.execute('DROP INDEX devices_management_code')
            c.execute('CREATE INDEX devices_management_code ON devices(management_code)')
        with self.assertRaisesRegex(RuntimeError,'唯一索引'):self.store.check()

    def test_stale_tui_snapshot_cannot_overwrite_newer_assignment(self):
        self.library.metadata('2',participant_code='P02')
        with self.assertRaisesRegex(ValueError,'已被其他操作更新'):
            self.library.metadata('2',participant_code='P03',expected={'participant_code':''})
        self.assertEqual(self.library.metadata('2')['participant_code'],'P02')
        self.assertEqual(len(self.store.actions('2')),1)
    def test_narrow_tui_shows_local_scope_warning_and_keeps_manual_add_shortcut(self):
        t=Terminal(Screen(18,68),self.library);t.devices={item['serial']:item for item in self.library.status()}
        t.handle('z');t.form['fields'][1][1]='P'+'0'*63;t.submit();t.draw()
        self.assertIn('仅当前领用信息',' '.join(t.screen.lines))
        t.handle('\x1b');t.handle('\x1b');t.handle('n')
        self.assertEqual(t.form['action'],'add')

    def test_malformed_csv_has_clear_validation_error_and_does_not_write(self):
        path=self.root/'bad.csv'
        for content in ('设备序列号,管理编号,管理编号\n2,D1-001,D1-002\n',
                        '设备序列号,管理编号\n2,D1-001,extra\n',
                        '设备序列号,管理编号\n,D1-001\n'):
            path.write_text(content)
            with self.assertRaises(ValueError):self.library.import_labels(path,execute=True)
        self.assertEqual(self.library.metadata('2')['management_code'],'')
