"""Offline proof of local/device clear separation, backups, opt-in sync and TUI targets."""
import asyncio
from contextlib import redirect_stdout,redirect_stderr
import io
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import AsyncMock,patch

from kestrel_drop import manager,protocol,storage
from kestrel_drop.database import Store
from kestrel_drop.data_management import local_history,erase_device,device_count
from kestrel_drop.errors import ConfigurationFailure,DeviceMismatch
from kestrel_drop.tui import Terminal
from test_database import fixture,rows
from test_manager import FakeBLE,record
from test_ui_workflow import Screen


class EraseFake(FakeBLE):
    def __init__(self,*,lost_ack=False,remaining=0,cancel=False,**kwargs):
        super().__init__([record(30)],104,**kwargs)
        self.erased=False;owner=self;base=self.module.BleakClient
        class Client(base):
            def __init__(self,*args,**kwargs):
                super().__init__(*args,**kwargs);self.management_frames=protocol.FrameStream()
            async def write_gatt_char(self,uuid,raw,response):
                frames=self.management_frames.feed(raw)
                if frames and frames[0].kind==0 and frames[0].payload[:2] in (b'\x01\x00',b'\x02\x00'):
                    frame=frames[0];owner.writes.append(frame)
                    if frame.payload[:2]==b'\x01\x00':
                        owner.erased=True
                        if cancel:raise asyncio.CancelledError()
                        if lost_ack:raise TimeoutError('injected lost erase ACK')
                        self.transmit(4,b'\x01\x00')
                    else:
                        self.transmit(4,b'\x02\x00');self.transmit(3,remaining.to_bytes(4,'little'))
                else:await super().write_gatt_char(uuid,raw,response)
        self.module.BleakClient=Client


class DataManagementTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.library=manager.Library(self.root/'data')
        self.library.import_archive(fixture(self.root));self.store=self.library.store
        self.store.import_rows('2','D3',rows(),103,{})
        self.library.metadata('2866855',management_code='D3-043',participant_code='P001')
    def info(self):return self.store.data_info('2866855')
    def terminal(self):
        t=Terminal(Screen(),self.library);t.devices={r['serial']:r for r in self.library.status()}
        t.selected=list(t.devices).index('2866855');return t
    def test_preferences_default_off_persist_per_device_and_are_backed_up(self):
        self.assertFalse(self.info()['auto_sync'])
        self.library.data_preferences('2866855',auto_sync=True)
        self.assertTrue(Store(self.library.database_path).preferences('2866855')['auto_sync'])
        self.assertFalse(self.store.preferences('2')['auto_sync'])
        count=len(self.store.actions('2866855'));self.library.data_preferences('2866855',auto_sync=True)
        self.assertEqual(len(self.store.actions('2866855')),count)
        path=self.root/'backup.sqlite';self.library.backup(path)
        self.assertTrue(Store(path).preferences('2866855')['auto_sync'])
    def test_data_info_is_readonly_and_revision_covers_participant_and_pending(self):
        before=self.library.database_path.read_bytes();info=self.info()
        self.assertEqual(info['records'],3);self.assertEqual(info['last_record'],'2026-10-22 14:25:00')
        self.assertEqual(before,self.library.database_path.read_bytes())
        self.library.metadata('2866855',participant_code='P002');self.assertNotEqual(info['revision'],self.info()['revision'])
        old=self.info()['revision'];t=self.store.begin_transfer('2866855',103,1)
        self.store.append_block(t,rows((30,)),[{'block':0,'records':1}])
        self.assertNotEqual(old,self.info()['revision']);self.assertEqual(self.info()['pending_blocks'],1)
    def test_clear_requires_explicit_serial_and_revision_and_dry_run_does_not_write(self):
        before=self.library.database_path.read_bytes()
        self.assertEqual(local_history(self.library,'2866855')['status'],'dry_run')
        for values in ({'confirmation':'2','revision':self.info()['revision']},{'confirmation':'2866855'}):
            with self.assertRaises(ValueError):local_history(self.library,'2866855',execute=True,**values)
        self.assertEqual(before,self.library.database_path.read_bytes())
        self.assertFalse((self.library.root/'backups').exists())
    def test_local_clear_only_removes_target_history_and_preserves_metadata_audit_preferences(self):
        self.library.data_preferences('2866855',auto_sync=True);original=self.store.query('2866855')
        t=self.store.begin_transfer('2866855',103,1);self.store.append_block(t,rows((30,)),[{'block':0,'records':1}])
        result=local_history(self.library,'2866855',execute=True,confirmation='2866855',revision=self.info()['revision'])
        snapshot=Store(Path(result['backup']));self.assertEqual(snapshot.query('2866855'),original)
        self.assertEqual(snapshot.data_info('2866855')['pending_blocks'],1)
        self.assertEqual(self.store.query('2866855'),[]);self.assertEqual(len(self.store.query('2')),3)
        self.assertEqual(self.store.device('2866855')['management_code'],'D3-043')
        self.assertEqual(self.store.device('2866855')['participant_code'],'P001')
        self.assertTrue(self.store.preferences('2866855')['auto_sync'])
        self.assertEqual(self.info()['pending_blocks'],0);self.assertEqual(result['generation'],2)
        self.assertIn('clear_local_history',self.store.actions('2866855')[0]['requested']);self.store.check()
    def test_stale_revision_and_failed_backup_prevent_clear(self):
        old=self.info()['revision'];self.library.metadata('2866855',participant_code='P002')
        with self.assertRaisesRegex(ValueError,'已变化'):local_history(self.library,'2866855',execute=True,confirmation='2866855',revision=old)
        with patch.object(self.library,'backup',side_effect=OSError('disk full')):
            with self.assertRaises(OSError):local_history(self.library,'2866855',execute=True,confirmation='2866855',revision=self.info()['revision'])
        self.assertEqual(len(self.store.query('2866855')),3)
    def test_sql_failure_rolls_back_local_clear(self):
        with self.store.transaction() as c:c.execute("CREATE TRIGGER fail_delete BEFORE DELETE ON observations BEGIN SELECT RAISE(ABORT,'injected'); END")
        before=self.store.query('2866855')
        with self.assertRaises(sqlite3.IntegrityError):local_history(self.library,'2866855',execute=True,confirmation='2866855',revision=self.info()['revision'])
        self.assertEqual(self.store.query('2866855'),before);self.assertEqual(self.store.device('2866855')['generation'],1)
    def test_local_clear_can_redownload_retained_device_history_into_new_generation(self):
        local_history(self.library,'2866855',execute=True,confirmation='2866855',revision=self.info()['revision'])
        fake=FakeBLE([record(20),record(25),record(25)],103)
        async def resolve(_):return object()
        with fake.patches(),patch.object(protocol,'resolve_ble_device',resolve),redirect_stdout(io.StringIO()):
            self.assertEqual(asyncio.run(self.library.download('2866855',execute=True)),0)
        self.assertEqual(len(self.store.query('2866855')),3)
        self.assertEqual(self.store.query('2866855')[0]['history_generation'],2);self.store.check()
    def test_schema5_is_readonly_then_backs_up_on_first_preference_write(self):
        with self.store.transaction() as c:c.execute('DROP TABLE device_preferences');c.execute('PRAGMA user_version=5')
        before=self.library.database_path.read_bytes();self.assertFalse(self.store.preferences('2866855')['auto_sync']);self.store.check()
        self.assertEqual(before,self.library.database_path.read_bytes())
        self.library.data_preferences('2866855',auto_sync=True)
        backups=list((self.library.root/'backups').glob('schema5-before-upgrade-*.sqlite'));self.assertEqual(len(backups),1)
        self.assertEqual(Store(backups[0]).check()['schema_version'],5)
        self.assertEqual(self.store.check()['schema_version'],6)
    def test_auto_sync_runs_on_device_connection_before_inspection_without_extra_connection(self):
        self.library.data_preferences('2866855',auto_sync=True);fake=FakeBLE([record(30)],104)
        async def resolve(_):return type('Device',(),{'address':'abc'})()
        with fake.patches(),patch('kestrel_drop.devices.resolve',resolve),patch('kestrel_drop.control.inspect',new_callable=AsyncMock,return_value={'settings':{}}),redirect_stdout(io.StringIO()):
            result=asyncio.run(self.library.device_operation('2866855'))
        self.assertEqual(result['serial'],'2866855');self.assertEqual(len(self.store.query('2866855')),4)
        self.assertEqual(sum(f.kind==0 and f.payload[:2]==b'\x03\x00' for f in fake.writes),1)
    def test_auto_sync_failure_prevents_setting_writes(self):
        self.library.data_preferences('2866855',auto_sync=True);fake=FakeBLE([],103)
        async def resolve(_):return type('Device',(),{'address':'abc'})()
        with fake.patches(),patch('kestrel_drop.devices.resolve',resolve),patch.object(self.library,'_sync_client',new_callable=AsyncMock,side_effect=RuntimeError('injected')),patch('kestrel_drop.control.apply',new_callable=AsyncMock) as apply:
            with self.assertRaises(RuntimeError):asyncio.run(self.library.device_operation('2866855',changes={'wrap':True},execute=True))
            apply.assert_not_called()
    def test_device_erase_dry_run_or_wrong_confirmation_never_connects(self):
        with patch('kestrel_drop.devices.resolve',new_callable=AsyncMock) as resolve:
            self.assertEqual(asyncio.run(erase_device(self.library,'2866855'))['status'],'dry_run')
            with self.assertRaises(ValueError):asyncio.run(erase_device(self.library,'2866855',execute=True,confirmation='2'))
            resolve.assert_not_called()
    def erase(self,fake,**extra):
        async def resolve(_):return type('Device',(),{'address':'abc'})()
        with fake.patches(),patch('kestrel_drop.devices.resolve',resolve),redirect_stdout(io.StringIO()):
            return asyncio.run(erase_device(self.library,'2866855',execute=True,confirmation='2866855',revision=self.info()['revision'],**extra))
    def test_device_erase_syncs_and_backs_up_first_then_preserves_computer_history_and_cursor(self):
        fake=EraseFake();result=self.erase(fake)
        self.assertTrue(fake.erased);self.assertEqual(result['status'],'erased_device')
        self.assertEqual(len(self.store.query('2866855')),4);self.assertEqual(self.store.device('2866855')['next_cursor'],104)
        self.assertEqual(len(Store(Path(result['backup'])).query('2866855')),4)
        self.assertEqual(self.store.device('2866855')['management_code'],'D3-043')
        self.assertEqual(self.store.device('2866855')['participant_code'],'P001')
        self.assertEqual(self.store.actions('2866855')[0]['status'],'verified');self.store.check()
        commands=[int.from_bytes(f.payload[:2],'little') for f in fake.writes if f.kind==0]
        self.assertLess(commands.index(5),commands.index(1));self.assertEqual(commands.count(1),1)
        self.assertNotIn(15,commands);self.assertNotIn(26,commands)
    def test_device_erase_lost_ack_is_unknown_never_retried_and_keeps_download(self):
        fake=EraseFake(lost_ack=True)
        with self.assertRaises(ConfigurationFailure):self.erase(fake)
        self.assertTrue(fake.erased);self.assertEqual(len(self.store.query('2866855')),4)
        action=self.store.actions('2866855')[0];self.assertEqual(action['status'],'failed_or_unknown')
        self.assertEqual(sum(f.kind==0 and f.payload[:2]==b'\x01\x00' for f in fake.writes),1)
    def test_new_record_after_erase_is_not_misreported_as_confirmed_empty(self):
        with self.assertRaises(ConfigurationFailure):self.erase(EraseFake(remaining=1))
        self.assertEqual(len(self.store.query('2866855')),4)
    def test_identity_mismatch_prevents_download_and_erase(self):
        fake=EraseFake(serial='2')
        with self.assertRaises(DeviceMismatch):self.erase(fake)
        self.assertFalse(fake.erased);self.assertFalse(fake.writes)
    def test_sync_or_backup_failure_never_sends_erase(self):
        for method in ('_sync_client','backup'):
            fake=EraseFake()
            failure=AsyncMock(side_effect=RuntimeError('injected')) if method=='_sync_client' else unittest.mock.Mock(side_effect=OSError('disk full'))
            with patch.object(self.library,method,failure):
                with self.assertRaises((RuntimeError,OSError)):self.erase(fake)
            self.assertFalse(fake.erased)
    def test_tui_data_page_targets_current_device_without_connecting_on_open(self):
        t=self.terminal()
        with patch.object(t,'start',return_value=True) as start:
            t.handle('h');start.assert_called_once_with('data-info',['data-info','2866855'])
        t.data_view['info']=self.info();t.draw();self.assertIn('Data Logging Rate',' '.join(t.screen.lines))
        t.handle('7');t.form['fields'][0][1]='2';t.submit();self.assertIn('完整序列号',t.form['error'])
        t.form['fields'][0][1]='2866855'
        with patch.object(t,'start',return_value=True) as start:
            t.submit();start.assert_not_called();self.assertEqual(t.review['action'],'clear-history');t.draw()
            t.handle('\x1b');self.assertIsNotNone(t.form)
            t.submit();t.handle('\n');self.assertEqual(start.call_args.args[0],'clear-history')
    def test_tui_settings_require_read_and_reuse_cached_state_for_preview(self):
        t=self.terminal();t.data_view={'serial':'2866855','info':self.info(),'state':None}
        t.handle('1');self.assertIsNone(t.form);self.assertIn('先按r',t.message)
        t.data_view['state']={'serial':'2866855','settings':{'update_seconds':60,'interval_seconds':300,'wrap':True}}
        t.handle('1');t.form['fields'][0][1]='30';t.submit()
        self.assertEqual(t.review['arguments'],['set','2866855','--update-seconds','30','--execute'])
    def test_data_page_and_clear_review_fit_minimum_terminal(self):
        t=self.terminal();t.screen=Screen(18,68);t.data_view={'serial':'2866855','info':self.info(),'state':None}
        t.draw();t.handle('8');t.form['fields'][0][1]='2866855';t.submit();t.draw()
        self.assertIn('不支持批量',' '.join(t.screen.lines))

    def test_direct_local_clear_rejects_missing_or_live_database_as_backup(self):
        for path in (self.root/'missing.sqlite',self.library.database_path):
            with self.assertRaises((FileNotFoundError,ValueError)):
                self.store.clear_local_history('2866855',backup=path,revision=self.info()['revision'])
        self.assertEqual(len(self.store.query('2866855')),3)
    def test_cancel_during_device_clear_keeps_backup_and_unknown_audit_without_retry(self):
        fake=EraseFake(cancel=True)
        with self.assertRaises(asyncio.CancelledError):self.erase(fake)
        self.assertTrue(fake.erased);self.assertEqual(len(self.store.query('2866855')),4)
        self.assertEqual(self.store.actions('2866855')[0]['status'],'failed_or_unknown')
        self.assertEqual(sum(f.kind==0 and f.payload[:2]==b'\x01\x00' for f in fake.writes),1)
    def test_audit_disk_failure_after_erase_is_surfaced_as_unknown(self):
        fake=EraseFake()
        with patch.object(self.store,'finish_action',side_effect=OSError('audit disk full')):
            # _writable_store returns a new Store; inject the same failing audit into all instances.
            with patch.object(Store,'finish_action',side_effect=OSError('audit disk full')):
                with self.assertRaisesRegex(ConfigurationFailure,'审计未能完成'):self.erase(fake)
        self.assertTrue(fake.erased);self.assertEqual(len(self.store.query('2866855')),4)
    def test_pending_sync_prevents_erase_without_dropping_pending_material(self):
        transfer=self.store.begin_transfer('2866855',103,1)
        self.store.append_block(transfer,rows((30,)),[{'block':0,'records':1}])
        fake=EraseFake();fake.rows=[]
        with self.assertRaisesRegex(RuntimeError,'未确认'):self.erase(fake)
        self.assertFalse(fake.erased);self.assertEqual(self.info()['pending_blocks'],1)
    def test_readonly_device_count_does_not_sync_even_when_auto_sync_enabled(self):
        self.library.data_preferences('2866855',auto_sync=True)
        fake=EraseFake(remaining=17)
        async def resolve(_):return type('Device',(),{'address':'abc'})()
        with fake.patches(),patch('kestrel_drop.devices.resolve',resolve):
            result=asyncio.run(device_count(self.library,'2866855'))
        self.assertEqual(result['device_records'],17);self.assertTrue(result['read_only'])
        self.assertEqual(len(self.store.query('2866855')),3);self.assertFalse(fake.erased)
        self.assertEqual([int.from_bytes(f.payload[:2],'little') for f in fake.writes if f.kind==0],[2])
    def test_device_page_returns_with_fresh_cache_after_managing_settings(self):
        t=self.terminal();old={'serial':'2866855','settings':{'interval_seconds':300}}
        t.device_state=old;t.data_view={'serial':'2866855','info':self.info(),'state':{'serial':'2866855','settings':{'interval_seconds':600}}}
        with patch.object(t,'start') as start:t.handle('\x1b');start.assert_not_called()
        self.assertEqual(t.device_state['settings']['interval_seconds'],600)
    def test_preference_and_local_clear_work_through_real_worker_and_refresh_page(self):
        t=self.terminal();t.data_view={'serial':'2866855','info':self.info(),'state':None}
        def finish():
            import time
            deadline=time.monotonic()+10
            while t.job and time.monotonic()<deadline:
                t.job.thread.join(.05);t.poll()
            if t.job:t.job.cancel();self.fail('worker did not finish')
        t.handle('4');finish();self.assertTrue(t.data_view['info']['auto_sync'])
        t.handle('7');t.form['fields'][0][1]='2866855';t.submit();t.handle('\n');finish()
        self.assertEqual(t.data_view['info']['records'],0)
        self.assertEqual(t.data_view['info']['management_code'],'D3-043')
        self.assertTrue(t.data_view['info']['auto_sync']);self.assertIsNone(t.result_view)
        self.assertTrue(any('backup' in line for line in t.logs))
    def test_schema5_upgrade_failure_keeps_original_data_and_version(self):
        with self.store.transaction() as c:
            c.execute('DROP TABLE device_preferences');c.execute('PRAGMA user_version=5')
            c.execute('CREATE INDEX device_preferences ON devices(alias)')
        with self.assertRaises(sqlite3.OperationalError):Store(self.library.database_path,create=True)
        self.assertEqual(self.store.check()['schema_version'],5)
        self.assertEqual(len(self.store.query('2866855')),3)
    def test_tui_does_not_auto_retry_unknown_clear_result(self):
        t=self.terminal();t.result_view={'action':'erase-device','arguments':['erase-device','2866855','--execute'],
            'outcomes':[],'failures':[{'retryable':True}],'code':2}
        with patch.object(t,'start') as start:t.handle('r');start.assert_not_called()

    def test_worker_rejects_false_clear_success_without_backup_or_empty_count(self):
        from kestrel_drop.worker import validate_result
        for value in ({'serial':'2866855','status':'erased_device','device_records':0},
                      {'serial':'2866855','status':'erased_device','device_records':1,'backup':'/tmp/snapshot.sqlite'},
                      {'serial':'2866855','status':'erased_device','device_records':False,'backup':'/tmp/snapshot.sqlite'}):
            with self.assertRaises(ValueError):validate_result('erase-device',value)
    def test_device_count_button_uses_readonly_command(self):
        t=self.terminal();t.data_view={'serial':'2866855','info':self.info(),'state':None,'ble_id':'abc'}
        with patch.object(t,'start',return_value=True) as start:
            t.handle('c');start.assert_called_once_with('device-count',['device-count','2866855','--ble-id','abc'])

    def test_next_download_after_device_erase_keeps_lifetime_sequence(self):
        self.erase(EraseFake())
        fake=FakeBLE([record(35)],105)
        async def resolve(_):return object()
        with fake.patches(),patch.object(protocol,'resolve_ble_device',resolve),redirect_stdout(io.StringIO()):
            self.assertEqual(asyncio.run(self.library.download('2866855',execute=True)),0)
        self.assertEqual(self.store.query('2866855')[-1]['device_sequence'],105)
        self.assertEqual(len(self.store.query('2866855')),5)
        self.assertEqual(self.store.device('2866855')['generation'],1)
    def test_default_off_does_not_download_on_connection(self):
        fake=FakeBLE([],103)
        async def resolve(_):return type('Device',(),{'address':'abc'})()
        with fake.patches(),patch('kestrel_drop.devices.resolve',resolve),patch('kestrel_drop.control.inspect',new_callable=AsyncMock,return_value={'settings':{}}),patch.object(self.library,'_sync_client',new_callable=AsyncMock) as sync:
            result=asyncio.run(self.library.device_operation('2866855'))
            sync.assert_not_called();self.assertFalse(result['auto_synced'])
    def test_erase_does_not_accept_live_database_in_place_of_backup(self):
        fake=EraseFake()
        with patch.object(self.library,'backup',return_value=self.library.database_path):
            with self.assertRaisesRegex(ValueError,'独立备份'):self.erase(fake)
        self.assertFalse(fake.erased)

    def test_scan_registration_sync_failure_does_not_downgrade_verified_identity(self):
        self.library.data_preferences('2866855',auto_sync=True)
        from kestrel_drop import devices
        identity={'serial':'2866855','model':'Kestrel DROP 3','ble_id':'abc','identity_verified':True,**devices.capabilities('D3')}
        snapshot={'abc':(type('Device',(),{'address':'abc'})(),None)}
        with patch.object(devices,'scan_snapshot',new_callable=AsyncMock,return_value=snapshot),patch.object(devices,'candidates',return_value=[{'ble_id':'abc'}]),patch.object(devices,'identify',new_callable=AsyncMock,return_value=identity),patch.object(self.library,'download',new_callable=AsyncMock,side_effect=RuntimeError('injected')):
            result=asyncio.run(manager.discover(register_to=self.library))
        self.assertTrue(result[0]['identity_verified']);self.assertTrue(result[0]['registered'])
        self.assertIn('auto_sync_error',result[0]);self.assertNotIn('identity_error',result[0])
