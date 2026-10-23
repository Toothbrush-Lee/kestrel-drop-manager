"""Mixed fleets: identification, connection selection, batch isolation, UI state."""
import asyncio
from contextlib import redirect_stdout, redirect_stderr
import io
import json
from pathlib import Path
import sys
import tempfile
from types import ModuleType, SimpleNamespace as Item
import unittest
from unittest.mock import patch, AsyncMock

from kestrel_drop import devices, manager, protocol
from kestrel_drop.database import Store
from kestrel_drop.errors import BluetoothFailure, DeviceMismatch
from kestrel_drop.tui import Terminal, merge_devices
from test_database import fixture, rows
from test_iteration import FakeScreen
from test_manager import FakeBLE, record


class Fleet:
    def __init__(self, specs):
        self.specs, self.reads, self.connections, self.writes = specs, [], [], []
        self.scans = 0
        self.module = ModuleType('bleak')
        self.errors = ModuleType('bleak.exc')
        self.errors.BleakError = type('BleakError', (Exception,), {})
        owner = self
        async def discover(**kwargs):
            owner.scans += 1
            return {address: (Item(address=address,name=s.get('name','')), Item(local_name=s.get('name',''),rssi=s.get('rssi',-50),service_uuids=s.get('services',[]))) for address,s in specs.items()}
        self.module.BleakScanner=Item(discover=discover)
        class Client:
            def __init__(self, device, **kwargs): self.address=device.address
            async def __aenter__(self):
                owner.connections.append(self.address)
                if specs[self.address].get('unavailable'): raise owner.errors.BleakError('busy')
                return self
            async def __aexit__(self,*args): pass
            async def read_gatt_char(self,uuid):
                owner.reads.append((self.address,uuid))
                key='model' if uuid==devices.MODEL_UUID else 'serial'
                value=specs[self.address].get(key)
                if value is None: raise owner.errors.BleakError('characteristic unavailable')
                return value if isinstance(value,bytes) else value.encode()
            async def write_gatt_char(self,*args,**kwargs):
                owner.writes.append((args,kwargs)); raise AssertionError('identity must never write')
        self.module.BleakClient=Client

    def patches(self): return patch.dict(sys.modules,{'bleak':self.module,'bleak.exc':self.errors})


class DiscoveryTests(unittest.TestCase):
    def scan(self,specs):
        fleet=Fleet(specs)
        with fleet.patches(): result=asyncio.run(manager.discover(1))
        return fleet,result

    def test_all_model_names_and_serials_are_parsed(self):
        names=['D1 - 000123','D2-456','D3_789','DROP 1: 12','Kestrel DROP D2 – 34','D3 56']
        _,found=self.scan({str(i):{'name':name} for i,name in enumerate(names)})
        self.assertEqual([r['device_type'] for r in found],['D1','D2','D3','D1','D2','D3'])
        self.assertEqual([r['serial'] for r in found],['000123','456','789','12','34','56'])
        self.assertFalse(any(r['identity_verified'] for r in found))

    def test_model_only_and_unnamed_service_candidates_remain_distinct(self):
        _,found=self.scan({'a':{'name':'D1'},'b':{'name':'D2'},'c':{'name':'','services':[devices.DROP_SERVICE]},'d':{'name':'Headphones'}})
        self.assertEqual(len(found),3)
        self.assertEqual([r['device_type'] for r in found],['D1','D2','未知'])
        self.assertTrue(all(r['serial'] is None for r in found))

    def test_d10_and_unverified_submodels_are_not_classified_as_d1_or_d3(self):
        for text in ('D10','Kestrel DROP 30','Kestrel DROP 3 Fire','X D3'):
            self.assertIsNone(devices.model_type(text))

    def test_duplicate_broadcast_serials_are_flagged(self):
        _,found=self.scan({'a':{'name':'D1 - 123'},'b':{'name':'D1 - 123'}})
        self.assertTrue(all(r['identity_conflict'] for r in found))

    def test_custom_name_preserves_model_hint_without_fabricated_serial(self):
        _,found=self.scan({'a':{'name':'D1 field-unit'},'b':{'name':'D2 sensor'},'c':{'name':'D10 speaker'}})
        self.assertEqual([r['device_type'] for r in found],['D1','D2'])
        self.assertTrue(all(r['serial'] is None for r in found))

    def test_negative_scan_timeout_never_touches_ble(self):
        with self.assertRaises(ValueError): asyncio.run(manager.discover(-1))

    def test_resolution_supports_d1_d2_and_changed_address(self):
        for model in ('D1','D2','D3'):
            fleet=Fleet({'new':{'name':model+' - 123'}})
            args=Item(serial='123',ble_id='old',scan_timeout=1)
            with fleet.patches(): chosen=asyncio.run(devices.resolve(args))
            self.assertEqual(chosen.address,'new')

    def test_ambiguous_name_is_refused_but_explicit_address_can_be_identified(self):
        fleet=Fleet({'a':{'name':'D1 - 123'},'b':{'name':'D1 - 123'}})
        with fleet.patches():
            with self.assertRaises(DeviceMismatch): asyncio.run(devices.resolve(Item(serial='123',ble_id=None,scan_timeout=1)))
            self.assertEqual(asyncio.run(devices.resolve(Item(serial='123',ble_id='b',scan_timeout=1))).address,'b')

    def test_shared_discovery_does_not_rescan_for_each_device(self):
        fleet=Fleet({str(i):{'name':f'D3 - {i}'} for i in range(100)})
        with fleet.patches():
            snapshot=asyncio.run(devices.scan_snapshot(1))
            for i in range(100):
                chosen=asyncio.run(devices.resolve(Item(serial=str(i),ble_id=None,scan_timeout=1,discovery=snapshot)))
                self.assertEqual(chosen.address,str(i))
        self.assertEqual(fleet.scans,1)

    def test_device_arriving_after_snapshot_gets_fresh_lookup(self):
        fleet=Fleet({'new':{'name':'D1 - 123'}})
        with fleet.patches():
            chosen=asyncio.run(devices.resolve(Item(serial='123',ble_id=None,scan_timeout=1,discovery={})))
        self.assertEqual(chosen.address,'new'); self.assertEqual(fleet.scans,1)


class IdentityTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)
        self.library=manager.Library(self.root/'library')

    def test_standard_gatt_reads_get_d1_d2_serials_without_name_hint(self):
        for number in (1,2,3):
            fleet=Fleet({'a':{'name':'custom','services':[devices.DROP_SERVICE],'model':f'Kestrel DROP {number}','serial':b'001234\x00\x00'}})
            with fleet.patches(): identity=asyncio.run(devices.identify('a',timeout=1))
            self.assertEqual((identity['serial'],identity['device_type'],identity['identity_verified']),('001234',f'D{number}',True))
            self.assertEqual(identity['history_supported'],True)
            self.assertEqual(fleet.writes,[])
            self.assertEqual({u for _,u in fleet.reads},{devices.MODEL_UUID,devices.SERIAL_UUID})

    def test_registration_survives_restart_without_creating_fake_observations(self):
        for number in (1,2,3):
            fleet=Fleet({'a':{'name':f'D{number}','model':f'Kestrel DROP {number}','serial':str(number)}})
            with fleet.patches(): asyncio.run(self.library.register(ble_id='a',timeout=1))
        reopened=manager.Library(self.library.root)
        self.assertEqual(reopened.targets(),['1','2','3'])
        self.assertEqual([r['device_type'] for r in reopened.status()],['D1','D2','D3'])
        self.assertEqual(reopened.database_info()['records'],0)

    def test_missing_serial_preserves_model_but_never_guesses_address_as_identity(self):
        fleet=Fleet({'a':{'name':'D1 - 123','model':'Kestrel DROP 1'}})
        with fleet.patches():
            identity=asyncio.run(devices.identify('a',timeout=1))
            with self.assertRaises(DeviceMismatch): asyncio.run(self.library.register(ble_id='a',timeout=1))
        self.assertEqual(identity['device_type'],'D1')
        self.assertIsNone(identity['serial'])
        self.assertFalse(self.library.root.exists())

    def test_invalid_utf8_or_embedded_null_serial_is_never_registered(self):
        for serial in (b'123\x00456',b'\xff',b'','１２３'.encode(),b'-123'):
            fleet=Fleet({'a':{'name':'D1','model':'Kestrel DROP 1','serial':serial}})
            with fleet.patches(): identity=asyncio.run(devices.identify('a',timeout=1))
            self.assertFalse(identity['identity_verified'])
            self.assertIn('serial',identity['identity_errors'])

    def test_batch_registration_uses_single_scan_and_keeps_partial_success(self):
        fleet=Fleet({'a':{'name':'D1','model':'DROP 1','serial':'1'},'b':{'name':'D2','unavailable':True},'c':{'name':'D3','model':'DROP 3','serial':'3'}})
        with fleet.patches():
            found=asyncio.run(manager.discover(1,register_to=self.library))
        self.assertEqual(fleet.scans,1)
        self.assertEqual(self.library.targets(),['1','3'])
        self.assertTrue(found[0]['registered']); self.assertTrue(found[2]['registered'])
        self.assertIn('identity_error',found[1])
        self.assertEqual(fleet.writes,[])

    def test_registration_model_conflict_does_not_replace_existing_identity(self):
        store=self.library._writable_store()
        store.register_device('123','Kestrel DROP 1')
        with self.assertRaises(RuntimeError): store.register_device('123','Kestrel DROP 2')
        self.assertEqual(store.device('123')['model'],'Kestrel DROP 1')

    def test_deep_scan_preserves_other_results_when_one_device_fails(self):
        fleet=Fleet({'a':{'name':'D1','model':'Kestrel DROP 1','serial':'1'},'b':{'name':'D2','unavailable':True},'c':{'name':'D3','model':'Kestrel DROP 3','serial':'3'}})
        with fleet.patches(): result=asyncio.run(manager.discover(1,identify=True))
        self.assertEqual([r.get('serial') for r in result],['1',None,'3'])
        self.assertIn('identity_error',result[1])
        self.assertEqual(fleet.writes,[])

    def test_wrong_serial_identification_refuses_registration(self):
        fleet=Fleet({'a':{'name':'D1 - 123','model':'Kestrel DROP 1','serial':'999'}})
        with fleet.patches():
            with self.assertRaises(DeviceMismatch): asyncio.run(self.library.register(serial='123',timeout=1))
        self.assertFalse(self.library.root.exists())

    def test_d1_registration_does_not_allow_d3_csv_as_d2_import(self):
        store=self.library._writable_store(); store.register_device('1','DROP 1')
        store.ensure_device('1','DROP 1')
        with self.assertRaises(RuntimeError): store.import_rows('2','DROP 2',rows(),103,{})
        self.assertEqual(store.summary()['records'],0)


class BatchTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name)

    def run_cli(self,arguments):
        output=io.StringIO()
        with redirect_stdout(output),redirect_stderr(io.StringIO()):
            code=manager.main(['--data-dir',str(self.root/'library'),'--events-json',*arguments])
        return code,[json.loads(line) for line in output.getvalue().splitlines()]

    def test_hundred_device_batch_scans_once_continues_after_failure_and_preserves_mapping(self):
        calls=[]
        async def download(library,serial,**kwargs):
            calls.append((serial,kwargs['ble_id'],kwargs['discovery']))
            if serial=='50': raise BluetoothFailure('one disconnected')
            library.download_results[serial]={'serial':serial,'status':'up_to_date','added_records':0,'total_records':1,'pending_records':0,'code':0}
            return 0
        snapshot={'sentinel':'scan'}
        with patch.object(manager.Library,'download',download),patch.object(devices,'scan_snapshot',new=AsyncMock(return_value=snapshot)) as scan:
            code,stream=self.run_cli(['download','--serials',*[str(i) for i in range(100)],'--ble-ids',*[f'addr{i}' for i in range(100)],'--execute'])
        self.assertEqual(code,2); self.assertEqual(len(calls),100); scan.assert_awaited_once()
        self.assertTrue(all(serial==address[4:] and source is snapshot for serial,address,source in calls))
        result=next(e['data'] for e in stream if e['event']=='result')
        self.assertEqual(len(result['downloads']),100)
        self.assertEqual([r['serial'] for r in result['incomplete']],['50'])

    def test_duplicate_serials_processed_once_and_conflicting_locators_rejected(self):
        calls=[]
        async def download(library,serial,**kwargs): calls.append(serial); return 0
        with patch.object(manager.Library,'download',download):
            code,_=self.run_cli(['download','--serials','1','1','2'])
            self.assertEqual((code,calls),(0,['1','2']))
            code,_=self.run_cli(['download','--serials','1','1','--ble-ids','a','b'])
            self.assertEqual(code,2)
        self.assertEqual(calls,['1','2'])

    def test_misaligned_or_shared_addresses_rejected_before_ble(self):
        for arguments in (['--serials','1','2','--ble-ids','a'],['--serials','1','2','--ble-ids','a','a'],['--serial','1','--ble-ids','a']):
            with patch.object(manager.Library,'download') as call:
                code,_=self.run_cli(['download',*arguments,'--execute'])
                self.assertEqual(code,2); call.assert_not_called()

    def test_all_includes_registered_and_initial_interrupted_devices(self):
        library=manager.Library(self.root/'library'); store=library._writable_store()
        store.register_device('1','DROP 3'); store.register_device('2','DROP 1')
        transfer=store.begin_transfer('1',0,1)
        store.append_block(transfer,rows((20,)),[{'block':0,'records':1}])
        calls=[]
        async def download(library,serial,**kwargs): calls.append(serial); return 0
        with patch.object(manager.Library,'download',download),patch.object(devices,'scan_snapshot',new=AsyncMock(return_value={})):
            code,_=self.run_cli(['download','--all','--execute'])
        self.assertEqual((code,calls),(0,['1','2']))

    def test_failed_device_pending_does_not_block_other_status_or_download(self):
        library=manager.Library(self.root/'library')
        library.import_archive(fixture(self.root,serial='1'))
        library.import_archive(fixture(self.root,'other.csv',serial='2'))
        store=library.store
        tid=store.begin_transfer('1',103,1); store.append_block(tid,rows((30,)),[{'block':0,'records':1}])
        with store.transaction() as c: c.execute("UPDATE pending_blocks SET sha256='bad'")
        status=library.status()
        self.assertIn('error',status[0]); self.assertNotIn('error',status[1])
        fake=FakeBLE([record(30)],104,serial='2')
        async def resolve(_): return Item(address='two')
        with fake.patches(),patch.object(protocol,'resolve_ble_device',resolve),redirect_stdout(io.StringIO()):
            asyncio.run(library.download('2',execute=True))
        self.assertEqual(len(library.records('2')),4)
        self.assertEqual(len(library.records('1')),3)
        with self.assertRaises(RuntimeError): library.database_info()

    def test_database_streams_do_not_mix_equal_cursors_and_pending(self):
        library=manager.Library(self.root/'library')
        for serial in ('1','2'): library.import_archive(fixture(self.root,serial+'.csv',serial=serial))
        for serial,minute in (('1',30),('2',35)):
            fake=FakeBLE([record(minute)],104,serial=serial)
            async def resolve(_): return Item(address=serial)
            with fake.patches(),patch.object(protocol,'resolve_ble_device',resolve),redirect_stdout(io.StringIO()):
                asyncio.run(library.download(serial,execute=True))
        self.assertEqual(library.records('1')[-1]['record_hex'],record(30).hex())
        self.assertEqual(library.records('2')[-1]['record_hex'],record(35).hex())
        self.assertEqual(library.database_info()['records'],8)

    def test_partial_deep_scan_emits_results_and_nonzero_exit(self):
        fleet=Fleet({'a':{'name':'D1','model':'DROP 1','serial':'1'},'b':{'name':'D2','unavailable':True}})
        with fleet.patches(): code,stream=self.run_cli(['scan','--timeout','1','--register'])
        self.assertEqual(code,2)
        result=next(e['data'] for e in stream if e['event']=='result')
        self.assertEqual(len(result),2)
        self.assertEqual(result[0]['serial'],'1')
        self.assertTrue(any(e['event']=='error' for e in stream))
        self.assertEqual(manager.Library(self.root/'library').targets(),['1'])

    def test_real_protocol_simulation_collects_two_custom_named_devices_into_separate_histories(self):
        fleet=Fleet({'a':{'name':'field-A','services':[devices.DROP_SERVICE]},'b':{'name':'field-B','services':[devices.DROP_SERVICE]}})
        fakes={'a':FakeBLE([record(20)],101,serial='1'),'b':FakeBLE([record(25)],201,serial='2')}
        fleet.module.BleakClient=lambda device,**kwargs: fakes[device.address].module.BleakClient(device,**kwargs)
        with fleet.patches():
            code,stream=self.run_cli(['download','--serials','1','2','--ble-ids','a','b','--execute','--timeout','1'])
        self.assertEqual(code,0); self.assertEqual(fleet.scans,1)
        library=manager.Library(self.root/'library')
        self.assertEqual(library.records('1')[0]['absolute_cursor'],100)
        self.assertEqual(library.records('2')[0]['absolute_cursor'],200)
        self.assertEqual(library.database_info()['records'],2)
        self.assertEqual([e['serial'] for e in stream if e['event']=='completed'],['1','2'])

    def test_unknown_model_in_mixed_batch_does_not_prevent_d3_download(self):
        fleet=Fleet({'a':{'name':'D1 - 1'},'b':{'name':'D3 - 2'}})
        d1=FakeBLE([record(20)],101,serial='1')
        d3=FakeBLE([record(25)],201,serial='2')
        base=d1.module.BleakClient
        class D1Client(base):
            async def read_gatt_char(self,uuid): return b'1' if uuid==devices.SERIAL_UUID else b'Kestrel DROP 30'
        fleet.module.BleakClient=lambda device,**kwargs: D1Client(device,**kwargs) if device.address=='a' else d3.module.BleakClient(device,**kwargs)
        with fleet.patches(): code,stream=self.run_cli(['download','--serials','1','2','--execute','--timeout','1'])
        self.assertEqual(code,2)
        self.assertEqual(d1.writes,[])
        self.assertEqual(manager.Library(self.root/'library').database_info()['records'],1)
        error=next(e for e in stream if e['event']=='error')
        self.assertEqual((error['serial'],error['code']),('1','device_mismatch'))

    def test_d1_d2_d3_mixed_batch_keeps_raw_width_nulls_and_cursors_separate(self):
        fleet=Fleet({address:{'name':model+' - '+serial}
            for address,model,serial in [('a','D1','1'),('b','D2','2'),('c','D3','3')]})
        from test_manager import RAW
        clients={address:FakeBLE([RAW[:width]],cursor,serial=serial,model=model)
            for address,model,serial,width,cursor in [('a','D1','1',9,101),('b','D2','2',16,201),('c','D3','3',21,301)]}
        fleet.module.BleakClient=lambda device,**kwargs: clients[device.address].module.BleakClient(device,**kwargs)
        with fleet.patches():
            code,stream=self.run_cli(['download','--serials','1','2','3','--ble-ids','a','b','c','--execute','--timeout','1'])
        self.assertEqual(code,0);self.assertEqual(fleet.scans,1)
        library=manager.Library(self.root/'library');library.store.check()
        for serial,width,cursor in [('1',9,101),('2',16,201),('3',21,301)]:
            row=library.records(serial)[0]
            self.assertEqual(row['record_hex'],RAW[:width].hex())
            self.assertEqual(row['absolute_cursor'],cursor-1)
            self.assertEqual(row['serial'],serial)
            self.assertEqual(row['station_pressure_hpa'] is None,serial!='3')
        self.assertEqual([e['serial'] for e in stream if e['event']=='completed'],['1','2','3'])

    def test_cancelling_batch_keeps_first_pending_and_does_not_start_second(self):
        fleet=Fleet({'a':{'name':'D3 - 1'},'b':{'name':'D3 - 2'}})
        first=FakeBLE([record(20)],101,serial='1',interrupt=True)
        second=FakeBLE([record(25)],201,serial='2')
        fleet.module.BleakClient=lambda device,**kwargs: (first if device.address=='a' else second).module.BleakClient(device,**kwargs)
        with fleet.patches(): code,stream=self.run_cli(['download','--serials','1','2','--execute','--timeout','1'])
        self.assertEqual(code,130)
        self.assertEqual(second.writes,[])
        self.assertEqual(manager.Library(self.root/'library').status()[0]['pending_records'],1)
        self.assertEqual(next(e for e in stream if e['event']=='error')['code'],'cancelled')



class MultiDeviceUI(unittest.TestCase):
    def terminal(self,items):
        terminal=Terminal(FakeScreen(),manager.Library('/private/tmp/kestrel-ui-model-only'))
        terminal.devices=items
        terminal.screen.refresh=lambda: None
        return terminal

    def test_type_column_and_unverified_serial_prompt_are_visible(self):
        terminal=self.terminal({'a':{'serial':None,'device_type':'D1','ble_id':'a'},'2':{'serial':'2','device_type':'D2'},'3':{'serial':'3','device_type':'D3'}})
        terminal.draw()
        text='\n'.join(terminal.screen.lines)
        self.assertIn('型号',text)
        for kind in ('D1','D2','D3'): self.assertIn(kind,text)
        self.assertIn('待核验',text)
        self.assertIn('历史采集',text)

    def test_d1_preview_renders_absent_measurements(self):
        terminal=self.terminal({})
        terminal.preview=[{'timestamp_device_local':'2026-10-23 00:00:00',
            'temperature_c':21.66,'relative_humidity_pct':None,'station_pressure_hpa':None}]
        terminal.draw()
        text=' '.join(terminal.screen.lines)
        self.assertIn('21.66',text);self.assertIn('--',text);self.assertNotIn('None',text)

    def test_unknown_serial_enter_reads_and_registers_instead_of_guessing(self):
        terminal=self.terminal({'ble:a':{'ble_id':'a','device_type':'D1','serial':None}})
        with patch.object(terminal,'start') as start:
            terminal.handle('\n'); start.assert_called_once_with('register',['register','--ble-id','a'])

    def test_marked_selection_and_locators_are_forwarded_to_batch(self):
        terminal=self.terminal({'1':{'serial':'1','ble_id':'a'},'2':{'serial':'2','ble_id':'b'}})
        terminal.handle(' ')
        with patch.object(terminal,'start') as start:
            terminal.handle('a')
            start.assert_called_once_with('download',['download','--serials','1','--ble-ids','a','--execute'])

    def test_unmarked_batch_includes_new_scanned_devices(self):
        terminal=self.terminal({'1':{'serial':'1','nearby':True},'2':{'serial':'2','ble_id':'b','nearby':True}})
        with patch.object(terminal,'start') as start:
            terminal.handle('a')
            start.assert_called_once_with('download',['download','--serials','1','2','--ble-ids','-','b','--execute'])

    def test_default_batch_excludes_offline_and_unsupported_devices(self):
        terminal=self.terminal({'1':{'serial':'1','nearby':True,'device_type':'D1','history_supported':False},
            '2':{'serial':'2','nearby':True,'device_type':'D2','history_supported':False},
            '3':{'serial':'3','nearby':False,'device_type':'D3','history_supported':True},
            '4':{'serial':'4','nearby':True,'device_type':'D3','history_supported':True,'ble_id':'four'}})
        with patch.object(terminal,'start') as start:
            terminal.handle('a')
            start.assert_called_once_with('download',['download','--serials','4','--ble-ids','four','--execute'])
        self.assertIn('历史待适配',''.join(terminal.logs))
        self.assertIn('本次未扫描到',''.join(terminal.logs))

    def test_explicitly_marked_offline_device_can_still_be_retried(self):
        terminal=self.terminal({'1':{'serial':'1','nearby':False,'history_supported':True}})
        terminal.marked={'1'}
        with patch.object(terminal,'start') as start:
            terminal.handle('a')
            start.assert_called_once_with('download',['download','--serials','1','--ble-ids','-','--execute'])

    def test_marking_unsupported_model_does_not_launch_a_failing_batch(self):
        terminal=self.terminal({'1':{'serial':'1','nearby':True,'device_type':'D1','history_supported':False}})
        terminal.marked={'1'}
        with patch.object(terminal,'start') as start:
            terminal.handle('a'); start.assert_not_called()
        self.assertIn('没有可采集目标',terminal.message)

    def test_unsupported_single_download_shows_explanation_without_ble_write(self):
        terminal=self.terminal({'1':{'serial':'1','device_type':'D1','history_supported':False}})
        with patch.object(terminal,'start') as start:
            terminal.handle('\n'); start.assert_not_called()
        self.assertIn('尚未适配',terminal.message)

    def test_unknown_candidate_disappears_after_identity_is_verified(self):
        original={'ble:a':{'serial':None,'ble_id':'a'}}
        merged=merge_devices(original,[{'serial':'123','ble_id':'a','device_type':'D1','identity_verified':True}])
        self.assertEqual(list(merged),['123'])

    def test_reused_address_does_not_transfer_history_counts_to_new_serial(self):
        original={'1':{'serial':'1','ble_id':'a','database_records':99,'identity_verified':True}}
        merged=merge_devices(original,[{'serial':'2','ble_id':'a','identity_verified':True}])
        self.assertEqual(merged['1']['database_records'],99)
        self.assertIsNone(merged['1']['ble_id'])
        self.assertNotIn('database_records',merged['2'])

    def test_bad_name_hint_is_replaced_by_gatt_identity_without_ghost_row(self):
        original={'1':{'serial':'1','ble_id':'a','identity_verified':False}}
        merged=merge_devices(original,[{'serial':'2','ble_id':'a','identity_verified':True}])
        self.assertEqual(list(merged),['2'])

    def test_scan_does_not_downgrade_verified_model(self):
        original={'1':{'serial':'1','device_type':'D3','model':'DROP 3','identity_verified':True,'ble_id':'old','database_records':99}}
        merged=merge_devices(original,[{'serial':'1','device_type':'D1','model':'DROP 1','identity_verified':False,'ble_id':'new'}],scan=True)
        self.assertEqual(merged['1']['device_type'],'D3')
        self.assertEqual(merged['1']['database_records'],99)
        self.assertTrue(merged['1']['identity_conflict'])

    def test_duplicate_serials_do_not_silently_overwrite_addresses(self):
        merged=merge_devices({},[{'serial':'1','ble_id':'a'},{'serial':'1','ble_id':'b'}],scan=True)
        self.assertEqual(len(merged),2)
        self.assertTrue(all(item['identity_conflict'] for item in merged.values()))

    def test_second_scan_clears_stale_unknowns_and_duplicate_rows(self):
        original={'ble:a':{'serial':None,'ble_id':'a'},'1':{'serial':'1','ble_id':'b'},'1@c':{'serial':'1','ble_id':'c'}}
        merged=merge_devices(original,[{'serial':'1','ble_id':'new'}],scan=True)
        self.assertEqual(list(merged),['1'])
        self.assertEqual(merged['1']['ble_id'],'new')
