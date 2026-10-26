"""Official DROP interfaces, migration, bounded concurrency and write uncertainty."""
import asyncio
from contextlib import redirect_stdout, redirect_stderr
import io
import json
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

from kestrel_drop import control, protocol, manager
from kestrel_drop.database import Store
from kestrel_drop.errors import ConfigurationFailure
from kestrel_drop.layouts import profile
from test_manager import FakeBLE, RAW
from test_devices import Fleet


class FakeControl:
    def __init__(self, *, reject=False, wrong_serial=False, fail_after=0, serial='123'):
        self.writes=[];self.reject=reject;self.fail_after=fail_after;self.audit_check=None
        raw=(300).to_bytes(3,'little')+bytes([1,30,0,1])+b'\0\0'+bytes(range(11))
        self.values={control.SETTINGS_UUID:raw,control.NAME_UUID:b'D2 - 123'.ljust(20,b'\0'),
            control.TIME_UUID:control.encode_time('2026-10-26 12:00:00'),
            control.BATTERY_UUID:b'\x5a',control.FIRMWARE_UUID:b'1.56',control.HARDWARE_UUID:b'1.0'}
        for _,code,width,signed,divisor in control.MEASUREMENTS:
            self.values[control.uuid(code)]=b'\x07'+(36000 if code==3 else 100).to_bytes(width,'little',signed=signed)
        self.mass_writes=[]
        owner=self
        class Client:
            def __init__(self,*args,**kwargs): self.parser=protocol.FrameStream()
            async def __aenter__(self):return self
            async def __aexit__(self,*args): pass
            async def read_gatt_char(self,uuid):
                if uuid==protocol.SERIAL_UUID:return b'999' if wrong_serial else serial.encode()
                if uuid==protocol.MODEL_UUID:return b'Kestrel DROP 2'
                return owner.values[uuid]
            async def start_notify(self,uuid,callback):self.callback=callback
            async def stop_notify(self,uuid):pass
            async def write_gatt_char(self,uuid,payload,response):
                if uuid==protocol.DOWN_UUID:
                    for frame in self.parser.feed(payload):
                        owner.mass_writes.append(frame)
                        if frame.kind==0:
                            command=int.from_bytes(frame.payload[:2],'little')
                            self.callback(None,protocol.encode_frame(4,frame.payload[:2]))
                            if command==15:
                                value=control.decode_link_time(frame.payload[2:])
                                owner.values[control.TIME_UUID]=control.encode_time(value)
                            if command==14:
                                value=control.decode_time(owner.values[control.TIME_UUID])
                                self.callback(None,protocol.encode_frame(3,control.encode_link_time(value)+b'\0'))
                    return
                if owner.audit_check:owner.audit_check()
                owner.writes.append((uuid,bytes(payload)))
                if not owner.reject: owner.values[uuid]=bytes(payload)
                if owner.fail_after and len(owner.writes)>=owner.fail_after: raise OSError('lost response after write')
        self.client=Client()
        self.module=ModuleType('bleak');self.module.BleakClient=Client
        self.exc=ModuleType('bleak.exc');self.exc.BleakError=type('BleakError',(Exception,),{})

    def patches(self):return patch.dict('sys.modules',{'bleak':self.module,'bleak.exc':self.exc})


class ControlTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.enterContext(FakeControl().patches())

    async def test_round_trip_settings_preserve_unrequested_and_reserved_bytes(self):
        fake=FakeControl()
        before=await control.inspect(fake.client)
        original=bytes.fromhex(before['settings_raw_hex'])
        changes={'interval_seconds':600,'wrap':False,'update_seconds':20,'reference_altitude_m':-10,'name':'Field-A','time':'2026-10-26 12:15:00'}
        after=await control.apply(fake.client,changes,before)
        self.assertEqual(after['settings']['interval_seconds'],600)
        self.assertFalse(after['settings']['wrap'])
        self.assertEqual(after['settings']['reference_altitude_m'],-10)
        payload=fake.writes[0][1]
        self.assertEqual(payload[5:7],original[5:7]);self.assertEqual(payload[9:],original[9:])
        self.assertEqual(after['name'],'Field-A');self.assertEqual(after['device_time'],changes['time'])

    async def test_live_status_and_official_gatt_scaling(self):
        fake=FakeControl()
        fake.values[control.uuid(1)]=b'\x07'+(-1234).to_bytes(2,'little',signed=True)
        fake.values[control.uuid(2)]=b'\x03\xff\xff'
        with fake.patches(): result=await control.live(fake.client)
        self.assertEqual(result['measurements']['temperature_c'],-12.34)
        self.assertEqual(result['measurements']['heat_index_c'],360)
        self.assertIsNone(result['measurements']['relative_humidity_pct'])
        self.assertFalse(fake.writes)

    async def test_unsupported_setting_and_readback_mismatch_do_not_report_success(self):
        fake=FakeControl(reject=True)
        before=await control.inspect(fake.client)
        with self.assertRaisesRegex(RuntimeError,'回读不一致'):
            await control.apply(fake.client,{'interval_seconds':600},before)
        self.assertEqual(len(fake.writes),1)
        raw=bytearray(bytes.fromhex(before['settings_raw_hex']));raw[:3]=b'\xff'*3
        before['settings_raw_hex']=bytes(raw).hex();before['settings']=control.decode_settings(raw)
        with self.assertRaisesRegex(ValueError,'不支持'): await control.apply(fake.client,{'interval_seconds':600},before)
        self.assertEqual(len(fake.writes),1)

    def test_invalid_values_are_rejected_before_bluetooth(self):
        for changes in ({},{'interval_seconds':900},{'interval_seconds':0},{'interval_seconds':True},
                        {'wrap':'on'},{'update_seconds':1},{'name':'中文'},{'name':'x'*21},
                        {'name':'a\nb'},{'time':'2026-10-26T12:00:00Z'},{'time':'2026-02-30 00:00:00'}):
            with self.subTest(changes=changes):
                with self.assertRaises(ValueError):control.validate_changes(changes)

    async def test_library_identity_dry_run_audit_and_ambiguous_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            library=manager.Library(Path(temp)/'archive')
            result=await library.device_operation('123',changes={'interval_seconds':600})
            self.assertEqual(result['status'],'dry_run');self.assertFalse(library.root.exists())
            async def resolve(_):return SimpleNamespace(address='a')
            fake=FakeControl()
            def audit():
                with library.store.connect() as c:
                    self.assertEqual(c.execute('SELECT status FROM device_actions ORDER BY id DESC LIMIT 1').fetchone()[0],'started')
            fake.audit_check=audit
            with fake.patches(),patch.object(manager.devices,'resolve',resolve):
                result=await library.device_operation('123',changes={'interval_seconds':600},execute=True)
            self.assertEqual(result['status'],'verified')
            with library.store.connect() as c:self.assertEqual(c.execute('SELECT status FROM device_actions').fetchone()[0],'verified')
            fake=FakeControl(fail_after=1)
            with fake.patches(),patch.object(manager.devices,'resolve',resolve):
                with self.assertRaises(ConfigurationFailure):
                    await library.device_operation('123',changes={'interval_seconds':600},execute=True)
            with library.store.connect() as c:
                self.assertEqual(c.execute('SELECT status FROM device_actions ORDER BY id DESC LIMIT 1').fetchone()[0],'failed_or_unknown')
            audit=library.store.actions('123')
            self.assertEqual([a['status'] for a in audit],['failed_or_unknown','verified'])
            self.assertIsNone(audit[0]['after'])
            backup=Path(temp)/'backup.sqlite';library.backup(backup)
            self.assertEqual(Store(backup).actions('123'),audit)
            fake=FakeControl(wrong_serial=True)
            with fake.patches(),patch.object(manager.devices,'resolve',resolve):
                with self.assertRaisesRegex(RuntimeError,'Serial mismatch'):
                    await library.device_operation('123',changes={'interval_seconds':600},execute=True)
            self.assertFalse(fake.writes)


class OfficialWireTests(unittest.IsolatedAsyncioTestCase):
    async def test_escaped_64bit_cursor_is_chunked_and_zero_padded(self):
        chunks=[]
        class Client:
            async def write_gatt_char(self,uuid,raw,response):chunks.append(raw)
        session=protocol.BleSession(Client(),model='D1')
        cursor=int.from_bytes(b'\x7e\x7d\x7e\x7d\x01\0\0\0','little')
        session.queue.put_nowait(protocol.Frame(3,(7).to_bytes(4,'little'),b''))
        self.assertEqual(await session.query_count(cursor),7)
        self.assertTrue(all(len(c)==20 for c in chunks))
        self.assertGreaterEqual(len(chunks),3)
        stream=protocol.FrameStream();frames=[]
        for chunk in chunks:frames.extend(stream.feed(chunk))
        self.assertFalse(stream.errors)
        self.assertEqual(int.from_bytes(frames[0].payload[2:],'little'),cursor)
        self.assertEqual(frames[-1].payload,b'\xff\xff')

    async def test_native_completion_uses_all_64_bits(self):
        class Client:
            async def write_gatt_char(self,*args,**kwargs):pass
        session=protocol.BleSession(Client(),model='D1')
        cursor=(1<<32)+40
        for f in (protocol.Frame(1,profile('D1').schema,b''),protocol.Frame(3,b'\0\0'+RAW[:9],b''),
                  protocol.Frame(0,b'\x12\0'+(cursor+1).to_bytes(8,'little'),b'')):
            session.queue.put_nowait(f)
        _,_,final=await session.download_from_cursor(cursor,1,20000)
        self.assertEqual(final,cursor+1)

    async def test_nack_has_explicit_command_and_reason(self):
        session=protocol.BleSession(None,model='D1')
        session.queue.put_nowait(protocol.Frame(5,b'\x1a\0\x0b\0',b''))
        with self.assertRaisesRegex(RuntimeError,'26.*11'):await session.next()


class RevisionTests(unittest.TestCase):
    def test_signed24_heat_and_schema3_backup_upgrade(self):
        raw=bytearray(RAW);raw[11:14]=(36000).to_bytes(3,'little',signed=True)
        legacy=profile('D3',legacy=True).decode(bytes(raw),block=0,position=0)
        self.assertEqual(legacy['heat_index_c'],-295.36)
        self.assertEqual(profile('D3').decode(bytes(raw),block=0,position=0)['heat_index_c'],360)
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'library.sqlite';store=Store(path,create=True)
            store.ensure_device('3','D3');t=store.begin_transfer('3',0,1)
            row=profile('D3').decode(bytes(raw),block=0,position=0)
            store.append_block(t,[row],[{'block':0,'records':1}]);store.finish_transfer(t,next_cursor=1,elapsed_seconds=0,maxrows=10,received_rows=[row])
            with store.transaction() as c:
                c.execute('DROP VIEW device_overview');c.execute('DROP TABLE device_actions')
                c.execute('UPDATE observations SET heat_index_c=?',(legacy['heat_index_c'],))
                c.execute("UPDATE transfers SET decoder_version='d3-21-v1'");c.execute('PRAGMA user_version=3')
            Store(path).check();self.assertEqual(Store(path).overview()[0]['records'],1)
            Store(path,create=True).check()
            self.assertEqual(Store(path).query('3')[0]['heat_index_c'],360)
            backup=next((Path(temp)/'backups').glob('schema3-before-upgrade-*.sqlite'))
            self.assertEqual(Store(backup).query('3')[0]['heat_index_c'],legacy['heat_index_c'])
            self.assertEqual(Store(path).query('3')[0]['record_hex'],bytes(raw).hex())

    def test_time_adjustment_keeps_sequence_and_latest_is_not_max_timestamp(self):
        with tempfile.TemporaryDirectory() as temp:
            store=Store(Path(temp)/'library.sqlite',create=True);store.ensure_device('1','D1')
            for cursor,minute in ((1,30),(2,20)):
                raw=bytearray(RAW[:9]);raw[1]=minute
                row=profile('D1').decode(bytes(raw),block=0,position=0)
                t=store.begin_transfer('1',0 if cursor==1 else cursor-1,1)
                store.append_block(t,[row],[{'block':0,'records':1}]);store.finish_transfer(t,next_cursor=cursor,elapsed_seconds=1,maxrows=10,received_rows=[row])
            store.check()
            self.assertEqual(store.status()[0]['last_timestamp'],'2026-10-22 14:20:00')
            self.assertEqual(store.overview()[0]['last_record_time'],'2026-10-22 14:20:00')
            self.assertEqual([r['absolute_cursor'] for r in store.query('1')],[0,1])


class ConcurrentTests(unittest.TestCase):
    def test_parallel_limit_real_sessions_shared_store_and_isolated_failure(self):
        with tempfile.TemporaryDirectory() as temp:
            root=Path(temp)/'archive'
            fleet=Fleet({str(i):{'name':'D1 - '+str(i)} for i in range(1,7)})
            expected={};owners={}
            for i in range(1,7):
                model=('D1','D2','D3')[(i-1)%3];width={'D1':9,'D2':16,'D3':21}[model]
                raw=bytearray(RAW[:width]);raw[7:9]=(1000+i).to_bytes(2,'little')
                expected[str(i)]=bytes(raw)
                owners[str(i)]=FakeBLE([bytes(raw)],100+i,serial=str(i),model=model)
            active=set();peak=[];closed=[];inside_writes=[]
            def client(device,**kwargs):
                serial=device.address;base=owners[serial].module.BleakClient
                class Client(base):
                    async def __aenter__(self):
                        active.add(serial);peak.append(len(active));await asyncio.sleep(.005)
                        return self
                    async def __aexit__(self,*args):active.discard(serial);closed.append(serial)
                    async def read_gatt_char(self,uuid):
                        if serial=='3':raise fleet.errors.BleakError('disconnected')
                        return await super().read_gatt_char(uuid)
                    async def write_gatt_char(self,*args,**kwargs):
                        inside_writes.append(len(active));await asyncio.sleep(.002)
                        return await super().write_gatt_char(*args,**kwargs)
                return Client(device,**kwargs)
            fleet.module.BleakClient=client
            output=io.StringIO()
            with fleet.patches(),redirect_stdout(output),redirect_stderr(io.StringIO()):
                code=manager.main(['--data-dir',str(root),'--events-json','download','--serials',*owners,'--parallel','2','--execute','--timeout','1'])
            self.assertEqual(code,2);self.assertEqual(max(peak),2);self.assertFalse(active)
            self.assertEqual(len(closed),6);self.assertIn(2,inside_writes)
            library=manager.Library(root);self.assertEqual(library.database_info()['records'],5)
            for serial in ('1','2','4','5','6'):self.assertEqual(library.records(serial)[0]['record_hex'],expected[serial].hex())
            events=[json.loads(line) for line in output.getvalue().splitlines()]
            self.assertEqual(len([e for e in events if e['event']=='completed']),5)
            self.assertEqual(next(e for e in events if e['event']=='error')['serial'],'3')

    def test_parallel_cancel_disconnects_all_and_preserves_only_durable_blocks(self):
        with tempfile.TemporaryDirectory() as temp:
            fleet=Fleet({str(i):{'name':'D1 - '+str(i)} for i in range(1,4)})
            owners={str(i):FakeBLE([RAW[:9]],101,serial=str(i),model='D1',interrupt=True) for i in range(1,4)}
            active=set();entered=[]
            def client(device,**kwargs):
                serial=device.address;base=owners[serial].module.BleakClient
                class Client(base):
                    async def __aenter__(self):active.add(serial);entered.append(serial);await asyncio.sleep(.005);return self
                    async def __aexit__(self,*args):active.discard(serial)
                return Client(device,**kwargs)
            fleet.module.BleakClient=client
            with fleet.patches(),redirect_stdout(io.StringIO()),redirect_stderr(io.StringIO()):
                code=manager.main(['--data-dir',temp,'--events-json','download','--serials','1','2','3','--parallel','2','--execute','--timeout','1'])
            self.assertEqual(code,130);self.assertFalse(active);self.assertNotIn('3',entered)
            info=manager.Library(temp).database_info()
            self.assertEqual(info['records'],0);self.assertTrue(any(d['pending_records'] for d in info['devices']))

class OfficialUITests(unittest.TestCase):
    def terminal(self):
        from kestrel_drop.tui import Terminal
        from test_iteration import FakeScreen
        terminal=Terminal(FakeScreen(),manager.Library('/tmp/kestrel-official-ui-only'))
        terminal.screen.refresh=lambda:None
        terminal.devices={'123':{'serial':'123','device_type':'D2','ble_id':'a','nearby':True}}
        return terminal

    def test_settings_entry_and_explicit_per_field_submission(self):
        terminal=self.terminal()
        with patch.object(terminal,'start') as start:
            terminal.handle('g');start.assert_called_once_with('device',['device','123','--ble-id','a'])
        terminal.device_state={'serial':'123','model':'D2','ble_id':'a','settings':{'interval_seconds':300,'wrap':True,'update_seconds':30}}
        terminal.handle('1');self.assertEqual(terminal.form['action'],'set_interval')
        terminal.form['fields'][0][1]='600'
        with patch.object(terminal,'start') as start:
            terminal.submit();start.assert_not_called()
            self.assertEqual(terminal.review['new'],'10 分钟（600 秒）')
            terminal.handle('\n')
            start.assert_called_once_with('set',['set','123','--interval','600','--execute','--ble-id','a'])
        self.assertIsNone(terminal.form)

    def test_parallel_selection_and_invalid_configuration_do_not_launch(self):
        terminal=self.terminal();terminal.handle('j');terminal.form['fields'][0][1]='2';terminal.submit()
        with patch.object(terminal,'start') as start:
            terminal.handle('A');terminal.handle('a');self.assertIn('--parallel',start.call_args.args[1]);self.assertEqual(start.call_args.args[1][-1],'2')
        terminal.device_state={'serial':'123','settings':{}}
        terminal.handle('4');terminal.form['fields'][0][1]='中文'
        with patch.object(terminal,'start') as start:terminal.submit();start.assert_not_called()
        self.assertIn('ASCII',terminal.form['error'])

    def test_per_device_progress_draws_both_receivers(self):
        terminal=self.terminal()
        terminal.devices['456']={'serial':'456','device_type':'D1','nearby':True}
        terminal.job=SimpleNamespace()
        for serial,count in [('123',12),('456',19)]:
            terminal.receive_event({'event':'progress','serial':serial,'received':count,'expected':100,'elapsed':2,'rate':5,'eta':10,'status':''})
        terminal.draw();text=' '.join(terminal.screen.lines)
        self.assertIn('接收 12/100',text);self.assertIn('接收 19/100',text)

    def test_external_local_database_change_triggers_read_only_refresh(self):
        terminal=self.terminal();terminal.last_stamp=((1,1),None)
        with patch.object(terminal,'database_stamp',return_value=((2,2),None)),patch.object(terminal,'start') as start:
            terminal.refresh_if_changed();start.assert_called_once_with('list',['list'])
        terminal.next_local_refresh=0;terminal.job=SimpleNamespace()
        with patch.object(terminal,'start') as start:terminal.refresh_if_changed();start.assert_not_called()

class ProtocolEdgeTests(unittest.IsolatedAsyncioTestCase):
    async def test_padded_clock_and_invalid_padding(self):
        value='2026-10-26 23:04:29'
        wire=bytes.fromhex('1d04171a0aea07')
        self.assertEqual(control.encode_link_time(value),wire)
        self.assertEqual(control.decode_link_time(wire+b'\0'),value)
        with self.assertRaises(ValueError):control.decode_link_time(wire+b'\1')
        fake=FakeControl()
        with fake.patches():
            before=await control.inspect(fake.client)
            after=await control.apply(fake.client,{'time':value},before)
        self.assertEqual(after['device_time'],value)
        writes=[f for f in fake.mass_writes if f.kind==0 and f.payload[:2]==b'\x0f\0']
        self.assertEqual(len(writes),1);self.assertEqual(writes[0].payload[2:],wire)
        self.assertNotIn(control.TIME_UUID,[uuid for uuid,_ in fake.writes])

    async def test_oversized_native_packet_and_short_completion_rejected(self):
        class Client:
            async def write_gatt_char(self,*args,**kwargs):pass
        session=protocol.BleSession(Client(),model='D1')
        session.callback(None,protocol.encode_frame(3,b'\0'*123))
        with self.assertRaisesRegex(RuntimeError,'128'):await session.next()
        session=protocol.BleSession(Client(),model='D1')
        for f in (protocol.Frame(1,profile('D1').schema,b''),protocol.Frame(3,b'\0\0'+RAW[:9],b''),
                  protocol.Frame(0,b'\x12\0'+(101).to_bytes(4,'little'),b'')):
            session.queue.put_nowait(f)
        with self.assertRaisesRegex(RuntimeError,'64-bit'):await session.download_from_cursor(100,1,100)

    async def test_atomic_schema3_upgrade_failure_preserves_original_semantics(self):
        import sqlite3
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/'library.sqlite';store=Store(path,create=True)
            store.ensure_device('3','D3')
            raw=bytearray(RAW);raw[11:14]=(36000).to_bytes(3,'little',signed=True)
            row=profile('D3').decode(bytes(raw),block=0,position=0)
            transfer=store.begin_transfer('3',0,1)
            store.append_block(transfer,[row],[{'block':0,'records':1}]);store.finish_transfer(transfer,next_cursor=1,elapsed_seconds=0,maxrows=10,received_rows=[row])
            with store.transaction() as c:
                c.execute('DROP VIEW device_overview');c.execute('DROP TABLE device_actions')
                c.execute('UPDATE observations SET heat_index_c=-295.36')
                c.execute("UPDATE transfers SET decoder_version='d3-21-v1'")
                c.execute("CREATE TRIGGER fail_upgrade BEFORE UPDATE ON transfers BEGIN SELECT RAISE(ABORT,'injected'); END")
                c.execute('PRAGMA user_version=3')
            with self.assertRaises(sqlite3.IntegrityError):Store(path,create=True)
            old=Store(path);old.check()
            self.assertEqual(old.query('3')[0]['heat_index_c'],-295.36)
            self.assertEqual(old.summary()['schema_version'],3)
            self.assertFalse(old.actions('3'))
            self.assertTrue(list((Path(temp)/'backups').glob('schema3-before-upgrade-*.sqlite')))

    async def test_cancel_in_settings_page_reaches_worker(self):
        terminal=OfficialUITests().terminal();terminal.device_state={'serial':'123'}
        terminal.job=SimpleNamespace(cancel=unittest.mock.Mock())
        terminal.handle('c');terminal.job.cancel.assert_called_once()

class CompletionUITests(unittest.TestCase):
    def test_each_completed_device_immediately_updates_its_own_row(self):
        terminal=OfficialUITests().terminal()
        terminal.devices['456']={'serial':'456','database_records':9}
        terminal.job=SimpleNamespace(outcomes=[])
        terminal.receive_event({'event':'completed','serial':'123','status':'downloaded',
            'added_records':3,'total_records':13,'pending_records':0,'last_timestamp':'2026-10-26 12:00:00'})
        self.assertEqual(terminal.devices['123']['database_records'],13)
        self.assertEqual(terminal.devices['456']['database_records'],9)
        terminal.draw();self.assertIn('已保存',' '.join(terminal.screen.lines))

class CommandValidationTests(unittest.IsolatedAsyncioTestCase):
    async def test_clock_data_requires_command_ack(self):
        class Client:
            async def write_gatt_char(self,*args,**kwargs):pass
        session=protocol.BleSession(Client(),model='D1')
        session.queue.put_nowait(protocol.Frame(3,b'\0'*8,b''))
        with self.assertRaisesRegex(RuntimeError,'before matching'):await session.read_command(14)

    async def test_reference_altitude_rejected_for_nonpressure_model(self):
        with tempfile.TemporaryDirectory() as temp:
            fake=FakeControl();library=manager.Library(temp)
            async def resolve(_):return SimpleNamespace(address='a')
            with fake.patches(),patch.object(manager.devices,'resolve',resolve):
                with self.assertRaisesRegex(ValueError,'此型号不适用'):
                    await library.device_operation('123',changes={'reference_altitude_m':100},execute=True)
            self.assertFalse(fake.writes);self.assertFalse(library.database_path.exists())
