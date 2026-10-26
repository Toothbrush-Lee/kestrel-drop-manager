"""Captured layouts and reversible schema-2 upgrade boundaries."""
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from kestrel_drop.database import Store, SCHEMA, APPLICATION_ID
from kestrel_drop.layouts import profile
from kestrel_drop import protocol
from test_manager import RAW

class LayoutTests(unittest.TestCase):
    def test_width_and_null_fields_and_negative_temperature(self):
        for model,width in [('D1',9),('D2',16),('D3',21)]:
            raw=bytearray(RAW[:width]);raw[7:9]=(-1234).to_bytes(2,'little',signed=True)
            row=profile(model).decode(raw,block=0,position=0)
            self.assertEqual(row['temperature_c'],-12.34)
            with self.assertRaises(ValueError): profile(model).decode(raw+b'\0',block=0,position=0)
            if model!='D3': self.assertIsNone(row['station_pressure_hpa'])
            if model=='D1': self.assertIsNone(row['relative_humidity_pct'])

    def test_absent_fields_cannot_be_fabricated_as_zero(self):
        from kestrel_drop.database import checked_rows
        for model,width in [('D1',9),('D2',16)]:
            row=profile(model).decode(RAW[:width],block=0,position=0)
            checked_rows([row],model=model)
            row['station_pressure_hpa']=0
            with self.assertRaisesRegex(RuntimeError,'必须为空'):checked_rows([row],model=model)

class UpgradeTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'library.sqlite'
        with sqlite3.connect(self.path) as c:
            for statement in SCHEMA[:7]:
                if statement.startswith('CREATE TABLE pending_blocks'):
                    statement=statement.replace('length(payload)>0','length(payload)>0 AND length(payload)%21=0')
                if statement.startswith('CREATE TABLE observations'):
                    statement=statement.replace('IN (9,16,21)','=21')
                    for field in ['relative_humidity_pct','station_pressure_hpa','heat_index_c','dew_point_c','density_altitude_m_raw']:
                        statement=statement.replace(field+' REAL,',field+' REAL NOT NULL,')
                c.execute(statement)
            c.execute('PRAGMA user_version=2');c.execute(f'PRAGMA application_id={APPLICATION_ID}')
        store=Store(self.path);store.ensure_device('3','D3')
        transfer=store.begin_transfer('3',0,1)
        row=protocol.decode_record(RAW,block=0,position=0)
        store.append_block(transfer,[row],[{'block':0,'records':1}])
        store.finish_transfer(transfer,next_cursor=101,elapsed_seconds=0,maxrows=20000,received_rows=[row])
        transfer=store.begin_transfer('3',101,1)
        raw=bytearray(RAW);raw[1]+=5
        row=protocol.decode_record(bytes(raw),block=0,position=0)
        store.append_block(transfer,[row],[{'block':0,'records':1}])

    def test_read_is_pure_upgrade_keeps_pending_and_backup(self):
        Store(self.path).check()
        self.assertFalse((self.path.parent/'backups').exists())
        store=Store(self.path,create=True);store.check()
        self.assertEqual(store.status()[0]['pending_records'],1)
        backups=list((self.path.parent/'backups').glob('*.sqlite'))
        self.assertEqual(len(backups),1);Store(backups[0]).check()
        self.assertEqual(store.query('3'),Store(backups[0]).query('3'))
        self.assertEqual(len(store.query('3')),1)
        with store.connect() as c: self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],4)
        store.ensure_device('1','D1')
        t=store.begin_transfer('1',0,1)
        row=protocol.decode_record(RAW[:9],block=0,position=0,model='D1')
        store.append_block(t,[row],[{'block':0,'records':1}])
        store.finish_transfer(t,next_cursor=1,elapsed_seconds=0,maxrows=20000,received_rows=[row])
        store.check()

    def test_ddl_failure_rolls_back_all_tables_and_version(self):
        original=Store._copy_v2_tables
        def fail(store,c): original(store,c);raise OSError('injected DDL failure')
        with patch.object(Store,'_copy_v2_tables',fail):
            with self.assertRaises(OSError): Store(self.path,create=True)
        store=Store(self.path);store.check()
        self.assertEqual(store.status()[0]['pending_records'],1)
        with store.connect() as c: self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],2)
        Store(self.path,create=True).check()

class ModelTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_two_blocks_commit_before_each_ack_and_final_publish(self):
        from kestrel_drop.protocol import BleSession, Frame, decode_frame
        for model,width in [('D1',9),('D2',16)]:
            sequence=[]
            class Client:
                async def write_gatt_char(self,uuid,raw,response):
                    frame=protocol.FrameStream().feed(raw)[0]
                    sequence.append(('write',frame.kind,frame.payload[:2]))
            session=BleSession(Client(),model=model)
            raw=RAW[:width]
            for frame in [Frame(1,profile(model).schema,b''),
                          Frame(3,b'\0\0'+raw,b''),Frame(3,b'\1\0'+raw,b''),
                          Frame(0,b'\x12\0'+(102).to_bytes(8,'little'),b'')]:
                session.queue.put_nowait(frame)
            def block(rows,meta): sequence.append(('durable',meta[0]['block'],len(bytes.fromhex(rows[0]['record_hex']))))
            def complete(rows,blocks,cursor): sequence.append(('published',len(rows),cursor))
            rows,_,cursor=await session.download_from_cursor(100,2,20000,on_block=block,on_complete=complete)
            self.assertEqual((len(rows),cursor),(2,102))
            self.assertEqual(sequence,[('write',0,b'\x05\0'),('write',4,b'\xff\xff'),
                ('durable',0,width),('write',4,b'\xff\xff'),('durable',1,width),
                ('write',4,b'\xff\xff'),('published',2,102),('write',4,b'\x12\0')])

    async def test_missing_schema_wrong_length_or_write_failure_never_ack_block(self):
        from kestrel_drop.protocol import BleSession, Frame, decode_frame, TransferError
        for model,width in [('D1',9),('D2',16)]:
            for fault in ('missing_schema','wrong_length','disk_failure'):
                writes=[];blocks=[]
                class Client:
                    async def write_gatt_char(self,uuid,raw,response): writes.append(protocol.FrameStream().feed(raw)[0])
                session=BleSession(Client(),model=model)
                if fault!='missing_schema':session.queue.put_nowait(Frame(1,profile(model).schema,b''))
                raw=RAW[:width]+(b'\0' if fault=='wrong_length' else b'')
                session.queue.put_nowait(Frame(3,b'\0\0'+raw,b''))
                def persist(rows,meta):
                    blocks.append(rows)
                    if fault=='disk_failure':raise OSError('disk full')
                with self.assertRaises(OSError if fault=='disk_failure' else TransferError):
                    await session.download_from_cursor(0,1,20000,on_block=persist)
                self.assertEqual(len(writes),1 if fault=='missing_schema' else 2)
                self.assertEqual(bool(blocks),fault=='disk_failure')
