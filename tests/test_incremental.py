"""Transactional streaming CSV tests; no BLE device or network required."""
import csv
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from kestrel_drop.protocol import (
    decode_record, _archive_paths, _sha256, _rows_checked, _state_loaded,
    _pending_loaded, _new_pending, _json_atomic, _check_archive_or_recover,
    BlockSpool, adopt_archive, commit_pending, archive_status,
)

SAMPLE = bytes.fromhex('00140e160aea07f506990dfa0500d000c227360300')


def make_rec(minute, humid_add=0):
    raw = bytearray(SAMPLE)
    raw[1] = minute
    if humid_add:
        rh = int.from_bytes(raw[9:11], 'little') + humid_add
        raw[9:11] = rh.to_bytes(2, 'little')
    return decode_record(bytes(raw), block=0, position=0)


class TestIncrementalArchive(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.output = Path(self.dir.name) / 'd3_2866855.csv'
        self.metadata = Path(self.dir.name) / 'd3_2866855.csv.json'
        self.start = [make_rec(20), make_rec(25)]
        with self.output.open('w',newline='',encoding='utf-8') as f:
            writer = csv.DictWriter(f,fieldnames=list(self.start[0]))
            writer.writeheader();writer.writerows(self.start)
        self.metadata.write_text(json.dumps({
            'serial':'2866855','model':'Kestrel DROP 3', 'rows':2,
            'first_record_time':self.start[0]['timestamp_device_local'],
            'last_record_time':self.start[-1]['timestamp_device_local'],
            'validation':{'next_cursor':102,'inferred_first_cursor':100}
        }),encoding='utf-8')
        self.paths = _archive_paths(self.output)

    def adopt(self):
        return adopt_archive(self.output,self.metadata)

    def initialize_pending(self, state):
        journal = _new_pending(self.paths,state)
        spool = BlockSpool(self.paths['pending'],state['fieldnames'])
        return journal, spool

    def complete(self, journal, next_cursor):
        journal['completed'] = {'next_cursor':next_cursor,'validation':{'verification':'complete'}}
        _json_atomic(self.paths['journal'], journal)

    def test_adopt_existing_with_cursor(self):
        state=self.adopt()
        self.assertEqual(state['next_cursor'],102)
        self.assertEqual(state['row_count'],2)
        self.assertEqual(state['serial'],'2866855')
        self.assertEqual(state['csv_sha256'],_sha256(self.output))
        self.assertEqual(archive_status(self.output),0)

    def test_duplicate_timestamp_not_deduped(self):
        state=self.adopt()
        journal,spool=self.initialize_pending(state)
        # Two physically different records may have identical device timestamps.
        additions=[make_rec(30),make_rec(30,humid_add=150)]
        spool.append(additions,[{'block':0,'records':2}])
        self.complete(journal,104)
        result=commit_pending(self.paths,state)
        self.assertEqual(result['next_cursor'],104)
        names,rows=_rows_checked(self.output)
        self.assertEqual(len(rows),4)
        self.assertEqual(rows[-2]['timestamp_device_local'],rows[-1]['timestamp_device_local'])
        self.assertNotEqual(rows[-2]['record_hex'],rows[-1]['record_hex'])

    def test_spool_survives_restart_before_completion(self):
        state=self.adopt()
        journal,spool=self.initialize_pending(state)
        spool.append([make_rec(30),make_rec(35)],[{'block':0,'records':2}])
        reloaded=_state_loaded(self.paths)
        journal2,rows=_pending_loaded(self.paths,reloaded)
        self.assertEqual(len(rows),2)
        self.assertEqual(reloaded['next_cursor']+len(rows),104)
        self.assertNotIn('completed',journal2)
        self.assertEqual(len(_rows_checked(self.output)[1]),2)
        with self.assertRaisesRegex(RuntimeError,'no validated end-of-transfer'):
            commit_pending(self.paths,reloaded)

    def test_finalize_atomic_commit_then_no_duplicates(self):
        state=self.adopt()
        original=self.output.read_bytes()
        journal,spool=self.initialize_pending(state)
        spool.append([make_rec(30),make_rec(35)],[{'block':0,'records':2}])
        self.complete(journal,104)
        updated=commit_pending(self.paths,state)
        self.assertEqual(updated['row_count'],4)
        self.assertEqual(updated['next_cursor'],104)
        self.assertFalse(self.paths['pending'].exists())
        self.assertFalse(self.paths['journal'].exists())
        self.assertTrue(self.output.read_bytes().startswith(original))
        self.assertEqual(updated['csv_sha256'],_sha256(self.output))
        self.assertEqual(_check_archive_or_recover(self.paths,updated),updated)
        self.assertEqual(len(_rows_checked(self.output)[1]),4)

    def test_crash_between_csv_replace_and_state_update_recovers(self):
        import kestrel_drop.protocol as kp
        state=self.adopt()
        journal,spool=self.initialize_pending(state)
        spool.append([make_rec(30)],[{'block':0,'records':1}])
        self.complete(journal,103)
        with patch.object(kp,'_commit_state_from_completed',side_effect=OSError('simulated crash')):
            with self.assertRaisesRegex(OSError,'simulated crash'):
                commit_pending(self.paths,state)
        self.assertEqual(_sha256(self.output),json.loads(self.paths['journal'].read_text())['completed']['staged_sha256'])
        self.assertNotEqual(_sha256(self.output),_state_loaded(self.paths)['csv_sha256'])
        recovered=_check_archive_or_recover(self.paths,_state_loaded(self.paths))
        self.assertEqual(recovered['row_count'],3)
        self.assertEqual(recovered['next_cursor'],103)
        self.assertEqual(len(_rows_checked(self.output)[1]),3)
        self.assertFalse(self.paths['pending'].exists())

    def test_tampered_canonical_csv_stops_sync(self):
        state=self.adopt()
        with self.output.open('a') as f:f.write('tampering!\n')
        with self.assertRaisesRegex(RuntimeError,'checksum changed'):
            _check_archive_or_recover(self.paths,state)
        self.assertEqual(_state_loaded(self.paths)['next_cursor'],102)

    def test_corrupt_partial_row_stops_sync(self):
        state=self.adopt()
        _,spool=self.initialize_pending(state)
        spool.append([make_rec(30)],[{'block':0,'records':1}])
        with self.paths['pending'].open('a') as f:f.write('partial,bad,row\n')
        with self.assertRaisesRegex(RuntimeError, 'corrupt CSV|failed integrity'):
            _pending_loaded(self.paths,state)

    def test_adopt_refuses_wrong_cursor(self):
        metadata=json.loads(self.metadata.read_text())
        metadata['validation']['inferred_first_cursor']=99
        self.metadata.write_text(json.dumps(metadata))
        with self.assertRaisesRegex(RuntimeError,'cursor inconsistent'):
            self.adopt()

    def test_no_second_adopt(self):
        self.adopt()
        with self.assertRaisesRegex(RuntimeError,'already exists'):
            self.adopt()


if __name__=='__main__':
    unittest.main()

class TestSyncWithFakeBLE(unittest.TestCase):
    def test_end_to_end_incremental_device_and_live_block_spool(self):
        """Exercise the real async sync workflow using a protocol-faithful fake BLE client."""
        import sys
        import asyncio
        from types import ModuleType, SimpleNamespace
        from unittest.mock import patch
        import kestrel_drop.protocol as kp
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'d3.csv'
            meta=Path(d)/'d3.csv.json'
            first=make_rec(20)
            with path.open('w',encoding='utf-8',newline='') as f:
                w=csv.DictWriter(f,fieldnames=list(first))
                w.writeheader();w.writerow(first)
            meta.write_text(json.dumps({
              'serial':'2866855','model':'Kestrel DROP 3','rows':1,
              'first_record_time':first['timestamp_device_local'],
              'last_record_time':first['timestamp_device_local'],
              'validation':{'next_cursor':101,'inferred_first_cursor':100}}))
            kp.adopt_archive(path,meta)
            next_record=make_rec(25)
            history = bytes.fromhex(next_record['record_hex'])
            client_received=[]
            class FakeClient:
                def __init__(self,*a,**kw):
                    self.callback=None
                    self.phase=0
                async def __aenter__(self):return self
                async def __aexit__(self,*a):return None
                async def read_gatt_char(self,uuid):
                    return b'2866855\x00' if uuid==kp.SERIAL_UUID else b'Kestrel DROP 3\x00'
                async def start_notify(self,uuid,cb):self.callback=cb
                async def stop_notify(self,uuid):pass
                def transmit(self,kind,payload):
                    raw=kp.encode_frame(kind,payload)
                    for offset in range(0,len(raw),20):
                        self.callback(None,raw[offset:offset+20])
                async def write_gatt_char(self,uuid,raw,response):
                    frame=kp.decode_frame(raw[1:-1])
                    client_received.append(frame)
                    if frame.kind==0 and frame.payload[:2]==b'\x03\x00':
                        self.transmit(4,b'\x03\x00')
                        self.transmit(3,(1).to_bytes(4,'little'))
                    elif frame.kind==0 and frame.payload[:2]==b'\x05\x00':
                        self.transmit(4,b'\x05\x00')
                        self.transmit(1,b'abc')
                    elif frame.kind==4 and frame.payload==b'\x05\x00':
                        if self.phase==0:
                            self.transmit(3,b'\x00\x00'+history)
                        elif self.phase==1:
                            # Verify block was durably written before app-layer ACK.
                            with (Path(str(path)+'.pending.csv')).open() as fd:
                                self.persisted_before_ack=len(list(csv.DictReader(fd)))
                            self.transmit(0,b'\x12\x00'+(102).to_bytes(4,'little'))
                        self.phase+=1
                persisted_before_ack=0
            fake=ModuleType('bleak')
            fake.BleakClient=FakeClient
            args=SimpleNamespace(execute=True,output=str(path),serial='2866855',
                                 ble_id=None,scan_timeout=1,maxrows=6220)
            async def fake_resolve(_):return object()
            with patch.dict(sys.modules,{'bleak':fake}), patch.object(kp,'resolve_ble_device',fake_resolve):
                self.assertEqual(asyncio.run(kp.sync_live(args)),0)
            state=json.loads(Path(str(path)+'.sync-state.json').read_text())
            self.assertEqual((state['row_count'],state['next_cursor']),(2,102))
            self.assertEqual(len(kp._rows_checked(path)[1]),2)
            self.assertFalse(Path(str(path)+'.pending.csv').exists())

class TestArchiveLock(unittest.TestCase):
    def test_lock_prevents_simultaneous_writers(self):
        from kestrel_drop.protocol import _archive_lock
        with tempfile.TemporaryDirectory() as d:
            output=Path(d)/'csv.csv'
            with _archive_lock(output):
                with self.assertRaisesRegex(RuntimeError,'Another download'):
                    with _archive_lock(output):pass
            with _archive_lock(output):pass
