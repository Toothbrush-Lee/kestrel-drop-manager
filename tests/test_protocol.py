"""Run from project root: python -m unittest discover -s tests -v"""
import asyncio
import unittest
from kestrel_drop.protocol import (
    encode_frame, decode_frame, crc16_x25, FrameStream, Frame,
    BleSession, decode_record, log_rows_from_frames,
)

class TestProtocol(unittest.TestCase):
    def test_known_packet_bytes_and_checksum(self):
        self.assertEqual(encode_frame(4,b'\x05\x00').hex(), '7e040002000500eda07e')
        self.assertEqual(encode_frame(0, bytes.fromhex('05 00 46 0c 01 00 00 00 00 00')).hex(),
                         '7e00000a000500460c01000000000077f07e')
        f=decode_frame(bytes.fromhex('040002000500eda0'))
        self.assertEqual((f.kind, f.payload), (4,b'\x05\x00'))

    def test_byte_stuffing_roundtrip(self):
        payload=bytes.fromhex('7e 7d 7d 7e 00')
        wrapped=encode_frame(3,payload)
        self.assertIn(b'\x7d\x5e',wrapped)
        self.assertIn(b'\x7d\x5d',wrapped)
        self.assertEqual(decode_frame(wrapped[1:-1]).payload,payload)

    def test_fragment_reassembly_and_zero_padding(self):
        frame=encode_frame(3,bytes(range(20)))
        parser=FrameStream();all=[]
        for chunk in (frame[:20],frame[20:]+b'\x00'*14):
            all.extend(parser.feed(chunk))
        self.assertEqual([f.payload for f in all],[bytes(range(20))])
        self.assertFalse(parser.errors)

    def test_crc_rejects_corruption(self):
        f=bytearray(encode_frame(4,b'\x05\x00'))
        f[5]^=1
        with self.assertRaises(ValueError):decode_frame(bytes(f[1:-1]))

    def test_observed_log_record(self):
        r=decode_record(bytes.fromhex('00140e160aea07f506990dfa0500d000c227360300'),block=0,position=0)
        self.assertEqual(r['timestamp_device_local'],'2026-10-22 14:20:00')
        self.assertEqual(r['temperature_c'],17.81)
        self.assertEqual(r['relative_humidity_pct'],34.81)
        self.assertEqual(r['station_pressure_hpa'],1017.8)
        self.assertEqual(r['density_altitude_m_official_integer_estimate'],82)

    def test_cursor_zero_after_log_wrap_does_not_discard_complete_read(self):
        async def run():
            sample = bytes.fromhex('00140e160aea07f506990dfa0500d000c227360300')
            class FakeClient:
                def __init__(self):
                    self.session = None
                    self.phase = 0
                async def write_gatt_char(self, uuid, raw, response):
                    decoded = decode_frame(raw[1:-1])
                    if decoded.kind == 0 and decoded.payload[:2] == b'\x05\x00':
                        self.session.queue.put_nowait(Frame(4, b'\x05\x00', b''))
                        self.session.queue.put_nowait(Frame(1, b'\x00'*42, b''))
                    elif decoded.kind == 4 and decoded.payload == b'\x05\x00':
                        if self.phase == 0:
                            self.session.queue.put_nowait(Frame(3, b'\x00\x00'+sample, b''))
                        else:
                            self.session.queue.put_nowait(Frame(0, b'\x12\x00'+(68726).to_bytes(4,'little'), b''))
                        self.phase += 1
            client = FakeClient()
            session = BleSession(client)
            client.session = session
            rows, blocks, next_cursor = await session.download_from_cursor(0, 1, 6220)
            self.assertEqual((len(rows), len(blocks), next_cursor), (1, 1, 68726))
        asyncio.run(run())

    def test_simulated_full_ble_transaction(self):
        async def run():
            sample=bytes.fromhex('00140e160aea07f506990dfa0500d000c227360300')
            status=Frame(3,(1).to_bytes(4,'little'),b'')
            data=Frame(3,b'\x00\x00'+sample,b'')
            final=Frame(0,b'\x12\x00'+(101).to_bytes(4,'little')+b'\x00'*4,b'')
            class FakeClient:
                def __init__(self):self.session=None;self.phase=0;self.sent=[]
                async def write_gatt_char(self,uuid,raw,response):
                    self.sent.append(raw)
                    decoded=decode_frame(raw[1:-1])
                    if decoded.kind==0 and decoded.payload[:2]==b'\x03\x00':
                        self.session.queue.put_nowait(Frame(4,b'\x03\x00',b''))
                        self.session.queue.put_nowait(status)
                    elif decoded.kind==0 and decoded.payload[:2]==b'\x05\x00':
                        self.session.queue.put_nowait(Frame(4,b'\x05\x00',b''))
                        self.session.queue.put_nowait(Frame(1,b'\x00'*42,b''))
                    elif decoded.kind==4 and decoded.payload==b'\x05\x00':
                        if self.phase==0:self.session.queue.put_nowait(data)
                        elif self.phase==1:self.session.queue.put_nowait(final)
                        self.phase+=1
            client=FakeClient();session=BleSession(client);client.session=session
            count=await session.query_count(100)
            self.assertEqual(count,1)
            rows,blocks,newcursor=await session.download_from_cursor(100,count,5)
            self.assertEqual((len(rows),len(blocks),newcursor),(1,1,101))
            self.assertEqual(len(client.sent),6)
        asyncio.run(run())

# Unit tests below deliberately use fake BLE objects, no physical device.
class TestConnectionResolution(unittest.IsolatedAsyncioTestCase):
    async def test_prefers_fresh_ble_device_object(self):
        import sys
        from types import SimpleNamespace, ModuleType
        from unittest.mock import patch
        from kestrel_drop.protocol import resolve_ble_device
        known = SimpleNamespace(address='43E58D0C-7132-D1CC-FCCC-432B5EDAD27E', name='D3 - 2866855')
        adv = SimpleNamespace(local_name='D3 - 2866855', rssi=-46)
        fake = ModuleType('bleak')
        class Scanner:
            @staticmethod
            async def discover(**kwargs):
                self.assertEqual(kwargs['return_adv'], True)
                return {'test': (known, adv)}
        async def find_device_by_filter(predicate, **kwargs):
            for device, advertisement in (await Scanner.discover(return_adv=True, **kwargs)).values():
                if predicate(device, advertisement): return device
            return None
        Scanner.find_device_by_filter = staticmethod(find_device_by_filter)
        fake.BleakScanner = Scanner
        args = SimpleNamespace(ble_id=known.address, serial='2866855',scan_timeout=10)
        with patch.dict(sys.modules, {'bleak': fake, 'bleak.exc': SimpleNamespace(BleakError=type('BleakError', (Exception,), {}))}):
            self.assertIs(await resolve_ble_device(args), known)

    async def test_fallback_by_advertised_serial_when_uuid_changed(self):
        import sys
        from types import SimpleNamespace, ModuleType
        from unittest.mock import patch
        from kestrel_drop.protocol import resolve_ble_device
        current = SimpleNamespace(address='NEW-COREBLUETOOTH-UUID', name='D3 - 2866855')
        adv = SimpleNamespace(local_name='D3 - 2866855', rssi=-41)
        fake = ModuleType('bleak')
        class Scanner:
            @staticmethod
            async def discover(**kwargs):
                return {'test': (current, adv)}
        async def find_device_by_filter(predicate, **kwargs):
            for device, advertisement in (await Scanner.discover(return_adv=True, **kwargs)).values():
                if predicate(device, advertisement): return device
            return None
        Scanner.find_device_by_filter = staticmethod(find_device_by_filter)
        fake.BleakScanner = Scanner
        args = SimpleNamespace(ble_id='OLD-UUID',serial='2866855',scan_timeout=10)
        with patch.dict(sys.modules, {'bleak': fake, 'bleak.exc': SimpleNamespace(BleakError=type('BleakError', (Exception,), {}))}):
            self.assertIs(await resolve_ble_device(args), current)

    async def test_no_broadcasts_gives_actionable_error(self):
        import sys
        from types import SimpleNamespace, ModuleType
        from unittest.mock import patch
        from kestrel_drop.protocol import resolve_ble_device
        fake = ModuleType('bleak')
        class Scanner:
            @staticmethod
            async def discover(**kwargs):
                return {}
        async def find_device_by_filter(predicate, **kwargs):
            for device, advertisement in (await Scanner.discover(return_adv=True, **kwargs)).values():
                if predicate(device, advertisement): return device
            return None
        Scanner.find_device_by_filter = staticmethod(find_device_by_filter)
        fake.BleakScanner = Scanner
        args = SimpleNamespace(ble_id='OLD-UUID',serial='2866855',scan_timeout=1)
        with patch.dict(sys.modules, {'bleak': fake, 'bleak.exc': SimpleNamespace(BleakError=type('BleakError', (Exception,), {}))}):
            with self.assertRaisesRegex(RuntimeError, 'No GATT commands were sent'):
                await resolve_ble_device(args)



class TestCountDriftValidation(unittest.TestCase):
    @staticmethod
    def fake_rows(n):
        from datetime import datetime, timedelta
        start = datetime(2026, 10, 20, 0, 0, 0)
        return [{'timestamp_device_local': (start + timedelta(minutes=5*i)).isoformat(sep=' ')} for i in range(n)]

    @staticmethod
    def blocks(n):
        return [{'block': i, 'records': min(5,n-5*i)} for i in range((n+4)//5)]

    def test_two_new_records_during_ten_minute_transfer(self):
        from kestrel_drop.protocol import verify_completed_transfer
        rows = self.fake_rows(5939)
        validation = verify_completed_transfer(
            rows=rows, blocks=self.blocks(5939), initial_count=5937,
            requested_cursor=0, next_cursor=68726, elapsed_seconds=600, maxrows=6220)
        self.assertEqual(validation['new_records_during_transfer'], 2)
        self.assertEqual(validation['estimated_record_interval_seconds'], 300)
        self.assertEqual(validation['verification'], 'complete_with_growth')
        self.assertTrue(validation['cursor_zero_clamped_to_oldest'])

    def test_shortfall_still_rejected(self):
        from kestrel_drop.protocol import verify_completed_transfer
        with self.assertRaisesRegex(RuntimeError, 'Incomplete transfer'):
            verify_completed_transfer(
                rows=self.fake_rows(8), blocks=self.blocks(8), initial_count=10,
                requested_cursor=0, next_cursor=100, elapsed_seconds=600, maxrows=6220)

    def test_gap_in_blocks_rejected(self):
        from kestrel_drop.protocol import verify_completed_transfer
        blocks = self.blocks(10)
        blocks[1]['block'] = 3
        with self.assertRaisesRegex(RuntimeError, 'blocks'):
            verify_completed_transfer(
                rows=self.fake_rows(10), blocks=blocks, initial_count=10,
                requested_cursor=0, next_cursor=100, elapsed_seconds=600, maxrows=6220)

    def test_implausibly_large_growth_rejected(self):
        from kestrel_drop.protocol import verify_completed_transfer
        with self.assertRaisesRegex(RuntimeError, 'Unexpected growth'):
            verify_completed_transfer(
                rows=self.fake_rows(30), blocks=self.blocks(30), initial_count=10,
                requested_cursor=0, next_cursor=100, elapsed_seconds=10, maxrows=6220)

    def test_nonzero_cursor_mismatch_rejected(self):
        from kestrel_drop.protocol import verify_completed_transfer
        with self.assertRaisesRegex(RuntimeError, 'Final cursor disagrees'):
            verify_completed_transfer(
                rows=self.fake_rows(10), blocks=self.blocks(10), initial_count=10,
                requested_cursor=100, next_cursor=500, elapsed_seconds=600, maxrows=6220)

    def test_clean_transfer_and_publish(self):
        import tempfile
        from pathlib import Path
        import json
        from kestrel_drop.protocol import verify_completed_transfer, publish_live_files
        rows = self.fake_rows(10)
        verdict = verify_completed_transfer(
            rows=rows, blocks=self.blocks(10), initial_count=10,
            requested_cursor=100, next_cursor=110, elapsed_seconds=50, maxrows=6220)
        self.assertEqual(verdict['verification'], 'complete')
        with tempfile.TemporaryDirectory() as d:
            outfile = Path(d) / 'history.csv'
            publish_live_files(outfile, rows, {'validation': verdict})
            self.assertTrue(outfile.exists())
            self.assertEqual(json.loads((Path(d)/'history.csv.json').read_text())['validation']['next_cursor'], 110)
            with self.assertRaises(FileExistsError):
                publish_live_files(outfile, rows, {})

if __name__ == '__main__':
    unittest.main()
