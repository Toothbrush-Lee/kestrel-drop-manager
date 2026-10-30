"""Firmware readback settling and unused bytes must not cause false failures."""
import asyncio
from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from kestrel_drop import control,manager
from kestrel_drop.errors import ConfigurationFailure
from test_official import FakeControl


class SettingsReadbackTests(unittest.IsolatedAsyncioTestCase):
    async def run_with_reads(self,change_read,*,changes=None,reject=False):
        fake=FakeControl(reject=reject)
        original=fake.client.read_gatt_char
        count=0
        async def read(uuid):
            nonlocal count
            raw=await original(uuid)
            if uuid==control.SETTINGS_UUID and fake.writes:
                count+=1
                return change_read(raw,count,fake)
            return raw
        with fake.patches():before=await control.inspect(fake.client)
        # Do not patch asyncio.sleep: cancellation and read-only delays are real.
        with fake.patches(),patch.object(fake.client,'read_gatt_char',side_effect=read):
            result=await control.apply(fake.client,changes or {'interval_seconds':600},before)
        return result,fake,count
    async def test_write_echo_settles_without_second_write(self):
        result,fake,count=await self.run_with_reads(lambda raw,n,f:f.writes[-1][1] if n<3 else raw)
        self.assertEqual(len(fake.writes),1);self.assertEqual(count,3)
        self.assertEqual(result['settings']['interval_seconds'],600)
        self.assertEqual(result['settings']['wrap'],True)
        self.assertEqual(result['settings_readback_attempts'],3)
        self.assertEqual(result['settings_raw_hex'],fake.values[control.SETTINGS_UUID].hex())
        self.assertEqual(result['settings_readback_samples'][:2],[fake.writes[0][1].hex()]*2)
        self.assertFalse(any(frame.payload[:2]==b'\x0f\0' for frame in fake.mass_writes))
    async def test_previous_value_settles_before_success(self):
        result,fake,count=await self.run_with_reads(lambda raw,n,f:(300).to_bytes(3,'little')+raw[3:] if n==1 else raw)
        self.assertEqual(count,2);self.assertEqual(len(fake.writes),1)
        self.assertTrue(result['settings_verification_complete'])
    async def test_unused_bytes_are_recorded_but_do_not_fail(self):
        result,fake,count=await self.run_with_reads(lambda raw,n,f:raw[:9]+b'\xff'*11)
        self.assertEqual(count,1);self.assertEqual(result['settings_unused_changed_offsets'],list(range(9,20)))
        self.assertEqual(fake.writes[0][1][9:],bytes(range(11)))
        self.assertEqual(result['settings_raw_hex'][18:],'ff'*11)
    async def test_every_defined_unselected_field_is_still_protected(self):
        for offset,new_value,field in ((3,0,'wrap'),(4,20,'update_seconds'),(5,1,'security_level'),(6,0,'setup_complete'),(7,5,'reference_altitude_m')):
            with self.subTest(field=field):
                def changed(raw,n,f):
                    data=bytearray(raw);data[offset]=new_value;return bytes(data)
                with self.assertRaises(control.SettingsVerificationError) as caught:await self.run_with_reads(changed)
                self.assertIn(field,str(caught.exception))
                self.assertEqual(len(caught.exception.readback['settings_readback_samples']),4)
                self.assertEqual(caught.exception.readback['settings_differences'][0]['offset'],offset)
    async def test_requested_value_rejection_is_not_silenced(self):
        with self.assertRaisesRegex(control.SettingsVerificationError,'interval_seconds.*回读|回读不一致.*interval_seconds') as caught:
            await self.run_with_reads(lambda raw,n,f:raw,reject=True)
        self.assertEqual(caught.exception.readback['settings']['interval_seconds'],300)
        self.assertEqual(len(caught.exception.readback['settings_readback_samples']),4)
    async def test_malformed_read_is_not_accepted_as_unused_difference(self):
        with self.assertRaisesRegex(control.SettingsVerificationError,'格式无效'):
            await self.run_with_reads(lambda raw,n,f:raw[:-1])
    async def test_repeated_echo_records_ignore_markers_without_claiming_success(self):
        with self.assertRaises(control.SettingsVerificationError) as caught:
            await self.run_with_reads(lambda raw,n,f:f.writes[-1][1])
        self.assertIn('security_level',str(caught.exception))
        self.assertEqual(caught.exception.readback['settings']['interval_seconds'],600)
        self.assertIsNone(caught.exception.readback['settings']['wrap'])
    async def test_transport_failure_preserves_partial_samples(self):
        def fail(raw,n,f):
            if n==2:raise TimeoutError('read timeout')
            return f.writes[-1][1]
        with self.assertRaises(control.SettingsVerificationError) as caught:await self.run_with_reads(fail)
        self.assertEqual(len(caught.exception.readback['settings_readback_samples']),1)
        self.assertFalse(caught.exception.readback['settings_verification_complete'])
    async def test_manager_failure_audit_keeps_expected_and_actual_bytes(self):
        fake=FakeControl();Client=fake.module.BleakClient;original=Client.read_gatt_char
        async def read(client,uuid):
            raw=await original(client,uuid)
            if uuid==control.SETTINGS_UUID and fake.writes:return raw[:5]+b'\x01'+raw[6:]
            return raw
        with tempfile.TemporaryDirectory() as temporary:
            library=manager.Library(Path(temporary)/'archive')
            with fake.patches(),patch.object(Client,'read_gatt_char',read),patch.object(manager.devices,'resolve',return_value=SimpleNamespace(address='a')),redirect_stdout(io.StringIO()):
                with self.assertRaisesRegex(ConfigurationFailure,'security_level'):
                    await library.device_operation('123',changes={'interval_seconds':600},execute=True)
            audit=library.store.actions('123')[0]
            self.assertEqual(audit['status'],'failed_or_unknown')
            self.assertEqual(audit['after']['settings']['interval_seconds'],600)
            self.assertEqual(len(audit['after']['settings_readback_samples']),4)
            self.assertEqual(audit['after']['settings_differences'],[{'offset':5,'expected':0,'actual':1}])
            self.assertEqual(len(fake.writes),1)
    async def test_cancel_during_readback_does_not_rewrite(self):
        fake=FakeControl();original=fake.client.read_gatt_char
        async def read(uuid):
            raw=await original(uuid)
            return fake.writes[-1][1] if uuid==control.SETTINGS_UUID and fake.writes else raw
        with fake.patches():before=await control.inspect(fake.client)
        with fake.patches(),patch.object(fake.client,'read_gatt_char',side_effect=read):
            task=asyncio.create_task(control.apply(fake.client,{'interval_seconds':600},before))
            for _ in range(100):
                await asyncio.sleep(.001)
                if fake.writes:break
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
        self.assertEqual(len(fake.writes),1)
