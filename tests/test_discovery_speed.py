"""Target address selection exits on discovery; serial-only scans remain complete."""
import asyncio
from types import ModuleType, SimpleNamespace as Item
import sys
import unittest
from unittest.mock import AsyncMock, patch
from kestrel_drop import devices, manager
from kestrel_drop.tui import Terminal
from test_iteration import FakeScreen


class TargetDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    def scanner(self, entries):
        module=ModuleType('bleak');errors=ModuleType('bleak.exc')
        errors.BleakError=type('BleakError',(Exception,),{})
        self.visited=[]
        self.full=AsyncMock(return_value={device.address:(device,adv) for device,adv in entries})
        async def find(predicate, **kwargs):
            self.timeout=kwargs['timeout']
            for device,adv in entries:
                self.visited.append(device.address)
                if predicate(device,adv):return device
            return None
        module.BleakScanner=Item(discover=self.full,find_device_by_filter=find)
        return patch.dict(sys.modules,{'bleak':module,'bleak.exc':errors})
    def entry(self,address,serial='123'):
        return Item(address=address,name='D1 - '+serial),Item(local_name='D1 - '+serial,rssi=-50,service_uuids=[])
    async def test_address_stops_at_target_without_full_scan(self):
        before=self.entry('other','456');target=self.entry('ABC');after=self.entry('never','789')
        with self.scanner([before,target,after]):
            found=await devices.resolve(Item(ble_id='abc',serial='123',scan_timeout=20))
        self.assertIs(found,target[0]);self.assertEqual(self.visited,['other','ABC'])
        self.full.assert_not_awaited();self.assertEqual(self.timeout,20)
    async def test_snapshot_hit_never_starts_scanner(self):
        target=self.entry('abc')
        with self.scanner([]):
            found=await devices.resolve(Item(ble_id='ABC',serial='123',scan_timeout=20,discovery={'a':target}))
        self.assertIs(found,target[0]);self.assertEqual(self.visited,[]);self.full.assert_not_awaited()
    async def test_stale_address_uses_unique_serial_seen_in_same_pass(self):
        target=self.entry('new')
        with self.scanner([target]):
            found=await devices.resolve(Item(ble_id='old',serial='123',scan_timeout=3))
        self.assertIs(found,target[0]);self.full.assert_not_awaited()
    async def test_stale_address_duplicate_serial_remains_ambiguous(self):
        with self.scanner([self.entry('a'),self.entry('b')]):
            with self.assertRaises(devices.DeviceMismatch):await devices.resolve(Item(ble_id='old',serial='123',scan_timeout=3))
        self.full.assert_not_awaited()
    async def test_serial_only_keeps_full_scan_for_duplicate_detection(self):
        with self.scanner([self.entry('a'),self.entry('b')]):
            with self.assertRaises(devices.DeviceMismatch):await devices.resolve(Item(ble_id=None,serial='123',scan_timeout=3))
        self.full.assert_awaited_once_with(timeout=3,return_adv=True);self.assertEqual(self.visited,[])
    async def test_missing_address_does_not_start_second_scan(self):
        with self.scanner([]):
            with self.assertRaises(devices.BluetoothFailure):await devices.resolve(Item(ble_id='absent',serial='123',scan_timeout=3))
        self.full.assert_not_awaited()
    async def test_cancellation_does_not_retry_scan(self):
        with self.scanner([]),patch('bleak.BleakScanner.find_device_by_filter',side_effect=asyncio.CancelledError):
            with self.assertRaises(asyncio.CancelledError):await devices.resolve(Item(ble_id='abc',serial='123',scan_timeout=3))
        self.full.assert_not_awaited()


class ScanControlsTests(unittest.TestCase):
    def test_cli_standard_scan_and_explicit_timeout(self):
        self.assertEqual(manager.parser().parse_args(['scan']).timeout,15)
        self.assertEqual(manager.parser().parse_args(['scan','--timeout','20']).timeout,20)
    def test_tui_has_one_standard_scan(self):
        terminal=Terminal(FakeScreen(),manager.Library('/tmp/kestrel-scan-ui-model-only'))
        with patch.object(terminal,'start') as start:
            terminal.handle('s');start.assert_called_once_with('scan',['scan'])
            start.reset_mock();terminal.handle('S');start.assert_called_once_with('scan',['scan'])
    def test_settings_pass_existing_scan_address(self):
        terminal=Terminal(FakeScreen(),manager.Library('/tmp/kestrel-scan-ui-model-only'))
        terminal.devices={'123':{'serial':'123','ble_id':'abc','nearby':True}}
        with patch.object(terminal,'start') as start:
            terminal.handle('g');start.assert_called_once_with('device',['device','123','--ble-id','abc'])
