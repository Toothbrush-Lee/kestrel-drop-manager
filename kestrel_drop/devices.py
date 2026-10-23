"""Model-aware discovery and read-only device identity.

Advertisements are hints; only Device Information Service reads establish the
identity used for registration. BLE addresses are connection locators, never IDs.
"""
from __future__ import annotations

import asyncio
import re
from types import SimpleNamespace

from . import events
from .errors import BluetoothFailure, DeviceMismatch, UnsupportedModel

DROP_SERVICE = '12630000-cc25-497d-9854-9b6c02c77054'
HISTORY_SERVICE = '85920000-0338-4b83-ae4a-ac1d217adb03'
SERIAL_UUID = '00002a25-0000-1000-8000-00805f9b34fb'
MODEL_UUID = '00002a24-0000-1000-8000-00805f9b34fb'


def serial_number(value):
    value = str(value).strip()
    if not re.fullmatch(r'[0-9]{1,20}', value):
        raise ValueError('序列号必须为 1–20 位数字')
    return value


def model_type(value):
    """Accept known model labels, never classify D10 as D1 or D3 Fire as D3."""
    match = re.fullmatch(r'(?:Kestrel\s+)?(?:DROP\s*D?|D)([123])', str(value).strip(), re.I)
    return 'D' + match[1] if match else None


def canonical_model(value):
    kind = model_type(value)
    if kind is None:
        raise UnsupportedModel(f'型号 {value!r} 尚未识别；不会发送历史或配置指令')
    return 'Kestrel DROP ' + kind[1]


def capabilities(value):
    kind = model_type(value)
    return {'device_type': kind or '未知', 'history_supported': kind in ('D1', 'D2', 'D3'),
            'supported': kind in ('D1', 'D2', 'D3'), 'identification_supported': kind is not None}


def advertisement(device, adv):
    name = (adv.local_name or device.name or '').strip().strip('\x00').strip()
    match = re.fullmatch(r'(?:Kestrel\s+)?(?:DROP\s*D?|D)([123])(?:\s*[-–—_:]\s*|\s+)([0-9]{1,20})', name, re.I)
    hint = re.match(r'(?:Kestrel\s+)?(?:DROP\s*D?|D)([123])(?:\s|[-–—_:]|$)', name, re.I)
    kind = 'D' + match[1] if match else 'D' + hint[1] if hint else model_type(name)
    services = {s.lower() for s in (getattr(adv, 'service_uuids', None) or [])}
    if not (kind or services & {DROP_SERVICE, HISTORY_SERVICE} or re.match(r'^(?:drop|kestrel)\b', name, re.I)):
        return None
    return {'serial': match[2] if match else None, 'name': name or '未命名 DROP 候选设备', 'advertised_name': name,
            'ble_id': device.address, 'rssi': adv.rssi, 'device_type': kind or '未知',
            'model': 'Kestrel DROP ' + kind[1] if kind else None,
            'identity_verified': False, 'identity_source': 'advertisement',
            'history_supported': kind in ('D1', 'D2', 'D3'), 'supported': kind in ('D1', 'D2', 'D3'),
            'identification_supported': True}


async def ble_call(operation, *args, **kwargs):
    from bleak.exc import BleakError
    try:
        return await operation(*args, **kwargs)
    except (BleakError, TimeoutError) as exc:
        raise BluetoothFailure(f'蓝牙操作失败：{exc}。请断开手机 LiNK，检查蓝牙开关和终端权限，然后重试。') from exc


async def scan_snapshot(timeout=15):
    if timeout <= 0: raise ValueError('扫描时间必须大于 0')
    from bleak import BleakScanner
    events.emit('phase', phase='scanning')
    return await ble_call(BleakScanner.discover, timeout=timeout, return_adv=True)


async def scan(timeout=15):
    return candidates(await scan_snapshot(timeout))


def candidates(found):
    result = []
    for device, adv in found.values():
        item = advertisement(device, adv)
        if item is not None: result.append(item)
    # Same serial in two advertisements is ambiguous until GATT identifies them.
    for item in result:
        if item['serial'] and sum(r['serial'] == item['serial'] for r in result) > 1:
            item['identity_conflict'] = True
    return sorted(result, key=lambda r: r['rssi'] if r['rssi'] is not None else -999, reverse=True)


async def resolve(args):
    """Resolve a fresh BLEDevice by address or unambiguous advertised serial."""
    timeout = args.scan_timeout
    if timeout <= 0: raise ValueError('扫描时间必须大于 0')
    expected = getattr(args, 'serial', None)
    if expected is not None: expected = serial_number(expected)
    target = (getattr(args, 'ble_id', None) or '').strip().casefold()
    if not expected and not target: raise ValueError('请提供序列号或 BLE 地址')
    events.emit('phase', serial=expected, phase='scanning')
    snapshot = getattr(args, 'discovery', None)
    found = snapshot if snapshot is not None else await scan_snapshot(timeout)
    exact, by_serial = [], []
    for device, adv in found.values():
        if target and device.address.casefold() == target:
            exact.append(device)
        item = advertisement(device, adv)
        if item and expected and item['serial'] == expected:
            by_serial.append(device)
    candidates = exact or by_serial
    if len(candidates) > 1:
        raise DeviceMismatch('附近存在重复序列号或 BLE 地址，无法唯一选择设备；请按 BLE 地址逐台核验')
    if candidates:
        return candidates[0]
    if snapshot is not None:
        return await resolve(SimpleNamespace(serial=expected, ble_id=target, scan_timeout=timeout))
    raise BluetoothFailure(f'DROP {expected or target} not seen during a {timeout:g}s BLE scan. '
                           '断开手机 LiNK，靠近设备重试；自定义名称请先按 BLE 地址读取编号。No GATT commands were sent.')


async def read_identity(client, *, ble_id=None):
    """Only standard GATT reads; no notify subscriptions or proprietary writes."""
    from bleak.exc import BleakError
    values, errors = {}, {}
    for key, characteristic in (('model', MODEL_UUID), ('serial', SERIAL_UUID)):
        try:
            raw = await asyncio.wait_for(client.read_gatt_char(characteristic), timeout=15)
            text = bytes(raw).decode('utf-8', 'strict').strip('\x00 ').strip()
            if not text or '\x00' in text: raise ValueError('字段为空或包含内嵌空字节')
            values[key] = serial_number(text) if key == 'serial' else text
        except (BleakError, ValueError, UnicodeError, TimeoutError) as exc:
            errors[key] = str(exc)
    kind = model_type(values.get('model'))
    verified = bool(kind and values.get('serial') and not errors)
    return {'serial': values.get('serial'), 'model': values.get('model'),
            'name': values.get('model') or '未核验 DROP', 'ble_id': ble_id,
            'identity_verified': verified, 'identity_source': 'gatt', 'identity_errors': errors,
            **capabilities(values.get('model'))}


def require_identity(identity, *, expected=None, history=False):
    if not identity['identity_verified']:
        raise DeviceMismatch('无法读取并核验设备型号与序列号：' + str(identity.get('identity_errors', {})))
    if expected is not None and identity['serial'] != serial_number(expected):
        raise DeviceMismatch(f'Serial mismatch: expected {expected}, connected {identity["serial"]}. No command sent.')
    if history and not identity['history_supported']:
        raise UnsupportedModel(f'{identity["device_type"]} 已识别，历史记录格式尚未验证（unsupported model），未发送读取指令')


async def identify(ble_id=None, *, serial=None, timeout=15, device=None):
    if timeout <= 0: raise ValueError('扫描时间必须大于 0')
    from bleak import BleakClient
    if device is None:
        device = await resolve(SimpleNamespace(serial=serial, ble_id=ble_id, scan_timeout=timeout))
    async def connected():
        events.emit('phase', serial=serial, phase='identifying')
        async with BleakClient(device, timeout=30) as client:
            result = await read_identity(client, ble_id=device.address)
            if serial is not None and result.get('serial') and result['serial'] != serial_number(serial):
                raise DeviceMismatch('序列号与请求不符，未登记设备')
            return result
    return await ble_call(connected)
