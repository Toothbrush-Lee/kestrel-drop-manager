"""Official DROP GATT settings and live measurements (355204 Rev AC, App. C).

Settings writes preserve every unrequested byte. Device identity is checked by
Library before any write. Erase, defaults, security and firmware are not exposed.
"""
from datetime import datetime

from .errors import TransferError

BATTERY_UUID = '00002a19-0000-1000-8000-00805f9b34fb'
FIRMWARE_UUID = '00002a26-0000-1000-8000-00805f9b34fb'
HARDWARE_UUID = '00002a27-0000-1000-8000-00805f9b34fb'
INTERVALS = (2, 5, 10, 20, 30, 60, 120, 300, 360, 600, 1200, 1800, 3600, 7200, 21600, 43200)


def uuid(code):
    return f'1263{code:04x}-cc25-497d-9854-9b6c02c77054'


SETTINGS_UUID = uuid(0x101)
COMMANDS_UUID = uuid(0x102)
NAME_UUID = uuid(0x103)
TIME_UUID = uuid(0x104)


def decode_time(raw):
    if len(raw) != 7: raise ValueError('设备时间字段必须为 7 字节')
    return datetime(int.from_bytes(raw[:2], 'little'), *raw[2:]).isoformat(sep=' ')


def encode_time(value):
    if not isinstance(value,str): raise ValueError('设备时间必须为 ISO 格式字符串')
    stamp = datetime.fromisoformat(value)
    if stamp.tzinfo is not None: raise ValueError('设备使用本地时间，请不要附加时区或 Z')
    if stamp.microsecond: raise ValueError('设备时间精度为秒，不接受小数秒')
    return stamp.year.to_bytes(2, 'little') + bytes((stamp.month, stamp.day, stamp.hour, stamp.minute, stamp.second))


def decode_link_time(raw):
    # DROP 1.61 Get Time returns an aligned uint64; the eighth byte is zero.
    if len(raw) not in (7,8) or (len(raw)==8 and raw[7]!=0):
        raise ValueError('Get Time 返回必须为 7 字节时间或带零填充的 8 字节时间')
    second,minute,hour,day,month=raw[:5]
    return datetime(int.from_bytes(raw[5:7],'little'),month,day,hour,minute,second).isoformat(sep=' ')


def encode_link_time(value):
    standard=encode_time(value)
    return bytes((standard[6],standard[5],standard[4],standard[3],standard[2]))+standard[:2]


async def clock(client, value=None):
    from . import protocol
    session=protocol.BleSession(client,model='D1')
    import asyncio
    await asyncio.wait_for(client.start_notify(protocol.UP_UUID,session.callback),15)
    try:
        if value is not None: await session.write_command(15,encode_link_time(value))
        raw=await session.read_command(14)
        return decode_link_time(raw),raw.hex()
    finally:
        import sys
        primary_error=sys.exc_info()[0] is not None
        try: await asyncio.wait_for(client.stop_notify(protocol.UP_UUID),10)
        except Exception:
            if not primary_error: raise


def decode_settings(raw):
    if len(raw) != 20: raise ValueError(f'设备设置字段应为 20 字节，实际 {len(raw)}')
    interval = int.from_bytes(raw[:3], 'little')
    reference = int.from_bytes(raw[7:9], 'little', signed=True)
    if raw[3] not in (0, 1, 255): raise ValueError('无效的滚动覆盖设置')
    return {'interval_seconds': None if interval == 0xffffff else interval,
            'wrap': None if raw[3] == 255 else bool(raw[3]),
            'update_seconds': None if raw[4] == 255 else raw[4],
            'security_level': None if raw[5] == 255 else raw[5],
            'setup_complete': None if raw[6] == 255 else bool(raw[6]),
            'reference_altitude_m': None if reference == -32767 else reference}


def validate_changes(changes):
    allowed = {'interval_seconds', 'wrap', 'update_seconds', 'reference_altitude_m', 'name', 'time'}
    if not changes or set(changes) - allowed: raise ValueError('未选择有效的设备配置项')
    if 'interval_seconds' in changes and (type(changes['interval_seconds']) is not int or changes['interval_seconds'] not in INTERVALS):
        raise ValueError('不支持的 DROP 采样间隔；可选秒数：' + ', '.join(map(str, INTERVALS)))
    if 'wrap' in changes and type(changes['wrap']) is not bool: raise ValueError('滚动覆盖只能为 on 或 off')
    if 'update_seconds' in changes and (type(changes['update_seconds']) is not int or not 2 <= changes['update_seconds'] <= 60):
        raise ValueError('实时更新间隔必须为 2–60 秒')
    if 'reference_altitude_m' in changes and (type(changes['reference_altitude_m']) is not int or not -32766 <= changes['reference_altitude_m'] <= 32767):
        raise ValueError('参考海拔超出有效的 int16 范围')
    if 'name' in changes:
        name = changes['name']
        if not isinstance(name, str) or not name or not name.isascii() or any(ord(c) < 32 or ord(c) > 126 for c in name) or len(name.encode('ascii')) > 20:
            raise ValueError('设备名称必须为 1–20 个可打印 ASCII 字符')
    if 'time' in changes and changes['time'] != 'now': encode_time(changes['time'])
    return dict(changes)


def settings_payload(raw, changes):
    old = decode_settings(raw)
    payload = bytearray(raw)
    for field in ('interval_seconds', 'wrap', 'update_seconds', 'reference_altitude_m'):
        if field in changes and old[field] is None: raise ValueError(f'此设备不支持配置 {field}')
    if 'interval_seconds' in changes: payload[:3] = changes['interval_seconds'].to_bytes(3, 'little')
    if 'wrap' in changes: payload[3] = int(changes['wrap'])
    if 'update_seconds' in changes: payload[4] = changes['update_seconds']
    if 'reference_altitude_m' in changes: payload[7:9] = changes['reference_altitude_m'].to_bytes(2, 'little', signed=True)
    return bytes(payload)


async def inspect(client):
    from bleak.exc import BleakError
    result, errors = {}, {}
    fields = [('settings', SETTINGS_UUID, decode_settings),
              ('name', NAME_UUID, lambda b: b.rstrip(b'\0').decode('ascii')),
              ('battery_pct', BATTERY_UUID, lambda b: b[0] if len(b)==1 and b[0]<=100 else None),
              ('firmware', FIRMWARE_UUID, lambda b: b.rstrip(b'\0').decode('ascii')),
              ('hardware', HARDWARE_UUID, lambda b: b.rstrip(b'\0').decode('ascii'))]
    for name, characteristic, decode in fields:
        try:
            import asyncio
            raw = bytes(await asyncio.wait_for(client.read_gatt_char(characteristic), 15))
            result[name] = decode(raw)
            if name == 'settings': result['settings_raw_hex'] = raw.hex()
        except (BleakError, ValueError, UnicodeError, TimeoutError, IndexError) as exc:
            errors[name] = str(exc)
            result[name] = None
    try:
        result['device_time'],result['device_time_wire_hex']=await clock(client)
        result['device_time_source']='link_command_14'
    except (BleakError, ValueError, TransferError, TimeoutError) as exc:
        result['device_time']=None;errors['device_time']=str(exc)
    result['read_errors'] = errors
    return result


async def apply(client, changes, before):
    """Write individually, verify each field, never retry an ambiguous write."""
    changes = validate_changes(changes)
    if before.get('settings') is None and any(k in changes for k in ('interval_seconds','wrap','update_seconds','reference_altitude_m')):
        raise TransferError('未读到设备当前设置，拒绝覆盖')
    import asyncio
    async def write(characteristic, payload):
        await asyncio.wait_for(client.write_gatt_char(characteristic, payload, response=True), 15)
    if any(k in changes for k in ('interval_seconds','wrap','update_seconds','reference_altitude_m')):
        payload = settings_payload(bytes.fromhex(before['settings_raw_hex']), changes)
        await write(SETTINGS_UUID, payload)
        actual_raw=bytes(await asyncio.wait_for(client.read_gatt_char(SETTINGS_UUID),15))
        actual = decode_settings(actual_raw)
        for field in changes.keys() & actual.keys():
            if actual[field] != changes[field]: raise TransferError(f'{field} 写入后回读不一致；请重新读取，不自动重试')
        if actual_raw!=payload: raise TransferError('设置中未选择的字节发生变化；请重新读取核对')
    if 'name' in changes:
        await write(NAME_UUID, changes['name'].encode('ascii').ljust(20, b'\0'))
        actual = bytes(await asyncio.wait_for(client.read_gatt_char(NAME_UUID), 15)).rstrip(b'\0').decode('ascii')
        if actual != changes['name']: raise TransferError('设备名称写入后回读不一致')
    if 'time' in changes:
        value = datetime.now().astimezone().replace(tzinfo=None, microsecond=0).isoformat(sep=' ') if changes['time']=='now' else changes['time']
        actual,_ = await clock(client,value)
        delta = (datetime.fromisoformat(actual)-datetime.fromisoformat(value)).total_seconds()
        if not 0 <= delta <= 10: raise TransferError('设备时间写入后回读偏差过大，请重新读取')
    after = await inspect(client)
    if 'time' in changes: after['time_write_target_local'] = value
    return after


MEASUREMENTS = (
    ('temperature_c', 1, 2, True, 100),
    ('relative_humidity_pct', 2, 2, False, 100),
    ('heat_index_c', 3, 3, True, 100),
    ('dew_point_c', 4, 2, True, 100),
    ('station_pressure_hpa', 7, 2, False, 10),
    ('density_altitude_m', 10, 3, True, 100),
)


async def live(client):
    from bleak.exc import BleakError
    import asyncio
    values, status, errors = {}, {}, {}
    for field, code, width, signed, divisor in MEASUREMENTS:
        try:
            raw = bytes(await asyncio.wait_for(client.read_gatt_char(uuid(code)), 10))
            if len(raw) != width+1: raise ValueError('实时测量长度与官方格式不一致')
            status[field] = {'present': bool(raw[0]&1), 'enabled':bool(raw[0]&2), 'valid':bool(raw[0]&4)}
            values[field] = int.from_bytes(raw[1:], 'little', signed=signed)/divisor if raw[0]&7==7 else None
        except (BleakError, ValueError, TimeoutError) as exc:
            values[field] = None; errors[field] = str(exc)
    return {'computer_time':datetime.now().astimezone().isoformat(timespec='seconds'),
            'measurements':values,'measurement_status':status,'read_errors':errors}
