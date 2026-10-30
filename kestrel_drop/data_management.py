"""Explicit single-device data maintenance. Never confuse local history with device flash."""
import asyncio
from types import SimpleNamespace

from . import devices,protocol,events
from .database import Store,utcnow
from .devices import serial_number
from .errors import ConfigurationFailure
from .storage import writer_lock


def confirm(serial,confirmation,execute):
    serial=serial_number(serial)
    if execute and confirmation!=serial:raise ValueError('清除操作必须完整输入目标序列号确认；不支持批量')
    return serial


def local_history(library,serial,*,execute=False,confirmation=None,revision=None):
    serial=confirm(serial,confirmation,execute)
    if not execute:return {**library.store.data_info(serial),'status':'dry_run','action':'clear_local_history'}
    if not revision:raise ValueError('请先读取data-info，并使用最新revision确认本地清除范围')
    with writer_lock(library.root):
        store=library._writable_store()
        if store.data_info(serial)['revision']!=revision:raise ValueError('本地档案已变化，请刷新数据管理页后重新确认')
        events.emit('phase',serial=serial,phase='backing_up')
        backup=library.backup()
        events.emit('phase',serial=serial,phase='clearing_local')
        return store.clear_local_history(serial,backup=backup,revision=revision)


async def device_count(library,serial,*,ble_id=None,timeout=20):
    serial=serial_number(serial)
    if timeout<=0:raise ValueError('超时须为正数')
    with writer_lock(library.root):
        device=await devices.resolve(SimpleNamespace(serial=serial,ble_id=ble_id,scan_timeout=timeout))
        from bleak import BleakClient
        async def connected():
            async with BleakClient(device,timeout=30) as client:
                identity=await devices.read_identity(client,ble_id=device.address)
                devices.require_identity(identity,expected=serial,history=True)
                session=protocol.BleSession(client,model=identity['model'])
                await devices.gatt_wait(client.start_notify(protocol.UP_UUID,session.callback),15)
                try:
                    raw=await session.read_command(2)
                    if len(raw)!=4:raise RuntimeError('设备记录数格式无效')
                    return {'serial':serial,'device_records':int.from_bytes(raw,'little'),'read_utc':utcnow(),'read_only':True}
                finally:
                    import sys
                    primary=sys.exc_info()[0] is not None
                    try:await devices.gatt_wait(client.stop_notify(protocol.UP_UUID),10)
                    except Exception:
                        if not primary:raise
        return await devices.ble_call(connected)


async def erase_device(library,serial,*,execute=False,confirmation=None,revision=None,ble_id=None,timeout=20):
    serial=confirm(serial,confirmation,execute)
    if timeout<=0:raise ValueError('超时须为正数')
    if not execute:return {'serial':serial,'status':'dry_run','action':'erase_device','note':'将先同步并备份电脑档案，再清除设备日志；电脑历史、编号和代号保留'}
    if not revision:raise ValueError('请先读取data-info，再使用最新revision确认清除目标')
    with writer_lock(library.root):
        store=library._writable_store()
        if store.data_info(serial)['revision']!=revision:raise ValueError('本地档案已变化，请刷新数据管理页后重新确认')
        device=await devices.resolve(SimpleNamespace(serial=serial,ble_id=ble_id,scan_timeout=timeout))
        from bleak import BleakClient
        async def connected():
            async with BleakClient(device,timeout=30) as client:
                identity=await devices.read_identity(client,ble_id=device.address)
                devices.require_identity(identity,expected=serial,history=True)
                # Shared verified downloader: same connection, contiguous durable commits, no reconnect gap.
                code=await library._sync_client(client,store,identity)
                if code:raise RuntimeError('同步尚有未确认数据，未发送任何清除命令')
                events.emit('phase',serial=serial,phase='backing_up')
                backup=library.backup()
                before=store.data_info(serial)
                snapshot=Store(backup)
                if snapshot.path==store.path or snapshot.data_info(serial)['revision']!=before['revision']:
                    raise ValueError('独立备份与同步后的档案不符，未发送清除命令')
                action=store.begin_action(serial,{'erase_device_log':True,'backup':str(backup)},before)
                session=protocol.BleSession(client,model=identity['model'])
                attempted=False
                try:
                    await devices.gatt_wait(client.start_notify(protocol.UP_UUID,session.callback),15)
                    try:
                        attempted=True
                        events.emit('phase',serial=serial,phase='erasing_device')
                        await session.write_command(1)
                        events.emit('phase',serial=serial,phase='checking_device_count')
                        raw=await session.read_command(2)
                        if len(raw)!=4:raise RuntimeError('擦除后记录数格式无效')
                        remaining=int.from_bytes(raw,'little')
                        if remaining:raise RuntimeError(f'擦除后读到{remaining}条，可能已产生新记录；不自动重试，请独立读取核对')
                    finally:
                        import sys
                        primary=sys.exc_info()[0] is not None
                        try:await devices.gatt_wait(client.stop_notify(protocol.UP_UUID),10)
                        except Exception:
                            if not primary:raise
                    result={'serial':serial,'status':'erased_device','device_records':0,'backup':str(backup),
                            'computer_records':before['records'],'verified_utc':utcnow(),
                            'note':'电脑历史与游标保留；设备序号按官方生命周期连续，不重置本地游标；未停采或校时'}
                    store.finish_action(action,after=result)
                    return result
                except BaseException as exc:
                    audit_error=None
                    try:store.finish_action(action,error=f'{type(exc).__name__}: {exc}; erase_attempted={attempted}; do not retry; inspect device count first')
                    except Exception as failed_audit:audit_error=str(failed_audit)
                    if isinstance(exc,(asyncio.CancelledError,KeyboardInterrupt)):raise
                    raise ConfigurationFailure(('设备可能已清除，请先核对' if attempted else '未发送设备清除命令')+f'；电脑数据与备份保留：{backup}。不自动重试。{exc}'+(f'；审计未能完成：{audit_error}' if audit_error else '')) from exc
        return await devices.ble_call(connected)
