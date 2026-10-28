"""Device library and operations shared by the CLI and terminal UI.

Only user-requested downloads touch BLE. Paths and device serials are validated
before proprietary commands. Downloads and imports publish exclusively through SQLite transactions.
"""
from __future__ import annotations

import asyncio
import csv
from datetime import datetime, timedelta
from contextlib import contextmanager, nullcontext, closing
from contextvars import ContextVar
import os
from pathlib import Path
import re
import sqlite3
import time
import sys
from types import SimpleNamespace
import uuid

from . import protocol, __version__
from .database import Store, FIELDS
from .storage import default_root, writer_lock, legacy_archives, migrate, configure
from . import events
from .errors import MigrationRequired, ConfigurationFailure, CompletionAckFailure, describe

from . import devices
from .devices import DROP_SERVICE, serial_number

_ble_call = devices.ble_call
_collection = ContextVar("collection", default=None)


async def discover(timeout=15, *, identify=False, register_to=None):
    snapshot = await devices.scan_snapshot(timeout)
    found = devices.candidates(snapshot)
    if identify or register_to is not None:
        for item in found:
            try:
                device = next(pair[0] for pair in snapshot.values() if pair[0].address == item['ble_id'])
                result = await devices.identify(item['ble_id'], timeout=timeout, device=device)
                item.update(result)
                if register_to is not None:
                    register_to.save_identity(result)
                    item['registered'] = True
            except (OSError, RuntimeError, ValueError) as exc:
                item['identity_error'] = str(exc)
    return found


class Library:
    def __init__(self, root=None):
        self.root = Path(root if root is not None else default_root()).expanduser().resolve()
        self.database_path = self.root / 'library.sqlite'
        self._store = None
        self.download_results = {}

    @property
    def store(self):
        if self._store is None:
            self._store = Store(self.database_path)
        return self._store

    def _writable_store(self):
        if not self.database_path.exists() and legacy_archives(self.root):
            raise MigrationRequired('发现旧 CSV 档案，请先运行 migrate；不会另建数据库绕过旧记录')
        self._store = Store(self.database_path, create=True)
        return self._store

    def query_store(self, serial):
        self.store.device(serial_number(serial))
        return self.store

    def database_info(self):
        return dict(self.store.check(), integrity='ok')

    def backup(self, destination=None):
        destination = destination or self.root / 'backups' / f'kestrel_{datetime.now():%Y%m%d-%H%M%S}_{uuid.uuid4().hex[:6]}.sqlite'
        return self.store.backup(destination)

    def archives(self):
        if not self.database_path.exists():
            return {}
        return {d['serial']: self.database_path for d in self.store.status() if d['database_records']}

    def status(self, serial=None):
        if not self.database_path.exists():
            if legacy_archives(self.root):
                raise MigrationRequired('发现旧 CSV 档案，请先运行 migrate 完成迁移')
            return []
        return self.store.status(serial)

    def targets(self):
        return [item['serial'] for item in self.status()]

    async def register(self, *, ble_id=None, serial=None, timeout=15):
        identity = await devices.identify(ble_id, serial=serial, timeout=timeout)
        devices.require_identity(identity, expected=serial)
        self.save_identity(identity)
        return identity

    def save_identity(self, identity):
        devices.require_identity(identity)
        with writer_lock(self.root):
            self._writable_store().register_device(identity['serial'], identity['model'])

    def import_archive(self, source, metadata=None):
        source = Path(source).expanduser().resolve()
        metadata = Path(metadata).expanduser().resolve() if metadata else Path(str(source) + '.json')
        # Validate legacy import on a copy; adoption must never modify the source.
        import tempfile
        import shutil
        with tempfile.TemporaryDirectory(prefix='kestrel-import-') as temporary:
            output = Path(temporary) / 'history.csv'
            shutil.copyfile(source, output)
            evidence = Path(str(output) + '.json')
            shutil.copyfile(metadata, evidence)
            state = protocol.adopt_archive(output, evidence)
            _, rows = protocol._rows_checked(output)
            with writer_lock(self.root):
                self._writable_store().import_rows(serial_number(state['serial']), state['model'], rows, state['next_cursor'],
                    {'source_sha256': state['csv_sha256'], 'metadata_sha256': protocol._sha256(evidence)})
        return self.database_path

    def timing(self, serial=None, *, interval=300, recent=100):
        from .timing import analyze
        serials=[serial_number(serial)] if serial else [row['serial'] for row in self.store.overview()]
        result=[]
        for item in serials:
            with closing(self.store.iter_query(item)) as records:
                evidence=analyze(records,interval,recent)
            evidence.update(serial=item,clock_actions=[action for action in self.store.actions(item,1000) if 'time' in action['requested']])
            result.append(evidence)
        return {'devices':result,'read_only':True,'expected_interval_seconds':interval}

    def complete_download(self, serial, previous=0, code=0, warning=None):
        info = next(iter(self.status(serial)), None)
        if info and info.get('error'):
            raise RuntimeError(info['error'])
        total = info['committed_records'] if info else 0
        outcome = {'serial': serial, 'status': 'pending' if code else 'empty' if not total else 'downloaded' if total > previous else 'up_to_date',
                   'added_records': total - previous, 'total_records': total,
                   'pending_records': info['pending_records'] if info else 0,
                   'superseded_records': info.get('superseded_records',0) if info else 0,
                   'resumable_records': info.get('resumable_records',0) if info else 0,
                   'first_timestamp': info['first_timestamp'] if info else None,
                   'last_timestamp': info['last_timestamp'] if info else None, 'code': code}
        if warning:
            outcome.update(status='saved_warning', warning=warning)
        self.download_results[serial] = outcome
        events.emit('completed', **outcome)
        return code

    async def download(self, serial, *, execute=False, ble_id=None, timeout=20.0, maxrows=20000, progress_factory=None, discovery=None):
        serial = serial_number(serial)
        if timeout <= 0 or maxrows < 1:
            raise ValueError('扫描时间和记录上限必须为正数')
        if not execute:
            print(f'预演：设备 {serial}；实际下载请加 --execute。未发送蓝牙指令。')
            return 0
        owner = _collection.get()
        if owner is not None and owner[0] is self:
            return await self._download_locked(serial, owner[1], ble_id=ble_id, timeout=timeout,
                                               maxrows=maxrows, progress_factory=progress_factory, discovery=discovery)
        with writer_lock(self.root):
            store = self._writable_store()
            return await self._download_locked(serial, store, ble_id=ble_id, timeout=timeout,
                                               maxrows=maxrows, progress_factory=progress_factory, discovery=discovery)

    async def _download_locked(self, serial, store, *, ble_id, timeout, maxrows, progress_factory, discovery):
        try:
            before = store.device(serial)['row_count']
        except FileNotFoundError:
            before = 0
        owner=_collection.get()
        args = SimpleNamespace(serial=serial, ble_id=ble_id, scan_timeout=timeout, discovery=discovery,
                               allow_rescan=not (owner is not None and owner[2]>1))
        from bleak import BleakClient
        device = await _ble_call(protocol.resolve_ble_device, args)
        events.emit('phase', serial=serial, phase='connecting')
        async def connected():
            async with BleakClient(device, timeout=30) as client:
                events.emit('phase', serial=serial, phase='identifying')
                identity = await devices.read_identity(client, ble_id=getattr(device, 'address', ble_id))
                devices.require_identity(identity, expected=serial, history=True)
                store.ensure_device(serial, devices.canonical_model(identity['model']))
                cursor, pending = store.resume(serial)
                session = protocol.BleSession(client, model=identity['model'])
                await client.start_notify(protocol.UP_UUID, session.callback)
                try:
                    events.emit('phase', serial=serial, phase='querying')
                    count = await session.query_count(cursor)
                    if not count:
                        return 3 if pending or any(d['pending_records'] or d.get('superseded_records') for d in store.status(serial)) else 0
                    if count > maxrows:
                        raise RuntimeError('设备报告的记录数超过 --maxrows；可能存在历史缺口，未继续读取')
                    transfer_id = store.begin_transfer(serial, cursor, count)
                    progress = (progress_factory or (lambda n: protocol.TransferProgress(n, log_seconds=0.5)))(count)
                    started = time.monotonic()
                    try:
                        events.emit('phase', serial=serial, phase='receiving')
                        def complete(rows, blocks, final):
                            events.emit('phase', serial=serial, phase='verifying')
                            events.emit('phase', serial=serial, phase='saving')
                            store.finish_transfer(transfer_id, next_cursor=final, elapsed_seconds=time.monotonic()-started,
                                                  maxrows=maxrows, received_rows=rows)
                        await session.download_from_cursor(cursor, count, maxrows, progress=progress,
                            on_block=lambda rows, blocks: store.append_block(transfer_id, rows, blocks), on_complete=complete)
                        progress.finish(success=True)
                    except CompletionAckFailure:
                        progress.finish(success=True)
                        raise
                    except BaseException:
                        progress.finish(success=False, partial_saved=True)
                        raise
                    return 0
                finally:
                    primary_error=sys.exc_info()[0] is not None
                    try: await client.stop_notify(protocol.UP_UUID)
                    except Exception:
                        if not primary_error: raise
        warning = None
        try:
            result = await _ble_call(connected)
        except CompletionAckFailure as exc:
            result, warning = 0, describe(exc)
        return self.complete_download(serial, previous=before, code=result, warning=warning)

    async def device_operation(self, serial, *, ble_id=None, timeout=20, changes=None, execute=False, blink=False, samples=0, period=2, allow_schedule_shift=False):
        from . import control
        serial = serial_number(serial)
        if timeout <= 0 or not 0 <= samples <= 10000 or not 0.2 <= period <= 3600:
            raise ValueError('设备查询参数超出有效范围')
        if changes is not None:
            changes = control.validate_changes(changes)
            if not execute: return {'serial':serial,'status':'dry_run','changes':changes,'sampling_risk':control.clock_risk(changes),'note':'未访问蓝牙；--execute 后逐项写入并回读验证'}
        if changes and 'time' in changes and allow_schedule_shift is not True:
            raise ValueError('校时可能改变采样相位；请先查看 timing 报告，确认风险后使用 --allow-schedule-shift')
        if blink and not execute: return {'serial':serial,'status':'dry_run','action':'blink','note':'--execute 后闪灯定位'}
        timings = {}
        started = time.monotonic()
        # One directory owner prevents simultaneous download/configuration of a device.
        owner = _collection.get()
        with nullcontext() if owner is not None and owner[0] is self else writer_lock(self.root):
            device = await devices.resolve(SimpleNamespace(serial=serial,ble_id=ble_id,scan_timeout=timeout))
            timings['locating'] = time.monotonic() - started
            from bleak import BleakClient
            events.emit('phase',serial=serial,phase='connecting')
            async def connected():
                connection_started = time.monotonic()
                async with BleakClient(device,timeout=30) as client:
                    timings['connecting'] = time.monotonic() - connection_started
                    identity_started = time.monotonic()
                    events.emit('phase',serial=serial,phase='identifying')
                    identity = await devices.read_identity(client,ble_id=device.address)
                    devices.require_identity(identity,expected=serial)
                    timings['identity'] = time.monotonic() - identity_started
                    if changes and 'reference_altitude_m' in changes and identity['device_type']!='D3':
                        raise ValueError('参考海拔用于 D3 气压计算，此型号不适用；未发送设置指令')
                    if samples:
                        values=[]
                        for index in range(samples):
                            sample={'serial':serial,**await control.live(client)}
                            events.emit('sample',**sample);values.append(sample)
                            if index+1 < samples: await asyncio.sleep(period)
                        return {'serial':serial,'samples':values,'stored':False}
                    read_started = time.monotonic()
                    before=await control.inspect(client)
                    timings['read_before'] = time.monotonic() - read_started
                    result={**identity,**before}
                    if changes is None and not blink: return result
                    audit_started = time.monotonic()
                    store=self._writable_store()
                    store.register_device(serial,identity['model'])
                    action=store.begin_action(serial,changes or {'blink':True},result)
                    timings['audit'] = time.monotonic() - audit_started
                    write_started = time.monotonic()
                    try:
                        if blink:
                            payload=bytearray(20);payload[1]=1
                            await devices.gatt_wait(client.write_gatt_char(control.COMMANDS_UUID,bytes(payload),response=True),15)
                            after={'command_acknowledged':True,'visual_confirmation_required':True}
                        else: after=await control.apply(client,changes,before,allow_schedule_shift=allow_schedule_shift)
                        timings['write_verify'] = time.monotonic() - write_started
                        audit_started = time.monotonic()
                        store.finish_action(action,after=after)
                        timings['audit'] += time.monotonic() - audit_started
                        return {'serial':serial,'status':'verified','action_id':action,'before':result,'after':after}
                    except BaseException as exc:
                        store.finish_action(action,error=f'{type(exc).__name__}: {exc}; device may have changed; read back before retry')
                        if isinstance(exc, asyncio.CancelledError): raise
                        raise ConfigurationFailure(str(exc)) from exc
            result = await _ble_call(connected)
            timings['total'] = time.monotonic() - started
            result['timings_seconds'] = timings
            return result

    @contextmanager
    def collection(self, execute, *, parallel=1):
        if not execute:
            yield
            return
        with writer_lock(self.root):
            store=self._writable_store()
            token=_collection.set((self,store,parallel))
            try: yield
            finally: _collection.reset(token)

    def records(self, serial, limit=30):
        if limit < 1: raise ValueError('条数必须大于 0')
        return self.query_store(serial).query(serial_number(serial), limit=limit)

    def export(self, serial, destination, *, since=None, until=None):
        start = datetime.fromisoformat(since) if since else None
        end = datetime.fromisoformat(until) if until else None
        if any(value and value.tzinfo for value in (start, end)):
            raise ValueError('设备时间没有时区，请输入不带时区的本地时间')
        if start and end and start > end: raise ValueError('开始时间不能晚于结束时间')
        end_exclusive = bool(until and re.fullmatch(r'\d{4}-\d{2}-\d{2}', until))
        if end_exclusive: end += timedelta(days=1)
        selected = self.query_store(serial).iter_query(serial_number(serial), since=start.isoformat(sep=' ') if start else None,
            until=end.isoformat(sep=' ') if end else None, end_exclusive=end_exclusive)
        names = ['serial', 'history_generation', 'absolute_cursor', 'device_sequence', *FIELDS]
        destination = Path(destination).expanduser().resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        import tempfile
        fd, temporary = tempfile.mkstemp(prefix='.kestrel-export-', suffix='.csv', dir=destination.parent)
        temporary = Path(temporary)
        try:
            count = 0
            with os.fdopen(fd, 'w', newline='', encoding='utf-8-sig') as stream, closing(selected):
                writer = csv.DictWriter(stream, fieldnames=names)
                writer.writeheader()
                for row in selected:
                    writer.writerow(row)
                    count += 1
                stream.flush(); os.fsync(stream.fileno())
            os.link(temporary, destination)
            protocol._fsync_directory(destination.parent)
        finally:
            temporary.unlink(missing_ok=True)
        return count


def parser():
    import argparse
    ap = argparse.ArgumentParser(description='Kestrel DROP 多设备识别与 D1/D2/D3 下载管理（默认打开中文 TUI）')
    ap.add_argument('--version', action='version', version=f'%(prog)s {__version__}')
    ap.add_argument('--data-dir', default=None, help='数据目录；优先使用已保存位置或 KESTREL_DATA_DIR')
    ap.add_argument('--events-json', action='store_true', help='输出版本化 JSON Lines 事件，供 TUI / 自动化使用')
    sub = ap.add_subparsers(dest='command')
    sub.add_parser('tui', help='交互式终端界面')
    scan = sub.add_parser('scan', help='扫描附近 DROP；默认只看广播，可选核验/登记')
    scan.add_argument('--timeout', type=float, default=15, help='广播扫描秒数，默认 15')
    scan.add_argument('--identify', action='store_true', help='逐台连接，只读核验型号与序列号')
    scan.add_argument('--register', action='store_true', help='逐台核验并登记到本地设备库，不下载历史')
    for name, help_text in (('identify', '只读查询型号与序列号'), ('register', '只读核验并登记设备')):
        identity = sub.add_parser(name, help=help_text)
        selection = identity.add_mutually_exclusive_group(required=True)
        selection.add_argument('--ble-id')
        selection.add_argument('--serial', type=serial_number)
        identity.add_argument('--timeout', type=float, default=15)
    sub.add_parser('list', help='本地档案状态（JSON）')
    sub.add_parser('db', help='只读完整检查：数据库、原始记录与恢复边界')
    sub.add_parser('migrate', help='备份、验证并迁移旧 CSV/SQLite 档案')
    location = sub.add_parser('configure', help='保存今后使用的数据位置；不搬动已有数据')
    location.add_argument('directory', type=Path)
    restore = sub.add_parser('restore', help='验证备份并恢复到当前空数据目录')
    restore.add_argument('backup', type=Path)
    backup = sub.add_parser('backup', help='生成一致的 SQLite 数据备份，不覆盖已有文件')
    backup.add_argument('output', nargs='?', type=Path, help='默认保存在数据目录/backups/')
    status = sub.add_parser('status', help='查看指定设备或所有档案状态')
    status.add_argument('serial', nargs='?', type=serial_number)
    download = sub.add_parser('download', help='首次完整下载或自动增量同步')
    target = download.add_mutually_exclusive_group(required=True)
    target.add_argument('--serial', type=serial_number)
    target.add_argument('--serials', nargs='+', type=serial_number, help='按列表采集多个设备，可设置并发数')
    target.add_argument('--all', action='store_true', help='处理全部登记设备，含空档案和首次中断；失败后继续')
    download.add_argument('--ble-id', help='单设备 BLE 地址或 UUID')
    download.add_argument('--ble-ids', nargs='+', help='与 --serials 一一对应的临时连接地址；- 表示按编号寻找')
    download.add_argument('--timeout', type=float, default=20)
    download.add_argument('--maxrows', type=int, default=20000)
    download.add_argument('--parallel', type=int, default=1, help='并发连接数 1–8；建议从 2 开始')
    download.add_argument('--execute', action='store_true', help='执行设备历史读取指令')
    sub.add_parser('overview', help='关联设备、条数、游标和最近下载时间的只读总览')
    timing=sub.add_parser('timing',help='只读检查原始采样时间、相位与校时审计，不访问蓝牙')
    timing.add_argument('serial',type=serial_number,nargs='?')
    timing.add_argument('--interval',type=int,default=300,help='预期采样间隔秒数，默认300；是诊断假设，不修改设备')
    timing.add_argument('--recent',type=int,default=100)
    audit=sub.add_parser('actions',help='只读查看设备配置审计记录')
    audit.add_argument('serial',type=serial_number)
    audit.add_argument('--limit',type=int,default=20)
    for command, help_text in (('device','读取设备设置、时间、电量与固件'),('set','配置设备，默认预演'),('blink','闪灯定位设备，默认预演'),('live','读取实时测量，不入历史库')):
        device = sub.add_parser(command,help=help_text)
        device.add_argument('serial',type=serial_number)
        device.add_argument('--ble-id')
        device.add_argument('--timeout',type=float,default=20)
        if command in ('set','blink'): device.add_argument('--execute',action='store_true')
        if command=='set':
            device.add_argument('--interval',type=int)
            device.add_argument('--wrap',choices=('on','off'))
            device.add_argument('--update-seconds',type=int)
            device.add_argument('--reference-altitude',type=int)
            device.add_argument('--name')
            device.add_argument('--allow-schedule-shift',action='store_true',help='确认校时可能改变采样相位；不代表恢复整点采样')
            device.add_argument('--time',help='设备本地时间 ISO 格式，或 now 使用电脑当前本地时间')
        if command=='live':
            device.add_argument('--samples',type=int,default=1)
            device.add_argument('--period',type=float,default=2)
    batch = sub.add_parser('set-batch', help='明确选择多台设备逐台配置，默认预演')
    batch.add_argument('--serials', nargs='+', type=serial_number, required=True)
    batch.add_argument('--ble-ids', nargs='+')
    batch.add_argument('--timeout', type=float, default=20)
    batch.add_argument('--execute', action='store_true')
    batch.add_argument('--interval', type=int)
    batch.add_argument('--wrap', choices=('on','off'))
    batch.add_argument('--update-seconds', type=int)
    batch.add_argument('--allow-schedule-shift',action='store_true',help='确认逐台校时可能使机群采样相位不同')
    batch.add_argument('--time', help='now 在每台实际写入时取电脑本地时间')
    imp = sub.add_parser('import', help='验证旧完整 CSV 与 .csv.json 并入库，不改源文件')
    imp.add_argument('csv', type=Path)
    imp.add_argument('--metadata', type=Path)
    export = sub.add_parser('export', help='导出 CSV，可按设备本地时间筛选，不覆盖文件')
    export.add_argument('serial', type=serial_number)
    export.add_argument('output', type=Path)
    export.add_argument('--since', help='含起点，例如 2026-10-22 或 "2026-10-22 12:00:00"')
    export.add_argument('--until', help='含终点；只写日期时包含当天整天')
    records = sub.add_parser('records', help='查看最新记录（JSON）')
    records.add_argument('serial', type=serial_number)
    records.add_argument('--limit', type=int, default=30)
    return ap


def main(argv=None):
    args = parser().parse_args(argv)
    if args.events_json:
        with events.stream(sys.stdout):
            code = _run(args)
            events.emit('finished', code=code)
            return code
    return _run(args)


def _run(args):
    try:
        if args.command == 'configure':
            events.result({'data_dir': str(configure(args.directory)), 'note': '位置已保存；已有数据未搬动'})
            return 0
        library = Library(args.data_dir)
        if args.command in (None, 'tui'):
            if args.events_json:
                raise ValueError('--events-json 仅供非交互子命令使用；打开 TUI 请移除此选项')
            from .tui import run
            return run(library)
        if args.command == 'scan':
            result = asyncio.run(discover(args.timeout, identify=args.identify, register_to=library if args.register else None))
            if (args.identify or args.register) and any(item.get('identity_error') or not item.get('identity_verified') for item in result):
                from .errors import DeviceMismatch
                for item in result:
                    if item.get('identity_error') or not item.get('identity_verified'):
                        events.emit('error', serial=item.get('serial'), ble_id=item['ble_id'],
                            **describe(DeviceMismatch(item.get('identity_error') or str(item.get('identity_errors')))))
                events.result(result)
                return 2
        elif args.command == 'identify':
            result = asyncio.run(devices.identify(args.ble_id, serial=args.serial, timeout=args.timeout))
            if not result['identity_verified']:
                from .errors import DeviceMismatch
                events.emit('error', **describe(DeviceMismatch('设备型号或编号读取未通过核验：' + str(result['identity_errors']))))
                events.result(result)
                return 2
        elif args.command == 'register':
            result = asyncio.run(library.register(ble_id=args.ble_id, serial=args.serial, timeout=args.timeout))
        elif args.command in ('list', 'status'):
            result = library.status()
            if getattr(args, 'serial', None):
                matches = [item for item in result if item['serial'] == args.serial]
                if not matches:
                    raise FileNotFoundError(f'设备 {args.serial} 尚无档案')
                result = matches[0]
            if (isinstance(result, dict) and result.get('error')) or (isinstance(result, list) and any(item.get('error') for item in result)):
                for item in result if isinstance(result, list) else [result]:
                    if item.get('error'):
                        events.emit('error', serial=item['serial'], **describe(RuntimeError(item['error'])))
                events.result(result)
                return 2
        elif args.command == 'migrate':
            result = migrate(library.root)
        elif args.command == 'restore':
            from .storage import restore
            result = restore(args.backup, library.root)
        elif args.command == 'db':
            result = library.database_info()
        elif args.command == 'backup':
            result = {'backup': str(library.backup(args.output))}
        elif args.command == 'import':
            result = {'database': str(library.import_archive(args.csv, args.metadata))}
        elif args.command == 'export':
            result = {'rows': library.export(args.serial, args.output, since=args.since, until=args.until),
                      'output': str(args.output.expanduser().resolve())}
        elif args.command == 'records':
            result = library.records(args.serial, args.limit)
        elif args.command == 'actions':
            result=library.store.actions(args.serial,args.limit)
        elif args.command == 'timing':
            result=library.timing(args.serial,interval=args.interval,recent=args.recent)
        elif args.command == 'overview':
            result=library.store.overview()
        elif args.command in ('device','set','blink','live'):
            changes=None
            if args.command=='set':
                changes={key:value for key,value in [('interval_seconds',args.interval),('wrap',None if args.wrap is None else args.wrap=='on'),
                    ('update_seconds',args.update_seconds),('reference_altitude_m',args.reference_altitude),('name',args.name),('time',args.time)] if value is not None}
            if args.command=='live' and args.samples < 1: raise ValueError('实时样本数必须为 1–10000')
            result=asyncio.run(library.device_operation(args.serial,ble_id=args.ble_id,timeout=args.timeout,
                changes=changes,execute=getattr(args,'execute',False),blink=args.command=='blink',
                samples=getattr(args,'samples',0),period=getattr(args,'period',2),
                allow_schedule_shift=getattr(args,'allow_schedule_shift',False)))
        elif args.command == 'set-batch':
            from .batch_settings import targets, configure as configure_batch
            changes = {key: value for key, value in (
                ('interval_seconds', args.interval),
                ('wrap', None if args.wrap is None else args.wrap == 'on'),
                ('update_seconds', args.update_seconds), ('time', args.time)) if value is not None}
            selected = targets(args.serials, args.ble_ids)
            result = asyncio.run(configure_batch(library, selected, changes,
                                                 execute=args.execute, timeout=args.timeout,allow_schedule_shift=args.allow_schedule_shift))
            events.result(result)
            return 2 if result['incomplete'] else 0
        elif args.command == 'download':
            if not 1 <= args.parallel <= 8: raise ValueError('并发连接数必须为 1–8')
            if args.ble_ids and (not args.serials or len(args.ble_ids) != len(args.serials)):
                raise ValueError('--ble-ids 必须与 --serials 数量相同')
            if (args.all or args.serials) and args.ble_id:
                raise ValueError('--ble-id 只能用于指定单个 --serial')
            requested = library.targets() if args.all else args.serials or [args.serial]
            addresses = args.ble_ids or [args.ble_id] * len(requested)
            targets = {}
            for serial, address in zip(requested, addresses):
                address = None if address == '-' else address
                if serial in targets and targets[serial] != address:
                    raise ValueError('同一编号指定了不同连接地址，请先核验设备身份')
                targets[serial] = address
            used_addresses = [v.casefold() for v in targets.values() if v]
            if len(used_addresses) != len(set(used_addresses)):
                raise ValueError('不同编号不能共用同一个 BLE 地址')
            serials = list(targets)
            if not serials:
                raise ValueError('没有本地档案；先 scan，再使用 download --serial 建立档案')
            failures = []
            outcomes = []
            async def batch():
                snapshot, scan_error = None, None
                if args.execute and len(serials) > 1:
                    try:
                        snapshot = await devices.scan_snapshot(args.timeout,
                            addresses=list(targets.values()) if all(targets.values()) else None)
                    except (OSError, RuntimeError, ValueError) as exc:
                        scan_error = exc
                semaphore=asyncio.Semaphore(args.parallel)
                stopped=asyncio.Event()
                async def worker(serial):
                    async with semaphore:
                        if stopped.is_set(): raise asyncio.CancelledError()
                        try:
                            await collect(serial)
                        except BaseException:
                            stopped.set()
                            raise
                async def collect(serial):
                    try:
                        if scan_error is not None:
                            raise scan_error
                        with events.context(serial=serial):
                            code = await library.download(serial, execute=args.execute, ble_id=targets[serial],
                                                          timeout=args.timeout, maxrows=args.maxrows, discovery=snapshot)
                        outcomes.append(library.download_results.get(serial, {'serial': serial, 'code': code, 'status': 'dry_run'}))
                        if code:
                            failures.append({'serial': serial, 'code': code})
                            events.emit('error', serial=serial, code='completion_pending', retryable=True,
                                        message='部分暂存数据缺少设备完成确认，尚未发布。', action='等待设备产生新样本后重试，保留数据库。')
                    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                        if asyncio.current_task().cancelling(): raise asyncio.CancelledError() from exc
                        failure = {'serial': serial, **describe(exc)}
                        failures.append(failure)
                        outcomes.append({'serial': serial, 'status': 'failed', 'error': failure})
                        events.emit('error', **failure)
                        print(f'设备 {serial} 失败：{exc}', file=__import__('sys').stderr)
                with library.collection(args.execute,parallel=args.parallel):
                    tasks=[asyncio.create_task(worker(serial)) for serial in serials]
                    try: await asyncio.gather(*tasks)
                    finally:
                        for task in tasks:
                            if not task.done(): task.cancel()
                        await asyncio.gather(*tasks,return_exceptions=True)
                # Return a stable result order even when transfers finish out of order.
                outcomes.sort(key=lambda item: serials.index(item['serial']))
            asyncio.run(batch())
            events.result({'downloads': outcomes, 'incomplete': failures})
            if failures:
                return 2
            return 0
        events.result(result)
        return 0
    except (KeyboardInterrupt, asyncio.CancelledError):
        events.emit('error', **describe(KeyboardInterrupt()))
        print('\n已取消；已落盘的数据会保留。', file=__import__('sys').stderr)
        return 130
    except (OSError, RuntimeError, ValueError, KeyError, ImportError, sqlite3.Error) as exc:
        events.emit('error', **describe(exc))
        print(f'错误：{exc}', file=__import__('sys').stderr)
        return 2
