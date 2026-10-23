"""Device library and operations shared by the CLI and terminal UI.

Only user-requested downloads touch BLE. Paths and device serials are validated
before proprietary commands. First imports are published as whole directories.
"""
from __future__ import annotations

import asyncio
import csv
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import sys
from types import SimpleNamespace
import uuid

from . import protocol
from .database import Store
from . import events
from .errors import BluetoothFailure, describe

DEFAULT_ROOT = Path(__file__).resolve().parents[1] / 'archive'
DROP_SERVICE = '12630000-cc25-497d-9854-9b6c02c77054'


async def _ble_call(operation, *args, **kwargs):
    from bleak.exc import BleakError
    try:
        return await operation(*args, **kwargs)
    except BleakError as exc:
        raise BluetoothFailure(f'蓝牙操作失败：{exc}。请断开手机 LiNK，检查蓝牙开关和终端权限，然后重试。') from exc


def serial_number(value: str) -> str:
    value = str(value).strip()
    if not re.fullmatch(r'[0-9]{1,20}', value):
        raise ValueError('序列号必须为 1–20 位数字')
    return value


async def discover(timeout: float = 15) -> list[dict]:
    if timeout <= 0:
        raise ValueError('扫描时间必须大于 0')
    from bleak import BleakScanner
    events.emit('phase', phase='scanning')
    found = await _ble_call(BleakScanner.discover, timeout=timeout, return_adv=True)
    devices = []
    for device, adv in found.values():
        name = (adv.local_name or device.name or '').strip()
        match = re.fullmatch(r'D3\s*-\s*([0-9]{1,20})', name, re.I)
        services = {x.lower() for x in adv.service_uuids or []}
        if match or name.lower().startswith(('d2', 'd3', 'drop', 'kestrel')) or DROP_SERVICE in services:
            devices.append({'serial': match[1] if match else None,
                            'name': name or '未命名 DROP 候选设备',
                            'ble_id': device.address, 'rssi': adv.rssi,
                            'supported': bool(match)})
    return sorted(devices, key=lambda d: d['rssi'] if d['rssi'] is not None else -999, reverse=True)


class Library:
    def __init__(self, root=DEFAULT_ROOT):
        self.root = Path(root).expanduser().resolve()
        self.database_path = self.root / 'library.sqlite'
        self._store = None
        self.download_results = {}

    def complete_download(self, serial, previous=0, code=0):
        if serial in self.archives():
            info = self.index_archive(serial)
            device = self.store.device(serial)
            outcome = {'serial': serial, 'status': 'pending' if code else 'downloaded' if info['committed_records'] > previous else 'up_to_date',
                       'added_records': info['committed_records'] - previous, 'total_records': info['committed_records'],
                       'pending_records': info['pending_records'], 'first_timestamp': device['first_timestamp'],
                       'last_timestamp': device['last_timestamp'], 'code': code}
        else:
            outcome = {'serial': serial, 'status': 'empty', 'added_records': 0, 'total_records': 0,
                       'pending_records': 0, 'code': code}
        self.download_results[serial] = outcome
        events.emit('completed', **outcome)
        return code

    @property
    def store(self) -> Store:
        if self._store is None:
            self._store = Store(self.database_path)
        return self._store

    def index_archive(self, serial: str) -> dict:
        """Replay committed CSV into SQLite while holding the transport lock."""
        output = self.path(serial)
        events.emit('phase', serial=serial, phase='archive_check')
        with protocol._archive_lock(output):
            info = protocol._archive_info_locked(output)
            state = protocol._state_loaded(protocol._archive_paths(output))
            names, rows = protocol._rows_checked(output)
            events.emit('phase', serial=serial, phase='indexing')
            info.update(self.store.ingest(output, state, names, rows))
        return info

    def query_store(self, serial: str) -> Store:
        serial = serial_number(serial)
        if serial in self.archives():
            self.index_archive(serial)
        elif not self.database_path.exists():
            raise FileNotFoundError(f'设备 {serial} 尚无档案，请先下载或导入')
        self.store.device(serial)
        return self.store

    def database_info(self) -> dict:
        statuses = self.status()
        errors = [f'{item["serial"]}: {item["error"]}' for item in statuses if item.get('error')]
        if errors:
            raise RuntimeError('部分档案未能校验入库：' + '; '.join(errors))
        return self.store.summary()

    def backup(self, destination: Path | None = None) -> Path:
        self.database_info()
        destination = destination or self.root / 'backups' / f'kestrel_{datetime.now():%Y%m%d-%H%M%S}_{uuid.uuid4().hex[:6]}.sqlite'
        return self.store.backup(destination)

    def archives(self) -> dict[str, Path]:
        """Also discover the earlier release's flat archive layout."""
        result = {}
        if not self.root.exists():
            return result
        for path in sorted(self.root.glob('*.csv.sync-state.json')) + sorted(self.root.glob('*/history.csv.sync-state.json')):
            state = json.loads(path.read_text(encoding='utf-8'))
            serial = serial_number(state['serial'])
            if serial in result:
                raise RuntimeError(f'序列号 {serial} 存在两个档案，请先检查：{result[serial]} / {path}')
            result[serial] = Path(str(path).removesuffix('.sync-state.json'))
        return result

    def path(self, serial: str) -> Path:
        serial = serial_number(serial)
        try:
            return self.archives()[serial]
        except KeyError:
            raise FileNotFoundError(f'设备 {serial} 尚无档案，请先下载或导入历史 CSV') from None

    def status(self) -> list[dict]:
        result = []
        for serial, path in self.archives().items():
            try:
                result.append(self.index_archive(serial))
            except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                result.append({'serial': serial, 'csv': str(path), 'error': str(exc)})
        known = {item['serial'] for item in result}
        if self.database_path.exists():
            for device in self.store.summary()['devices']:
                if device['serial'] not in known:
                    result.append({'serial': device['serial'], 'committed_records': device['row_count'],
                                   'database_records': device['row_count'], 'next_cursor': device['next_cursor'],
                                   'database': str(self.database_path),
                                   'note': '仅有数据库副本，可预览导出；继续下载需完整恢复档案目录'})
                    known.add(device['serial'])
        for directory in sorted((self.root / '.downloads').glob('*')):
            if not directory.is_dir() or not re.fullmatch(r'[0-9]{1,20}', directory.name):
                continue
            attempts = self.attempts(directory.name)
            if attempts and directory.name not in known:
                result.append({'serial': directory.name, 'initial_attempts': len(attempts),
                               'initial_partial_files': [str(p) for p in attempts],
                               'note': '首次下载中断；原始记录保留，再次下载从最早可读记录开始'})
        return result

    def attempts(self, serial: str) -> list[Path]:
        return sorted((self.root / '.downloads' / serial_number(serial)).glob('*/history.csv.partial.csv'))

    def _attempt(self, serial: str, category='downloads') -> Path:
        directory = self.root / f'.{category}' / serial / (datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + uuid.uuid4().hex[:8])
        directory.mkdir(parents=True)
        return directory / 'history.csv'

    def _publish(self, serial: str, output: Path) -> Path:
        state_path = protocol._archive_paths(output)['state']
        if not state_path.exists():
            protocol.adopt_archive(output, Path(str(output) + '.json'))
        info = protocol.archive_info(output)
        if info['serial'] != serial:
            raise RuntimeError('下载档案序列号与目标设备不同，拒绝发布')
        target = self.root / serial
        if target.exists() or serial in self.archives():
            raise FileExistsError(f'档案已存在，拒绝覆盖：{target}')
        os.rename(output.parent, target)
        self.index_archive(serial)
        return target / 'history.csv'

    def import_archive(self, source: Path, metadata: Path | None = None) -> Path:
        source = Path(source).expanduser().resolve()
        metadata = Path(metadata).expanduser().resolve() if metadata else Path(str(source) + '.json')
        meta = json.loads(metadata.read_text(encoding='utf-8'))
        serial = serial_number(meta['serial'])
        self.root.mkdir(parents=True, exist_ok=True)
        with protocol._archive_lock(self.root / f'.device-{serial}'):
            if serial in self.archives():
                raise FileExistsError(f'设备 {serial} 已有档案；导入不会覆盖已有记录')
            output = self._attempt(serial, category='imports')
            shutil.copyfile(source, output)
            shutil.copyfile(metadata, Path(str(output) + '.json'))
            return self._publish(serial, output)

    async def download(self, serial: str, *, execute=False, ble_id=None,
                       timeout=20.0, maxrows=6220, progress_factory=None) -> int:
        serial = serial_number(serial)
        if timeout <= 0 or maxrows < 1:
            raise ValueError('扫描时间和记录上限必须为正数')
        if not execute:
            print(f'预演：设备 {serial}；实际下载请加 --execute。未发送蓝牙指令。')
            return 0
        self.root.mkdir(parents=True, exist_ok=True)
        # Serial lock protects first publication and prevents two first downloads.
        with protocol._archive_lock(self.root / f'.device-{serial}'):
            archives = self.archives()
            args = SimpleNamespace(serial=serial, ble_id=ble_id, execute=True,
                                   scan_timeout=timeout, maxrows=maxrows)
            args.progress_factory = progress_factory or (lambda count: protocol.TransferProgress(count, log_seconds=0.5))
            if serial in archives:
                before = self.index_archive(serial)['committed_records']
                args.output = str(archives[serial])
                result = await _ble_call(protocol.sync_live, args)
                return self.complete_download(serial, previous=before, code=result)
            if self.database_path.exists():
                if any(item['serial'] == serial for item in self.store.summary()['devices']):
                    raise RuntimeError('数据库中已有该设备，但缺少下载恢复档案。请恢复整个 archive 备份后再同步。')
            # A completed initial transfer can be adopted/published after a crash
            # without contacting the sensor or downloading the same rows again.
            for candidate in sorted((self.root / '.downloads' / serial).glob('*/history.csv')):
                if Path(str(candidate) + '.json').exists():
                    print(f'恢复已完成的首次下载：{self._publish(serial, candidate)}')
                    return self.complete_download(serial)
            attempts = self.attempts(serial)
            if attempts:
                print(f'保留 {len(attempts)} 份首次下载中断数据。绝对起始游标尚未确认，本次重新读取完整历史。')
            output = self._attempt(serial)
            args.output, args.cmd, args.cursor = str(output), 'live', 0
            result = await _ble_call(protocol.live_read, args)
            if output.exists():
                print(f'档案已建立：{self._publish(serial, output)}')
            else:
                print('设备没有可下载记录；尚未建立档案。')
            return self.complete_download(serial, code=result)

    def records(self, serial: str, limit=30) -> list[dict]:
        if limit < 1:
            raise ValueError('条数必须大于 0')
        return self.query_store(serial).query(serial_number(serial), limit=limit)

    def export(self, serial: str, destination: Path, *, since=None, until=None) -> int:
        start = datetime.fromisoformat(since) if since else None
        end = datetime.fromisoformat(until) if until else None
        if any(value and value.tzinfo for value in (start, end)):
            raise ValueError('设备时间没有时区，请输入不带时区的本地时间')
        if start and end and start > end:
            raise ValueError('开始时间不能晚于结束时间')
        end_exclusive = bool(until and re.fullmatch(r'\d{4}-\d{2}-\d{2}', until))
        if end_exclusive:
            end += timedelta(days=1)
        store = self.query_store(serial)
        selected = store.query(serial_number(serial), since=start.isoformat(sep=' ') if start else None,
                               until=end.isoformat(sep=' ') if end else None, end_exclusive=end_exclusive)
        names = ['serial', 'absolute_cursor', *json.loads(store.device(serial)['fieldnames_json'])]
        destination = Path(destination).expanduser().resolve()
        destination.parent.mkdir(parents=True, exist_ok=True)
        # Exclusive creation: no existing CSV, state or evidence can be overwritten.
        with destination.open('x', newline='', encoding='utf-8-sig') as fd:
            writer = csv.DictWriter(fd, fieldnames=names)
            writer.writeheader()
            writer.writerows(selected)
            fd.flush(); os.fsync(fd.fileno())
        return len(selected)


def parser():
    import argparse
    ap = argparse.ArgumentParser(description='Kestrel DROP D3 一站式下载管理（默认打开中文 TUI）')
    ap.add_argument('--data-dir', default=str(DEFAULT_ROOT), help='档案目录，默认项目 archive/')
    ap.add_argument('--events-json', action='store_true', help='输出版本化 JSON Lines 事件，供 TUI / 自动化使用')
    sub = ap.add_subparsers(dest='command')
    sub.add_parser('tui', help='交互式终端界面')
    scan = sub.add_parser('scan', help='扫描附近 DROP，不连接或写入设备')
    scan.add_argument('--timeout', type=float, default=15)
    sub.add_parser('list', help='本地档案状态（JSON）')
    sub.add_parser('db', help='校验所有档案并事务入库，显示 SQLite 状态')
    backup = sub.add_parser('backup', help='生成一致的 SQLite 数据备份，不覆盖已有文件')
    backup.add_argument('output', nargs='?', type=Path, help='默认保存在 archive/backups/')
    status = sub.add_parser('status', help='查看指定设备或所有档案状态')
    status.add_argument('serial', nargs='?', type=serial_number)
    download = sub.add_parser('download', help='首次完整下载或自动增量同步')
    target = download.add_mutually_exclusive_group(required=True)
    target.add_argument('--serial', type=serial_number)
    target.add_argument('--all', action='store_true', help='依次同步全部本地档案；单设备失败后继续')
    download.add_argument('--ble-id', help='未命名候选设备的 BLE 地址或 UUID')
    download.add_argument('--timeout', type=float, default=20)
    download.add_argument('--maxrows', type=int, default=6220)
    download.add_argument('--execute', action='store_true', help='执行设备历史读取指令')
    imp = sub.add_parser('import', help='复制并验证已有完整 CSV 与 .csv.json，不改源文件')
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
    library = Library(args.data_dir)
    try:
        if args.command in (None, 'tui'):
            if args.events_json:
                raise ValueError('--events-json 仅供非交互子命令使用；打开 TUI 请移除此选项')
            from .tui import run
            return run(library)
        if args.command == 'scan':
            result = asyncio.run(discover(args.timeout))
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
        elif args.command == 'db':
            result = library.database_info()
        elif args.command == 'backup':
            result = {'backup': str(library.backup(args.output))}
        elif args.command == 'import':
            result = {'csv': str(library.import_archive(args.csv, args.metadata))}
        elif args.command == 'export':
            result = {'rows': library.export(args.serial, args.output, since=args.since, until=args.until),
                      'output': str(args.output.expanduser().resolve())}
        elif args.command == 'records':
            result = library.records(args.serial, args.limit)
        elif args.command == 'download':
            if args.all and args.ble_id:
                raise ValueError('--ble-id 只能用于指定单个 --serial')
            serials = list(library.archives()) if args.all else [args.serial]
            if not serials:
                raise ValueError('没有本地档案；先 scan，再使用 download --serial 建立档案')
            failures = []
            outcomes = []
            async def batch():
                for serial in serials:
                    try:
                        with events.context(serial=serial):
                            code = await library.download(serial, execute=args.execute, ble_id=args.ble_id,
                                                          timeout=args.timeout, maxrows=args.maxrows)
                        outcomes.append(library.download_results.get(serial, {'serial': serial, 'code': code, 'status': 'dry_run'}))
                        if code:
                            failures.append({'serial': serial, 'code': code})
                            events.emit('error', serial=serial, code='completion_pending', retryable=True,
                                        message='部分暂存数据缺少设备完成确认，尚未发布。', action='等待设备产生新样本后重试，保留 pending 文件。')
                    except (OSError, RuntimeError, ValueError, sqlite3.Error) as exc:
                        failure = {'serial': serial, **describe(exc)}
                        failures.append(failure)
                        outcomes.append({'serial': serial, 'status': 'failed', 'error': failure})
                        events.emit('error', **failure)
                        print(f'设备 {serial} 失败：{exc}', file=__import__('sys').stderr)
            asyncio.run(batch())
            events.result({'downloads': outcomes, 'incomplete': failures})
            if failures:
                return 2
            return 0
        events.result(result)
        return 0
    except KeyboardInterrupt:
        events.emit('error', **describe(KeyboardInterrupt()))
        print('\n已取消；已落盘的数据会保留。', file=__import__('sys').stderr)
        return 130
    except (OSError, RuntimeError, ValueError, KeyError, ImportError, sqlite3.Error) as exc:
        events.emit('error', **describe(exc))
        print(f'错误：{exc}', file=__import__('sys').stderr)
        return 2
