"""Responsive curses UI. BLE work runs in a cancellable child process.

No background thread writes to curses; output is delivered through a queue.
The child receives SIGINT on cancellation and exits through protocol cleanup.
"""
from __future__ import annotations

from collections import deque
import curses
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import threading
import time
import unicodedata

from . import __version__
from .protocol import TransferProgress
from .errors import TITLES
from .devices import model_type

PHASES = {'scanning': '扫描附近设备', 'connecting': '连接设备', 'identifying': '核对型号和序列号',
          'querying': '查询新增记录', 'receiving': '接收并保存连续数据块', 'verifying': '验证完整性和完成游标',
          'saving': '发布下载档案', 'archive_check': '检查本地档案', 'indexing': '校验并入库'}
STATUSES = {'downloaded': '已保存', 'up_to_date': '已经是最新', 'pending': '有数据尚未确认',
            'empty': '设备暂无记录', 'failed': '未完成', 'dry_run': '仅预演'}


def clip(text, width):
    """Clip by terminal cells instead of splitting Chinese wide characters."""
    result, used = [], 0
    for char in str(text):
        if unicodedata.category(char).startswith('C'):
            char = ' '
        size = 0 if unicodedata.combining(char) else (2 if unicodedata.east_asian_width(char) in 'WF' else 1)
        if used + size > width:
            break
        result.append(char)
        used += size
    return ''.join(result)


def merge_devices(current, incoming, *, scan=False):
    """Merge transient locators without conflating serials or losing archives."""
    result = {key: dict(value) for key, value in current.items()
              if not scan or (value.get('serial') and '@' not in key)}
    if scan:
        for item in result.values():
            item['nearby'] = False
            item.pop('identity_conflict', None)
    for observation in incoming:
        item = dict(observation)
        serial, address = item.get('serial'), item.get('ble_id')
        if scan: item['nearby'] = True
        if serial and address:
            for key, old in list(result.items()):
                if old.get('ble_id') == address and not old.get('serial'):
                    del result[key]
                elif old.get('ble_id') == address and old.get('serial') != serial:
                    if old.get('identity_verified') or old.get('database'):
                        old['ble_id'], old['nearby'] = None, False
                    else:
                        del result[key]
        key = serial or 'ble:' + str(address)
        old = result.get(key, {})
        if (serial and address and old.get('nearby') and old.get('ble_id')
                and old['ble_id'] != address):
            old['identity_conflict'] = item['identity_conflict'] = True
            key = serial + '@' + address
            old = result.get(key, {})
        # A broadcast must not downgrade a GATT-verified model to a name hint.
        if old.get('identity_verified') and not item.get('identity_verified'):
            if item.get('device_type') not in (None, '未知', old.get('device_type')):
                item['identity_conflict'] = True
            for field in ('model','device_type','history_supported','supported','identity_verified'):
                if field in old: item[field] = old[field]
        result[key] = {**old, **item}
    return result


class Job:
    def __init__(self, root, arguments):
        self.events = queue.Queue()
        self.cancelled = False
        self.arguments = list(arguments)
        self.payload = None
        self.failures = []
        self.outcomes = []
        self.protocol_error = False
        self.finished = False
        self.process = subprocess.Popen(
            [sys.executable, '-u', str(Path(__file__).resolve().parents[1] / 'kestrel.py'),
             '--data-dir', str(root), '--events-json', *arguments],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            encoding='utf-8', errors='replace', bufsize=1,
            env={**os.environ, 'PYTHONUNBUFFERED': '1'},
        )
        self.thread = threading.Thread(target=self._read, daemon=True)
        self.thread.start()

    def _read(self):
        def errors():
            try:
                for line in self.process.stderr:
                    self.events.put(('stderr', line.rstrip()))
            finally:
                self.process.stderr.close()
        error_thread = threading.Thread(target=errors, daemon=True)
        error_thread.start()
        try:
            for line in self.process.stdout:
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict) or event.get('version') != 1 or event.get('event') not in (
                            'phase', 'progress', 'log', 'error', 'completed', 'result', 'sample', 'finished'):
                        raise ValueError('unknown event schema')
                    if event['event'] == 'finished':
                        self.finished = True
                    self.events.put(('event', event))
                except (ValueError, TypeError):
                    self.protocol_error = True
                    self.events.put(('stderr', '后台事件格式错误：' + line.rstrip()))
        finally:
            self.process.stdout.close()
            code = self.process.wait()
            error_thread.join()
            self.events.put(('exit', 2 if code == 0 and (self.protocol_error or not self.finished) else code))

    def cancel(self):
        if self.process.poll() is None:
            self.cancelled = True
            try:
                self.process.send_signal(signal.SIGINT)
            except ProcessLookupError:
                pass  # The child may exit between poll() and send_signal().

    def close(self):
        self.cancel()
        try:
            self.process.wait(timeout=8)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait()
        self.thread.join(timeout=1)


class Terminal:
    def __init__(self, screen, library):
        self.screen, self.library = screen, library
        self.devices = {}
        self.selected = 0
        self.marked = set()
        self.logs = deque(maxlen=500)
        self.job = None
        self.action = None
        self.message = 's 扫描附近设备 · n 输入序列号 · i 导入历史 CSV'
        self.progress = ''
        self.form = None
        self.preview = None
        self.preview_offset = 0
        self.quit_after_cancel = False
        self.refresh_message = None
        self.text_view = None
        self.text_offset = 0
        self.result_view = None
        self.device_state = None
        self.device_progress = {}
        self.parallel = 1
        self.last_stamp = None
        self.list_stamp = None
        self.next_local_refresh = 0

    def database_stamp(self):
        result=[]
        for name in ('library.sqlite','library.sqlite-wal'):
            try:
                stat=(self.library.root/name).stat();result.append((stat.st_mtime_ns,stat.st_size))
            except FileNotFoundError: result.append(None)
        return tuple(result)

    def refresh_if_changed(self):
        if time.monotonic() < self.next_local_refresh: return
        self.next_local_refresh=time.monotonic()+2
        if self.job or self.form or self.preview is not None or self.text_view is not None or self.result_view is not None or self.device_state is not None: return
        if self.last_stamp is not None and self.database_stamp()!=self.last_stamp:
            self.refresh_message='本地数据已更新（自动刷新）'
            self.start('list',['list'])

    def start(self, action, arguments):
        if self.job:
            self.message = '任务执行中；按 c 取消，或等待完成'
            return
        if action=='list': self.list_stamp=self.database_stamp()
        if action=='download': self.device_progress={}
        self.action = action
        self.progress = ''
        self.job = Job(self.library.root, arguments)
        self.message = {'scan': '正在扫描蓝牙广播…', 'list': '正在读取本地档案…',
                        'download': '正在连接设备；c 可取消并保留已落盘数据…'}.get(action, '正在处理…')
        self.logs.append('> ' + ' '.join(arguments))

    def poll(self):
        if not self.job:
            return
        while True:
            try:
                kind, value = self.job.events.get_nowait()
            except queue.Empty:
                return
            if kind == 'event':
                self.receive_event(value)
                continue
            if kind == 'stderr':
                self.logs.append(value)
                continue
            job, action = self.job, self.action
            self.job = None
            self.action = None
            self.message = ('已取消；已落盘数据保留，可再次下载' if job.cancelled else
                            '操作完成' if value == 0 else f'操作未完成（退出码 {value}），请查看下方日志')
            if (value == 0 or (action in ('list', 'scan') and value == 2)) and action in ('scan', 'list', 'records', 'backup', 'export', 'import', 'migrate', 'configure', 'register', 'identify', 'device', 'set', 'blink', 'live'):
                try:
                    payload = job.payload
                    if payload is None:
                        raise ValueError(job.failures[0]['action'] if job.failures else '后台未返回结果')
                    if action == 'scan':
                        selected = self.selected_device()
                        selected_serial = selected.get('serial') if selected else None
                        self.devices = merge_devices(self.devices, payload, scan=True)
                        self.selected = min(self.selected, max(0, len(self.devices)-1))
                        if selected_serial and selected_serial in self.devices:
                            self.selected = list(self.devices).index(selected_serial)
                        self.message = f'扫描完成：{len(payload)} 个 DROP；x 读取编号并登记 · 空格选择 · a 批量采集'
                    elif action == 'list':
                        self.last_stamp=self.list_stamp
                        # Clear transient errors and old pending counts on refresh.
                        for device in self.devices.values():
                            for field in ('csv', 'error', 'committed_records', 'pending_records',
                                          'last_timestamp', 'initial_attempts', 'initial_partial_files', 'note',
                                          'database', 'database_records', 'database_next_cursor', 'database_added_records'):
                                device.pop(field, None)
                        self.devices = merge_devices(self.devices, payload)
                        self.selected = min(self.selected, max(0, len(self.devices)-1))
                        failures = sum(bool(item.get('error')) for item in payload)
                        self.message = (f'{failures} 个档案校验或入库失败，请查看选中设备及日志' if failures else
                                        self.refresh_message or f'已读取 {len(payload)} 个设备 · s 扫描 · Enter 下载')
                        self.refresh_message = None
                    elif action in ('device','set'):
                        self.device_state = payload if action=='device' else {**payload['before'],**payload['after']}
                        self.message = '设置已回读验证' if action=='set' else '设备信息已读取；不会自动校时'
                    elif action == 'blink':
                        self.message = '已发送闪灯定位，请观察设备 LED'
                    elif action == 'live':
                        self.device_state = self.device_state or {'serial':payload['serial']}
                        self.device_state['live'] = payload['samples'][-1]
                        self.message='实时测量已读取，不写入历史库'
                    elif action == 'records':
                        self.preview, self.preview_offset = list(reversed(payload)), 0
                    elif action in ('register', 'identify'):
                        self.devices = merge_devices(self.devices, [payload])
                        serial = payload.get('serial')
                        if serial and serial in self.devices:
                            self.selected = list(self.devices).index(serial)
                        self.message = (f'{payload["device_type"]} · {serial} 已核验登记；'
                                        + ('Enter 下载' if payload['history_supported'] else '历史格式待适配，可继续管理其他设备'))
                    elif action == 'backup':
                        self.message = f'数据库备份完成：{payload["backup"]}（按 l 查看完整路径）'
                    elif action == 'migrate':
                        self.message = f'迁移完成：{payload["records"]} 条，旧材料保留在 legacy/（按 l 查看详情）'
                    elif action == 'configure':
                        from .manager import Library
                        self.library = Library(payload['data_dir'])
                        self.devices = {}
                        self.marked.clear()
                        self.message = '数据位置已保存；已有数据未搬动'
                    elif action == 'export':
                        self.message = f'已导出 {payload["rows"]} 条：{payload["output"]}（按 l 查看完整路径）'
                    else:
                        self.message = f'已导入完整档案：{payload["database"]}（按 l 查看完整路径）'
                except (ValueError, TypeError, KeyError) as exc:
                    self.message = f'无法读取任务结果：{exc}'
            if action in ('download', 'import', 'migrate', 'configure', 'register', 'identify', 'set'):
                self.refresh_message = self.message
                self.start('list', ['list'])
            if action == 'download' or (value and action != 'list'):
                outcomes = (job.payload or {}).get('downloads', job.outcomes) if isinstance(job.payload, dict) else job.outcomes
                failures = job.failures or ([{'code': 'cancelled', 'message': '任务已取消，已落盘数据保留。',
                                            'retryable': True, 'action': '可稍后重新下载。'}] if job.cancelled else [])
                if value and not failures:
                    failures = [{'code': 'worker_failed', 'message': '后台任务未完成。', 'retryable': False,
                                 'action': '按 l 查看日志，检查后台启动或事件通信问题。'}]
                if action in ('set','blink'):
                    for failure in failures: failure['retryable']=False
                self.result_view = {'outcomes': outcomes, 'failures': failures, 'code': value,
                                    'action': action, 'arguments': job.arguments}
                if len(outcomes) == 1 and outcomes[0]['serial'] in self.devices:
                    self.selected = list(self.devices).index(outcomes[0]['serial'])
            return

    def receive_event(self, event):
        kind = event['event']
        if event.get('serial') and kind in ('phase','progress','completed','error'):
            self.device_progress.setdefault(event['serial'],{}).update(event)
        if kind == 'log':
            self.logs.append(event['message'])
        elif kind == 'phase':
            self.message = f'{event.get("serial", "")} · {PHASES.get(event["phase"], event["phase"])}'
        elif kind == 'progress':
            self.progress = (f'{event["received"]:,}/{event["expected"]:,} 条  {event["rate"]:.1f} 条/秒  '
                             f'已用 {TransferProgress.duration(event["elapsed"])}  '
                             f'预计剩余 {TransferProgress.duration(event["eta"])}')
        elif kind == 'result':
            self.job.payload = event['data']
            if self.action in ('export', 'backup', 'import', 'migrate', 'configure', 'register', 'identify'):
                self.logs.append('结果：' + json.dumps(event['data'], ensure_ascii=False))
        elif kind == 'completed':
            for item in self.devices.values():
                if item.get('serial')==event['serial']:
                    item.update(database_records=event['total_records'],committed_records=event['total_records'],
                                pending_records=event['pending_records'],last_timestamp=event.get('last_timestamp'))
            self.job.outcomes.append(event)
            self.logs.append(f'{event["serial"]}: {STATUSES.get(event["status"], event["status"])}，'
                             f'新增 {event["added_records"]}，累计 {event["total_records"]}，未确认 {event["pending_records"]}')
        elif kind == 'error':
            self.job.failures.append(event)
            self.logs.append(f'{event["code"]}: {event["message"]}；{event["action"]}')

    def selected_device(self):
        values = list(self.devices.values())
        return values[min(self.selected, len(values) - 1)] if values else None

    def open_form(self, title, fields, action):
        self.form = {'title': title, 'fields': [[label, str(value)] for label, value in fields],
                     'index': 0, 'action': action, 'error': ''}

    def submit(self):
        values = [field[1].strip() for field in self.form['fields']]
        action = self.form['action']
        try:
            if action=='parallel':
                value=int(values[0])
                if not 1 <= value <= 8: raise ValueError('并发连接数必须为 1–8，建议从 2 开始')
                self.parallel=value;self.message=f'本次批量并发上限：{value}'
            elif action.startswith('set_'):
                from .control import validate_changes
                mapping={'set_interval':('interval_seconds','--interval',int),'set_wrap':('wrap','--wrap',lambda v: v=='on' if v in ('on','off') else None),
                    'set_name':('name','--name',str),'set_time':('time','--time',str),'set_update':('update_seconds','--update-seconds',int),
                    'set_altitude':('reference_altitude_m','--reference-altitude',int)}
                field,flag,convert=mapping[action]
                validate_changes({field:convert(values[0])})
                args=['set',self.device_state['serial'],flag,values[0],'--execute']
                if self.device_state.get('ble_id'): args += ['--ble-id',self.device_state['ble_id']]
                self.start('set',args)
            elif action == 'add':
                from .manager import serial_number
                serial = serial_number(values[0])
                self.devices[serial] = {**self.devices.get(serial, {}), 'serial': serial,
                                        'ble_id': values[1] or None, 'name': f'DROP - {serial}'}
                self.selected = list(self.devices).index(serial)
                self.message = '设备已添加；Enter 下载并在连接后核验序列号'
            elif action == 'import':
                if not values[0]:
                    raise ValueError('请输入 CSV 路径')
                self.start('import', ['import', values[0]] + (['--metadata', values[1]] if values[1] else []))
            elif action == 'configure':
                if not values[0]:
                    raise ValueError('请输入数据目录')
                self.start('configure', ['configure', values[0]])
            elif action == 'export':
                if not values[0]:
                    raise ValueError('请输入导出路径')
                args = ['export', self.selected_device()['serial'], values[0]]
                if values[1]:
                    args += ['--since', values[1]]
                if values[2]:
                    args += ['--until', values[2]]
                self.start('export', args)
            self.form = None
        except (ValueError, OSError) as exc:
            self.form['error'] = str(exc)

    def handle_form(self, key):
        form = self.form
        if key == '\x1b':
            self.form = None
        elif key in ('\t', curses.KEY_DOWN):
            form['index'] = (form['index'] + 1) % len(form['fields'])
        elif key in (curses.KEY_BTAB, curses.KEY_UP):
            form['index'] = (form['index'] - 1) % len(form['fields'])
        elif key in ('\n', '\r', curses.KEY_ENTER):
            if form['index'] + 1 < len(form['fields']):
                form['index'] += 1
            else:
                self.submit()
        elif key in ('\b', '\x7f', curses.KEY_BACKSPACE):
            form['fields'][form['index']][1] = form['fields'][form['index']][1][:-1]
        elif key == '\x15':
            form['fields'][form['index']][1] = ''
        elif isinstance(key, str) and key.isprintable():
            form['fields'][form['index']][1] += key

    def handle(self, key):
        if self.job and key=='c':
            self.job.cancel()
            self.message='正在取消；配置可能部分生效，请在取消后重新读取' if self.action in ('set','blink') else '正在取消，等待连接清理'
            return True
        if self.form:
            self.handle_form(key)
            return True
        if self.device_state is not None and self.result_view is None:
            if key in ('q','\x1b','\n','\r'):
                if not self.job: self.device_state=None
                return True
            if self.job: return True
            state=self.device_state;settings=state.get('settings') or {}
            forms={'1':('采样间隔：输入秒数（2,5,10,20,30,60,120,300,360,600,1200,1800,3600,7200,21600,43200）',settings.get('interval_seconds') or 300,'set_interval'),
                   '2':('滚动覆盖：on 覆盖最早记录 / off 满后停止记录','on' if settings.get('wrap') else 'off','set_wrap'),
                   '3':('校时：输入设备本地时间或 now；不会修改旧记录','now','set_time'),
                   '4':('设备名称：1–20 个可打印 ASCII 字符',state.get('name') or '','set_name'),
                   '5':('实时更新间隔：2–60 秒',settings.get('update_seconds') or 30,'set_update'),
                   '6':('气压计算参考海拔：米',settings.get('reference_altitude_m') or 0,'set_altitude')}
            if key=='6' and (state.get('device_type') or model_type(state.get('model')))!='D3':
                self.message='此型号没有气压测量，无需配置参考海拔'
            elif key in forms:
                title,value,action=forms[key];self.open_form(title,[('新值（Enter 写入并回读）',value)],action)
            elif key in ('r','w','b'):
                action={'r':'device','w':'live','b':'blink'}[key]
                args=[action,state['serial']]
                if state.get('ble_id'):args+=['--ble-id',state['ble_id']]
                if action=='blink':args+=['--execute']
                self.start(action,args)
            return True
        if self.text_view is not None:
            if key in ('q', '\x1b', '\n'):
                self.text_view = None
            elif key in (curses.KEY_DOWN, curses.KEY_NPAGE):
                self.text_offset = min(max(0, len(self.text_view) - 1), self.text_offset + (10 if key == curses.KEY_NPAGE else 1))
            elif key in (curses.KEY_UP, curses.KEY_PPAGE):
                self.text_offset = max(0, self.text_offset - (10 if key == curses.KEY_PPAGE else 1))
            return True
        if self.preview is not None:
            if key in ('q', '\x1b', '\n'):
                self.preview = None
            elif key == curses.KEY_DOWN:
                self.preview_offset = min(max(0, len(self.preview) - 1), self.preview_offset + 1)
            elif key == curses.KEY_UP:
                self.preview_offset = max(0, self.preview_offset - 1)
            return True
        if self.result_view is not None:
            view = self.result_view
            if key in ('\n', '\r', '\x1b', 'q'):
                self.result_view = None
            elif key == 'r' and any(failure.get('retryable') for failure in view['failures']):
                if not self.job:
                    self.result_view = None
                    self.start(view['action'], view['arguments'])
            elif key in ('v', 'e', 'b', 'l'):
                if self.job:
                    return True
                self.result_view = None
                return self.handle(key)
            return True
        if key in ('?', 'l'):
            if key == 'l':
                width = max(20, self.screen.getmaxyx()[1] - 3)
                lines = []
                for entry in self.logs:
                    while entry:
                        part = clip(entry, width)
                        lines.append(part)
                        entry = entry[len(part):]
                self.text_view = lines or ['暂无日志']
                self.text_offset = max(0, len(self.text_view) - self.screen.getmaxyx()[0] + 4)
            else:
                self.text_view = [
                    's 扫描 D1/D2/D3 广播；x 只读核验编号并登记，扫描名称仅作提示。',
                    '首次下载自动建档；以后使用保存的绝对游标，仅同步新增记录。',
                    '空格勾选设备；a 采集可支持的勾选项，无勾选时只处理本次附近设备。',
                    'c 取消当前任务；q 取消并退出。已落盘数据保留。',
                    'n 手动输入序列号；未命名候选设备可同时指定 BLE UUID。',
                    'i 导入原始完整 CSV 与 .csv.json，源文件保留。',
                    'v 从 SQLite 预览最新记录；e 导出 CSV，支持起止时间筛选。',
                    'b 备份数据库到数据目录/backups/，包括下载恢复进度。',
                    'm 迁移旧档案，先备份核验；p 保存数据位置（不搬动已有数据）；o 打开文件夹。',
                    'r 只读刷新本地档案；l 查看日志，↑↓ / PgUp / PgDn 滚动。',
                    '首次下载中断：保留在数据库，下次重新读取完整历史。',
                    '增量下载中断：数据库保留待确认块，下次从绝对游标恢复。',
                    'D1/D2/D3 按官方协议解析；g 查看配置，j 设置批量并发数。',
                    '设备时间没有时区；结束日期只写日期时包含当天整天。',
                ]
                self.text_offset = 0
            return True
        if key in ('q', '\x1b'):
            if self.job:
                self.job.cancel()
                self.quit_after_cancel = True
                self.message = '正在取消并断开蓝牙，随后退出…'
                return True
            return False
        if key == 'c' and self.job:
            self.job.cancel()
            self.message = '正在取消；等待设备连接清理…'
        elif key == curses.KEY_DOWN:
            self.selected = min(max(0, len(self.devices) - 1), self.selected + 1)
        elif key == curses.KEY_UP:
            self.selected = max(0, self.selected - 1)
        elif not self.job:
            device = self.selected_device()
            if key == 'j':
                self.open_form('设置本次批量并发数（1–8，建议从 2 开始）',[('并发数',self.parallel)],'parallel')
            elif key == 'g':
                if not device or not device.get('serial'): self.message='先扫描并按 x 核验编号'
                else:
                    args=['device',device['serial']]
                    if device.get('ble_id'): args+=['--ble-id',device['ble_id']]
                    self.start('device',args)
            elif key == 'm':
                self.start('migrate', ['migrate'])
            elif key == 'p':
                self.open_form('保存数据位置（不搬动已有数据；新位置可以为空）', [('数据目录', str(self.library.root))], 'configure')
            elif key == 'o':
                if self.library.root.exists():
                    subprocess.Popen(['open' if sys.platform == 'darwin' else 'xdg-open', str(self.library.root)])
                    self.message = f'已打开：{self.library.root}'
                else:
                    self.message = '尚未建立数据目录；首次下载后即可打开'
            elif key == 's':
                self.start('scan', ['scan'])
            elif key == 'r':
                self.start('list', ['list'])
            elif key == 'b':
                self.start('backup', ['backup'])
            elif key == 'x':
                if not device:
                    self.message = '先按 s 扫描并选择设备'
                elif device.get('ble_id'):
                    self.start('register', ['register', '--ble-id', device['ble_id']])
                elif device.get('serial'):
                    self.start('register', ['register', '--serial', device['serial']])
                else:
                    self.message = '没有连接地址，请重新扫描'
            elif key == ' ':
                if device and device.get('serial') and not device.get('identity_conflict'):
                    serial = device['serial']
                    if serial in self.marked: self.marked.remove(serial)
                    else: self.marked.add(serial)
                    self.message = f'已勾选 {len(self.marked)} 台；按 a 依次采集'
                else:
                    self.message = '请先用 x 读取编号；重复编号需先核验'
            elif key == 'n':
                self.open_form('添加设备 / 为未命名候选设备指定序列号',
                               [('序列号', ''), ('BLE 地址 / UUID（可留空）',
                                device.get('ble_id', '') if device and not device.get('serial') else '')], 'add')
            elif key == 'i':
                self.open_form('导入完整 CSV（源文件保留，必须有原始下载元数据）',
                               [('CSV 路径', ''), ('元数据路径（留空使用 CSV.json）', '')], 'import')
            elif key in ('\n', '\r', curses.KEY_ENTER, 'd'):
                if device and not device.get('serial') and device.get('ble_id'):
                    self.start('register', ['register', '--ble-id', device['ble_id']])
                elif not device or not device.get('serial'):
                    self.message = '先 s 扫描或 n 输入序列号'
                elif device.get('identity_conflict'):
                    self.message = '设备身份有冲突，请按 x 按地址核验后再下载'
                elif device.get('history_supported') is False:
                    self.message = f'{device.get("device_type", "此型号")} 编号已识别；历史格式尚未适配，不会套用 D3 解析'
                else:
                    args = ['download', '--serial', device['serial'], '--execute']
                    if device.get('ble_id'):
                        args += ['--ble-id', device['ble_id']]
                    self.start('download', args)
            elif key == 'a':
                targets, addresses, skipped = [], [], []
                for item in self.devices.values():
                    serial = item.get('serial')
                    if self.marked and serial not in self.marked:
                        continue
                    if not serial or item.get('identity_conflict'):
                        skipped.append(f'{serial or item.get("name", "未知设备")}：身份未核验或冲突')
                        continue
                    if item.get('history_supported') is False:
                        skipped.append(f'{serial}：{item.get("device_type", "此型号")} 历史待适配')
                        continue
                    if not self.marked and not item.get('nearby'):
                        skipped.append(f'{serial}：本次未扫描到')
                        continue
                    if serial not in targets:
                        targets.append(serial)
                        addresses.append(item.get('ble_id') or '-')
                if skipped:
                    self.logs.append('本次跳过（档案保留）：' + '；'.join(skipped))
                if targets:
                    args=['download', '--serials', *targets, '--ble-ids', *addresses, '--execute']
                    if self.parallel>1:args+=['--parallel',str(self.parallel)]
                    self.start('download',args)
                else:
                    self.message = '没有可采集目标；先 s 扫描，离线档案保留。'
            elif key in ('v', 'e'):
                if not device or not (device.get('csv') or device.get('database_records')) or device.get('error'):
                    self.message = '请先选择已建立且可校验的本地档案'
                elif key == 'v':
                    self.start('records', ['records', device['serial'], '--limit', '100'])
                else:
                    default = str(self.library.root / 'exports' / f'{(device.get("device_type") or "drop").lower()}_{device["serial"]}_{time.strftime("%Y%m%d-%H%M%S")}.csv')
                    self.open_form('导出 CSV（设备本地时间；结束日期包含当天整天）',
                                   [('输出路径（不覆盖）', default), ('开始时间（可空）', ''),
                                    ('结束时间（可空）', '')], 'export')
        return True

    def put(self, row, text, attribute=0):
        height, width = self.screen.getmaxyx()
        if row < 0 or row >= height:
            return
        try:
            self.screen.addstr(row, 1, clip(text, max(0, width - 2)), attribute)
        except curses.error:
            pass

    def draw(self):
        self.screen.erase()
        height, width = self.screen.getmaxyx()
        if height < 18 or width < 68:
            self.put(0, '请将终端扩大至至少 68 列 × 18 行；q 退出')
            self.screen.refresh()
            return
        self.put(0, f'KESTREL DROP  /  电脑下载管理  {__version__}', curses.A_BOLD)
        self.put(1, f'档案目录：{self.library.root}', curses.A_DIM)
        self.put(2, 'D1/D2/D3 身份识别与历史采集 · 连接前断开手机 LiNK', curses.A_DIM)
        self.put(4, '选 型号  设备序列号          附近    已入库    未确认   状态 / 最新时间', curses.A_BOLD)
        visible = max(2, min(8, height - 15))
        values = list(self.devices.values())
        top = max(0, self.selected - visible + 1)
        for index, item in enumerate(values[top:top + visible], top):
            serial = item.get('serial') or '待核验（x）'
            kind = item.get('device_type') or model_type(item.get('model')) or '未知'
            state = (item.get('error') or ('身份冲突，请核验' if item.get('identity_conflict') else None)
                     or item.get('last_timestamp') or item.get('identity_error')
                     or ('广播编号；x 核验' + ('，历史待适配' if item.get('history_supported') is False else '') if item.get('serial') and not item.get('identity_verified') else
                         '历史待适配' if item.get('history_supported') is False else '待下载' if item.get('serial') else '按 x 读取编号'))
            progress=self.device_progress.get(item.get('serial'),{})
            if self.job and progress.get('event')=='progress':
                state=f'接收 {progress.get("received",0)}/{progress.get("expected",0)}'
            elif self.job and progress.get('event')=='phase': state=PHASES.get(progress.get('phase'),state)
            elif self.job and progress.get('event')=='completed': state=STATUSES.get(progress.get('status'),state)
            elif self.job and progress.get('event')=='error': state='未完成：'+progress.get('message','查看日志')
            pending = item.get('pending_records')
            line = (f'{"*" if item.get("serial") in self.marked else " "}  {kind:<4} {serial:<18} '
                    f'{"附近" if item.get("nearby") else "—":<4} '
                    f'{item.get("database_records", item.get("committed_records", "—")):>7}  '
                    f'{pending if pending is not None else "—":>7}  {state}')
            self.put(5 + index - top, line, curses.A_REVERSE if index == self.selected else 0)
        if not values:
            self.put(5, '暂无设备。按 s 扫描附近设备，或按 i 导入已有 CSV。')
        row = 6 + visible
        self.put(row, self.message, curses.A_BOLD)
        self.put(row + 1, self.progress or ('执行中 ' + '|/-\\'[int(time.monotonic() * 4) % 4] if self.job else '等待操作'))
        log_start = row + 3
        for number, line in enumerate(list(self.logs)[-max(1, height - log_start - 3):]):
            self.put(log_start + number, line, curses.A_DIM)
        self.put(height - 2, '↑↓ 选择  空格勾选  a 批量  Enter/d 下载  s 扫描  x 读编号  v 记录  e 导出')
        self.put(height - 1, f'g 设备配置  j 并发({self.parallel})  b 备份  m 迁移  p 位置  c 取消  ? 帮助  q 退出', curses.A_DIM)
        if self.preview is not None:
            self.screen.erase()
            self.put(0, '最近 100 条记录 · ↑↓ 滚动 · Esc/q 返回 · 时间为设备本地时间', curses.A_BOLD)
            self.put(2, '时间                    温度°C     湿度%     气压hPa')
            for index, item in enumerate(self.preview[self.preview_offset:self.preview_offset + height - 5]):
                humidity = item['relative_humidity_pct']
                pressure = item['station_pressure_hpa']
                self.put(index + 3, f'{item["timestamp_device_local"]}   {item["temperature_c"]:>8}  '
                         f'{str(humidity) if humidity is not None else "--":>8}  '
                         f'{str(pressure) if pressure is not None else "--":>10}')
        if self.text_view is not None:
            self.screen.erase()
            self.put(0, '帮助 / 任务日志 · ↑↓ / PgUp / PgDn 滚动 · Esc/q 返回', curses.A_BOLD)
            for index, line in enumerate(self.text_view[self.text_offset:self.text_offset + height - 3]):
                self.put(index + 2, line)
        if self.device_state is not None:
            self.screen.erase()
            state=self.device_state;settings=state.get('settings') or {}
            self.put(0,f'设备 {state.get("serial")} · {state.get("model", "")} · 配置 / 实时测量',curses.A_BOLD)
            wrap_label='设备未提供' if settings.get('wrap') is None else '开启' if settings.get('wrap') else '关闭'
            battery='未读到' if state.get('battery_pct') is None else str(state['battery_pct'])+'%（设备报告）'
            lines=[f'名称：{state.get("name") or "默认名称"}   电量：{battery}',
                f'固件：{state.get("firmware")}   硬件：{state.get("hardware")}',
                f'设备当前时间：{state.get("device_time")}（不会自动校时）',
                f'1 采样间隔：{settings.get("interval_seconds")} 秒',
                f'2 滚动覆盖：{wrap_label}（关闭时满后停止记录）',
                '3 校准设备时间（保留原始历史时间）',
                '4 修改设备名称',f'5 实时更新间隔：{settings.get("update_seconds")} 秒',
                f'6 气压参考海拔：{settings.get("reference_altitude_m")} 米',
                'r 重新读取   w 读取实时测量   b 闪灯定位',
                self.message]
            if state.get('live'):
                readings=state['live']['measurements']
                entries=[(key,label,unit) for key,label,unit in [('temperature_c','温度','°C'),('relative_humidity_pct','湿度','%'),('heat_index_c','热指数','°C'),('dew_point_c','露点','°C'),('station_pressure_hpa','气压','hPa'),('density_altitude_m','密度高度','m')]]
                for offset in range(0,len(entries),2):
                    parts=[]
                    for key,label,unit in entries[offset:offset+2]:
                        value=readings.get(key);parts.append(f'{label}：{value if value is not None else "--"} {unit}')
                    lines.append('    '.join(parts))
            if state.get('read_errors'):lines.append('部分字段未读到：'+str(state['read_errors']))
            for row,line in enumerate(lines,2): self.put(row,line)
            self.put(height-1,'Enter / Esc 返回设备列表；配置写入后逐项回读，失败不自动重试',curses.A_DIM)
        if self.form:
            self.screen.erase()
            self.put(1, self.form['title'], curses.A_BOLD)
            for index, (label, value) in enumerate(self.form['fields']):
                self.put(4 + index * 3, label)
                if len(clip(value, width - 6)) < len(value):
                    value = '…' + clip(value[::-1], width - 7)[::-1]
                self.put(5 + index * 3, '> ' + value, curses.A_REVERSE if index == self.form['index'] else 0)
            self.put(height - 4, self.form['error'])
            self.put(height - 2, 'Tab / ↑↓ 切换字段 · Enter 下一项 / 提交 · Ctrl+U 清空 · Esc 返回')
        if self.result_view is not None and self.text_view is None:
            self.draw_result(height, width)
        self.screen.refresh()

    def draw_result(self, height, width):
        self.screen.erase()
        view = self.result_view
        self.put(1, '下载结果' if view['action'] == 'download' else '操作结果', curses.A_BOLD)
        if view['outcomes']:
            incomplete = sum(item['status'] in ('pending', 'failed') for item in view['outcomes'])
            self.put(2, f'本次处理 {len(view["outcomes"])} 台设备 · 未完成 {incomplete} 台（完整明细见日志）')
        row = 3
        for item in view['outcomes'][:max(1, height - 12)]:
            self.put(row, f'{item["serial"]} · {STATUSES.get(item["status"], item["status"])}', curses.A_BOLD)
            row += 1
            if item.get('status') != 'failed':
                self.put(row, f'本次新增 {item.get("added_records", 0):,} 条 · 累计 {item.get("total_records", 0):,} 条 · 未确认 {item.get("pending_records", 0):,} 条')
                row += 1
                if len(view['outcomes']) == 1:
                    self.put(row, f'首末记录时间：{item.get("first_timestamp") or "—"} / {item.get("last_timestamp") or "—"}')
                    row += 2
            if row >= height - 10:
                break
        if view['failures']:
            failure = view['failures'][0]
            self.put(row, failure.get('title') or TITLES.get(failure['code'], '操作未完成'), curses.A_BOLD)
            row += 1
            for text in (failure['action'],):
                while text and row < height - 4:
                    segment = clip(text, width - 3)
                    self.put(row, segment)
                    text = text[len(segment):]
                    row += 1
        elif not view['code']:
            statuses = {item['status'] for item in view['outcomes']}
            self.put(row, '设备暂无记录，尚未建立档案。' if statuses == {'empty'} else
                     '本次仅预演，未访问蓝牙或建立档案。' if statuses == {'dry_run'} else
                     '完整记录已验证保存。可以预览、导出或备份。')
        self.put(height - 3, ('r 重试本次操作 · ' if any(item.get('retryable') for item in view['failures']) else '')
                 + 'l 查看详情日志 · Enter 返回')
        self.put(height - 2, 'v 查看已保存记录 · e 导出 CSV · b 数据库备份', curses.A_DIM)

    def loop(self):
        self.screen.timeout(100)
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        self.start('list', ['list'])
        try:
            while True:
                self.poll()
                self.refresh_if_changed()
                if self.quit_after_cancel and self.job is None:
                    return 0
                self.draw()
                try:
                    key = self.screen.get_wch()
                except curses.error:
                    continue
                if not self.handle(key):
                    return 0
        finally:
            if self.job:
                self.job.close()


def run(library):
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        raise RuntimeError('TUI 需要交互式终端；脚本请使用 list / scan / download 等子命令')
    import locale
    locale.setlocale(locale.LC_ALL, '')
    return curses.wrapper(lambda screen: Terminal(screen, library).loop())
