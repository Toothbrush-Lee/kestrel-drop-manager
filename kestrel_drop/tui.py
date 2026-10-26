"""Responsive curses UI. BLE work runs in a cancellable child process.

No background thread writes to curses; output is delivered through a queue.
The child receives SIGINT on cancellation and exits through protocol cleanup.
"""
from __future__ import annotations

from collections import deque
import curses
import json
import queue
import subprocess
import sys
from datetime import datetime
import time

from . import __version__
from .protocol import TransferProgress
from .errors import TITLES
from .devices import model_type
from .worker import Job
from .ui_text import clip, text_width, wrap_lines, input_window
from .ui_actions import format_interval, setting_change, retry_download_arguments, selected_devices, batch_setting_change

PHASES = {'scanning': '扫描附近设备', 'locating': '定位已知设备（发现即连接）', 'connecting': '连接设备', 'identifying': '核对型号和序列号',
          'querying': '查询新增记录', 'receiving': '接收并保存连续数据块', 'verifying': '验证完整性和完成游标',
          'saving': '发布下载档案', 'archive_check': '检查本地档案', 'indexing': '校验并入库'}
STATUSES = {'downloaded': '已保存', 'up_to_date': '已经是最新', 'pending': '有数据尚未确认',
            'verified': '配置已回读验证', 'not_attempted': '尚未执行', 'cancelled_unknown': '取消，需回读',
            'empty': '设备暂无记录', 'failed': '未完成', 'dry_run': '仅预演'}


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
        self.review = None
        self.batch_settings = None
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
        if self.job or self.form or self.preview is not None or self.text_view is not None or self.result_view is not None or self.device_state is not None or self.review is not None or self.batch_settings is not None: return
        if self.last_stamp is not None and self.database_stamp()!=self.last_stamp:
            self.refresh_message='本地数据已更新（自动刷新）'
            self.start('list',['list'])

    def start(self, action, arguments):
        if self.job:
            self.message = '任务执行中；按 c 取消，或等待完成'
            return False
        if action=='list': self.list_stamp=self.database_stamp()
        if action in ('download','set-batch'): self.device_progress={}
        self.action = action
        self.progress = ''
        try:
            self.job = Job(self.library.root, arguments)
        except OSError as exc:
            self.action=None
            self.message=f'无法启动后台任务：{exc}'
            self.logs.append(self.message)
            return False
        self.message = {'scan': '正在扫描附近设备（15 秒）…', 'list': '正在读取本地档案…',
                        'download': '正在连接设备；c 可取消并保留已落盘数据…'}.get(action, '正在处理…')
        self.logs.append('> ' + ' '.join(arguments))
        return True

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
                        self.marked.clear()
                        self.selected = min(self.selected, max(0, len(self.devices)-1))
                        if selected_serial and selected_serial in self.devices:
                            self.selected = list(self.devices).index(selected_serial)
                        self.message = f'扫描完成：{len(payload)} 个 DROP；已清空旧勾选 · 空格选择 / A 全选 · Enter 下载所选'
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
                        self.device_state['read_stale']=False
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
            if action in ('download', 'import', 'migrate', 'configure', 'register', 'identify', 'set', 'set-batch'):
                self.refresh_message = self.message
                self.start('list', ['list'])
            if action in ('download','set-batch') or (value and action != 'list'):
                outcomes = (job.payload or {}).get('configurations' if action=='set-batch' else 'downloads', job.outcomes) if isinstance(job.payload, dict) else job.outcomes
                failures = job.failures or ([{'code': 'cancelled', 'message': '任务已取消，已落盘数据保留。',
                                            'retryable': True, 'action': '可稍后重新下载。'}] if job.cancelled else [])
                if value and not failures:
                    failures = [{'code': 'worker_failed', 'message': '后台任务未完成。', 'retryable': False,
                                 'action': '按 l 查看日志，检查后台启动或事件通信问题。'}]
                if action in ('set','blink','set-batch'):
                    for failure in failures: failure['retryable']=False
                self.result_view = {'outcomes': outcomes, 'failures': failures, 'code': value,
                                    'action': action, 'arguments': job.arguments, 'selected':0}
                if len(outcomes) == 1 and outcomes[0]['serial'] in self.devices:
                    self.selected = list(self.devices).index(outcomes[0]['serial'])
            return

    def receive_event(self, event):
        kind = event['event']
        if event.get('serial') and kind in ('phase','progress','completed','configured','error'):
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
            if self.action in ('export', 'backup', 'import', 'migrate', 'configure', 'register', 'identify', 'device', 'set'):
                self.logs.append('结果：' + json.dumps(event['data'], ensure_ascii=False))
        elif kind == 'completed':
            for item in self.devices.values():
                if item.get('serial')==event['serial']:
                    item.update(database_records=event['total_records'],committed_records=event['total_records'],
                                pending_records=event['pending_records'],last_timestamp=event.get('last_timestamp'))
            self.job.outcomes.append(event)
            self.logs.append(f'{event["serial"]}: {STATUSES.get(event["status"], event["status"])}，'
                             f'新增 {event["added_records"]}，累计 {event["total_records"]}，未确认 {event["pending_records"]}')
        elif kind == 'configured':
            self.job.outcomes.append(event)
            self.logs.append(f'{event["serial"]}: {STATUSES.get(event["status"],event["status"])}')
        elif kind == 'error':
            self.job.failures.append(event)
            self.logs.append(f'{event["code"]}: {event["message"]}；{event["action"]}')

    def selected_device(self):
        values = list(self.devices.values())
        return values[min(self.selected, len(values) - 1)] if values else None

    def open_form(self, title, fields, action):
        self.form = {'title': title, 'fields': [[label, str(value)] for label, value in fields],
                     'index': 0, 'action': action, 'error': '',
                     'cursors': [len(str(value)) for _,value in fields],
                     'target': self.device_state['serial'] if action.startswith('set_') and self.device_state else (self.selected_device() or {}).get('serial')}

    def submit(self):
        values = [field[1].strip() for field in self.form['fields']]
        action = self.form['action']
        try:
            if action=='parallel':
                value=int(values[0])
                if not 1 <= value <= 8: raise ValueError('并发连接数必须为 1–8，建议从 2 开始')
                self.parallel=value;self.message=f'本次批量并发上限：{value}'
            elif action.startswith('batch_set_'):
                self.review=batch_setting_change(action,values[0],self.form['targets'])
                return
            elif action.startswith('set_'):
                self.review=setting_change(action,values[0],self.device_state)
                return
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
                if self.start('import', ['import', values[0]] + (['--metadata', values[1]] if values[1] else [])) is False:
                    self.form['error']=self.message
                    return
            elif action == 'configure':
                if not values[0]:
                    raise ValueError('请输入数据目录')
                if self.start('configure', ['configure', values[0]]) is False:
                    self.form['error']=self.message
                    return
            elif action == 'export':
                if not values[0]:
                    raise ValueError('请输入导出路径')
                args = ['export', self.form['target'], values[0]]
                if values[1]:
                    args += ['--since', values[1]]
                if values[2]:
                    args += ['--until', values[2]]
                if self.start('export', args) is False:
                    self.form['error']=self.message
                    return
            self.form = None
        except (ValueError, OSError) as exc:
            self.form['error'] = str(exc)

    def handle_form(self, key):
        form=self.form
        index=form['index']
        positions=form.setdefault('cursors',[len(field[1]) for field in form['fields']])
        value=form['fields'][index][1]
        cursor=max(0,min(positions[index],len(value)))
        if key=='\x1b': self.form=None
        elif key in ('\t',curses.KEY_DOWN): form['index']=(index+1)%len(form['fields'])
        elif key in (curses.KEY_BTAB,curses.KEY_UP): form['index']=(index-1)%len(form['fields'])
        elif key in ('\n','\r',curses.KEY_ENTER):
            if index+1<len(form['fields']): form['index']+=1
            else: self.submit()
        elif key==curses.KEY_LEFT: cursor=max(0,cursor-1)
        elif key==curses.KEY_RIGHT: cursor=min(len(value),cursor+1)
        elif key==curses.KEY_HOME: cursor=0
        elif key==curses.KEY_END: cursor=len(value)
        elif key in ('\b','\x7f',curses.KEY_BACKSPACE):
            if cursor: value=value[:cursor-1]+value[cursor:];cursor-=1
        elif key==curses.KEY_DC: value=value[:cursor]+value[cursor+1:]
        elif key=='\x15': value='';cursor=0
        elif isinstance(key,str) and key.isprintable(): value=value[:cursor]+key+value[cursor:];cursor+=len(key)
        # submit() can replace the current overlay, so update only its original buffer.
        if self.form is form and self.review is None:
            form['fields'][index][1]=value
            positions[index]=cursor

    def open_text(self, mode):
        self.text_view=[]
        if mode=='l':
            for entry in self.logs: self.text_view.extend(str(entry).splitlines() or [''])
            self.text_view=self.text_view or ['暂无日志']
            self.text_offset=max(0,len(self.wrapped_text())-self.screen.getmaxyx()[0]+4)
        else:
            self.text_view=[
                '快速开始：s 扫描 15 秒 → ↑↓ 选择 → Enter 下载 → e 导出。',
                '已知地址按目标定位，发现即连接；默认扫描 15 秒。',
                '首次读取最早仍保留的历史，以后只下载新增；设备中的旧记录不会自动删除。',
                '编号未知：选择该行按 x 连接核验；已知编号仍会在下载前再次核对。',
                '批量：空格勾选，A / Ctrl+A 全选附近可采集项，u 清空，Enter / a 下载所选。',
                '没有勾选时，Enter 下载当前设备，a 提示先选择；重新扫描会清空旧勾选。',
                'j 设置本次并发数；默认 1，建议先尝试 2，更多连接数需现场验证。',
                '列表和结果：↑↓ / PgUp / PgDn / Home / End 导航，结果页 r 只重试未完成项。',
                'g 设置：已勾选时进入批量设置，否则读取当前设备；预览后再 Enter 写入。',
                '配置页 r 重新读取，w 读实时测量，f 闪灯定位，b 仍是数据库备份。',
                '配置结果不确定时先 r 回读；校时只影响之后的记录，不改写历史。',
                'c 取消任务；主界面 q 取消并退出；其他页面 Esc 返回上一层。',
                'v 预览最近记录；e 导出 CSV；b 备份 SQLite（含续传和配置审计）。',
                'r 刷新本地档案；空闲主界面也会感知外部数据库更新，不会自动连接蓝牙。',
                'o 打开数据目录；p 保存位置，不自动搬动旧数据；m 迁移，i 导入旧 D3 原始档案。',
                '已入库可查询导出；未确认已落盘但尚未完成。首次中断会完整重读，增量可续传。',
                '完整说明：docs/user-guide.md；CLI：docs/cli.md；开发：docs/development.md。']
            self.text_offset=0

    def wrapped_text(self):
        width=max(1,self.screen.getmaxyx()[1]-3)
        return [line for source in self.text_view or [] for line in wrap_lines(source,width)]

    def handle_result(self, key):
        view=self.result_view
        outcomes=view['outcomes']
        selected=min(view.get('selected',0),max(0,len(outcomes)-1))
        step=max(1,self.screen.getmaxyx()[0]-13)
        if key in ('\n','\r','\x1b','q'): self.result_view=None
        elif key==curses.KEY_DOWN: selected=min(max(0,len(outcomes)-1),selected+1)
        elif key==curses.KEY_UP: selected=max(0,selected-1)
        elif key==curses.KEY_NPAGE: selected=min(max(0,len(outcomes)-1),selected+step)
        elif key==curses.KEY_PPAGE: selected=max(0,selected-step)
        elif key==curses.KEY_HOME: selected=0
        elif key==curses.KEY_END: selected=max(0,len(outcomes)-1)
        elif key=='r' and view['action'] not in ('set','set-batch','blink') and any(f.get('retryable') for f in view['failures']) and not self.job:
            args=retry_download_arguments(view['arguments'],outcomes,view['failures']) if view['action']=='download' else view['arguments']
            if args is None: self.message='所有可重试项均已保存；其余问题请核对后从列表选择'
            elif self.start(view['action'],args) is not False: self.result_view=None
        elif key=='g' and view['action']=='set-batch' and outcomes and not self.job:
            item=self.devices.get(outcomes[selected]['serial'],outcomes[selected])
            args=['device',item['serial']]
            if item.get('ble_id'):args+=['--ble-id',item['ble_id']]
            if self.start('device',args) is not False:self.result_view=None
        elif key in ('v','e','b') and not self.job:
            if outcomes and outcomes[selected]['serial'] in self.devices:
                self.selected=list(self.devices).index(outcomes[selected]['serial'])
            self.result_view=None
            self.device_state=None
            return self.handle(key)
        view['selected']=selected
        return True

    def handle(self, key):
        if self.job and key=='c':
            self.job.cancel()
            self.message='正在取消；配置可能部分生效，请在取消后重新读取' if self.action in ('set','blink','set-batch') else '正在取消，等待连接清理'
            return True
        if self.review is not None:
            if key in ('\x1b','q'): self.review=None
            elif self.review.get('targets') and key in (curses.KEY_UP,curses.KEY_DOWN,curses.KEY_NPAGE,curses.KEY_PPAGE,curses.KEY_HOME,curses.KEY_END):
                self.navigate_targets(self.review,key)
            elif key in ('\n','\r',curses.KEY_ENTER):
                action=self.review.get('action','set')
                if self.start(action,self.review['arguments']) is not False:
                    self.review=None;self.form=None
                    if action=='set-batch': self.batch_settings=None
                    elif self.device_state is not None: self.device_state['read_stale']=True
                else: self.review['error']=self.message
            return True
        if self.form:
            self.handle_form(key)
            return True
        if self.text_view is not None:
            total=len(self.wrapped_text())
            step=max(1,self.screen.getmaxyx()[0]-4)
            if key in ('q','\x1b','\n','\r'): self.text_view=None
            elif key in (curses.KEY_DOWN,curses.KEY_NPAGE): self.text_offset=min(max(0,total-1),self.text_offset+(step if key==curses.KEY_NPAGE else 1))
            elif key in (curses.KEY_UP,curses.KEY_PPAGE): self.text_offset=max(0,self.text_offset-(step if key==curses.KEY_PPAGE else 1))
            elif key==curses.KEY_HOME:self.text_offset=0
            elif key==curses.KEY_END:self.text_offset=max(0,total-step)
            return True
        if key in ('?','l'):
            self.open_text(key)
            return True
        if self.result_view is not None: return self.handle_result(key)
        if self.preview is not None:
            if key in ('q','\x1b','\n','\r'): self.preview=None
            elif key in (curses.KEY_DOWN,curses.KEY_NPAGE): self.preview_offset=min(max(0,len(self.preview)-1),self.preview_offset+(max(1,self.screen.getmaxyx()[0]-5) if key==curses.KEY_NPAGE else 1))
            elif key in (curses.KEY_UP,curses.KEY_PPAGE): self.preview_offset=max(0,self.preview_offset-(max(1,self.screen.getmaxyx()[0]-5) if key==curses.KEY_PPAGE else 1))
            elif key==curses.KEY_HOME:self.preview_offset=0
            elif key==curses.KEY_END:self.preview_offset=max(0,len(self.preview)-max(1,self.screen.getmaxyx()[0]-5))
            return True
        if self.batch_settings is not None:
            if key in ('q','\x1b'): self.batch_settings=None
            elif key in (curses.KEY_UP,curses.KEY_DOWN,curses.KEY_NPAGE,curses.KEY_PPAGE,curses.KEY_HOME,curses.KEY_END):
                self.navigate_targets(self.batch_settings,key)
            elif key in ('1','2','3','5') and not self.job:
                title,value,action={
                    '1':('批量设置采样间隔（秒）','300','batch_set_interval'),
                    '2':('批量设置滚动覆盖（on/off）','on','batch_set_wrap'),
                    '3':('批量同步设备时间','now','batch_set_time'),
                    '5':('批量设置实时更新间隔（秒）','30','batch_set_update')}[key]
                self.open_form(title,[('新值（Enter 查看目标和变更）',value)],action)
                self.form['targets']=[dict(item) for item in self.batch_settings['targets']]
            return True
        if self.device_state is not None and self.result_view is None:
            if key in ('q','\x1b','\n','\r'):
                if not self.job: self.device_state=None
                else: self.message='任务仍在进行；c 取消，完成后 Esc 返回'
                return True
            if self.job: return True
            state=self.device_state;settings=state.get('settings') or {}
            forms={'1':('设置采样间隔（秒）',settings.get('interval_seconds') or 300,'set_interval'),
                   '2':('滚动覆盖：on 覆盖最早记录 / off 满后停止记录','on' if settings.get('wrap') else 'off','set_wrap'),
                   '3':('校时：输入设备本地时间或 now；不会修改旧记录','now','set_time'),
                   '4':('设备名称：1–20 个可打印 ASCII 字符',state.get('name') or '','set_name'),
                   '5':('实时更新间隔：2–60 秒',settings.get('update_seconds') or 30,'set_update'),
                   '6':('气压计算参考海拔：米',settings.get('reference_altitude_m') or 0,'set_altitude')}
            if key in forms and state.get('read_stale'):
                self.message='上次修改结果未确认，请先按 r 重新读取设备设置'
            elif key=='6' and (state.get('device_type') or model_type(state.get('model')))!='D3':
                self.message='此型号没有气压测量，无需配置参考海拔'
            elif key in {'1','2','5','6'} and settings.get({'1':'interval_seconds','2':'wrap','5':'update_seconds','6':'reference_altitude_m'}[key]) is None:
                self.message='此设置未读到或设备不支持，请先按 r 重新读取'
            elif key in forms:
                title,value,action=forms[key];self.open_form(title,[('新值（Enter 查看变更）',value)],action)
            elif key=='b': self.start('backup',['backup'])
            elif key in ('r','w','f'):
                action={'r':'device','w':'live','f':'blink'}[key]
                args=[action,state['serial']]
                if state.get('ble_id'):args+=['--ble-id',state['ble_id']]
                if action=='blink':args+=['--execute']
                self.start(action,args)
            return True
        if key=='\x1b':
            self.message='已在设备列表；按 q 退出'
            return True
        if key=='q':
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
        elif key in (curses.KEY_NPAGE,curses.KEY_PPAGE,curses.KEY_HOME,curses.KEY_END):
            step=max(2,min(8,self.screen.getmaxyx()[0]-15))
            if key==curses.KEY_NPAGE:self.selected=min(max(0,len(self.devices)-1),self.selected+step)
            elif key==curses.KEY_PPAGE:self.selected=max(0,self.selected-step)
            elif key==curses.KEY_HOME:self.selected=0
            else:self.selected=max(0,len(self.devices)-1)
        elif not self.job:
            device = self.selected_device()
            if key=='u':
                self.marked.clear();self.message='已清空勾选；Enter 下载当前设备，A 全选附近设备'
            elif key in ('\x01','A'):
                self.marked={item['serial'] for item in self.devices.values() if item.get('serial') and item.get('nearby') and not item.get('identity_conflict') and item.get('history_supported') is not False}
                self.message=f'已勾选 {len(self.marked)} 台附近可采集设备'
            elif key == 'j':
                self.open_form('设置本次批量并发数（1–8，建议从 2 开始）',[('并发数',self.parallel)],'parallel')
            elif key == 'g':
                if self.marked:
                    try: self.batch_settings={'targets':selected_devices(self.devices,self.marked),'offset':0}
                    except ValueError as exc:self.message=str(exc)
                elif not device or not device.get('serial'): self.message='先扫描并按 x 核验编号'
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
            elif key in ('s', 'S'):
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
                if device and device.get('serial') and not device.get('identity_conflict') and device.get('history_supported') is not False:
                    serial = device['serial']
                    if serial in self.marked: self.marked.remove(serial)
                    else: self.marked.add(serial)
                    self.message = f'已勾选 {len(self.marked)} 台；按 a 以并发 {self.parallel} 采集'
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
                if self.marked:
                    self.download_selected()
                elif device and not device.get('serial') and device.get('ble_id'):
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
                self.download_selected()
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

    def download_selected(self):
        try:
            targets=selected_devices(self.devices,self.marked)
        except ValueError as exc:
            self.message=str(exc)
            return
        args=['download','--serials',*[item['serial'] for item in targets],
              '--ble-ids',*[item.get('ble_id') or '-' for item in targets],'--execute']
        if self.parallel>1: args+=['--parallel',str(self.parallel)]
        self.start('download',args)

    def navigate_targets(self, view, key):
        maximum=max(0,len(view['targets'])-1)
        step=max(1,self.screen.getmaxyx()[0]-14)
        offset=view.get('offset',0)
        if key==curses.KEY_HOME:offset=0
        elif key==curses.KEY_END:offset=maximum
        else:offset+= {curses.KEY_UP:-1,curses.KEY_DOWN:1,curses.KEY_NPAGE:step,curses.KEY_PPAGE:-step}[key]
        view['offset']=max(0,min(maximum,offset))

    def draw_targets(self, view, row, count):
        targets=view['targets'];offset=view.get('offset',0)
        top=max(0,offset-count+1)
        for index,item in enumerate(targets[top:top+count],top):
            self.put(row+index-top,f"{index+1:>3}. {item['serial']} · {item.get('device_type','')} · {item.get('ble_id') or '按编号寻找'}",curses.A_REVERSE if index==offset else 0)

    def draw_batch_settings(self,height,width):
        self.screen.erase()
        view=self.batch_settings
        self.put(0,f"批量设置 · 明确勾选的 {len(view['targets'])} 台",curses.A_BOLD)
        self.put(1,'1 采样间隔   2 滚动覆盖   3 同步电脑时间   5 实时更新间隔')
        self.put(2,'逐台核验身份 → 读取旧值 → 写入 → 回读 → 保存审计',curses.A_DIM)
        self.draw_targets(view,4,max(1,height-9))
        self.put(height-3,'所有目标均可用 ↑↓ / PgUp / PgDn / Home / End 浏览')
        self.put(height-2,'选择项目后编辑 → 检查目标与变更 → Enter 才执行')
        self.put(height-1,'Esc 返回列表；此页尚未连接或修改设备',curses.A_DIM)

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
        height,width=self.screen.getmaxyx()
        if width<68 or height<18:
            self.put(0,'窗口太小：请扩大到至少 68 列 × 18 行')
            self.put(2,'c 取消任务；q 返回 / 退出；扩大窗口后会恢复当前页面')
            self.screen.refresh()
            return
        self.draw_dashboard(height,width)
        # Overlay order is also the keyboard priority: the topmost page owns input.
        if self.device_state is not None: self.draw_device(height,width)
        if self.batch_settings is not None:self.draw_batch_settings(height,width)
        if self.preview is not None: self.draw_preview(height,width)
        if self.result_view is not None: self.draw_result(height,width)
        if self.text_view is not None: self.draw_text(height,width)
        if self.form is not None: self.draw_form(height,width)
        if self.review is not None: self.draw_review(height,width)
        try: curses.curs_set(1 if self.form is not None and self.review is None else 0)
        except curses.error: pass
        self.screen.refresh()

    def draw_dashboard(self,height,width):
        self.put(0,f'KESTREL DROP  /  电脑下载管理  {__version__}',curses.A_BOLD)
        self.put(1,f'数据目录：{self.library.root}',curses.A_DIM)
        nearby=sum(bool(item.get('nearby')) for item in self.devices.values())
        scope=f'勾选的 {len(self.marked)} 台' if self.marked else '尚未勾选（A 全选）'
        self.put(2,f'附近 {nearby} 台 · 已选 {len(self.marked)} 台 · 并发 {self.parallel} · Enter / a 下载：{scope}')
        values=list(self.devices.values())
        if values:
            item=values[min(self.selected,len(values)-1)]
            self.put(3,f'当前 {self.selected+1}/{len(values)}：{item.get("serial") or "编号待核验"} · '+('已核验' if item.get('identity_verified') else '连接后核验编号'),curses.A_DIM)
        self.put(4,'选 型号  设备编号          附近    已入库    未确认   状态 / 末条记录时间',curses.A_BOLD)
        visible=max(2,min(8,height-15))
        top=max(0,self.selected-visible+1)
        for index,item in enumerate(values[top:top+visible],top):
            serial=item.get('serial') or '待核验（x）'
            kind=item.get('device_type') or model_type(item.get('model')) or '未知'
            state=(item.get('error') or ('身份冲突，请核验' if item.get('identity_conflict') else None)
                   or item.get('identity_error') or item.get('last_timestamp') or
                   ('待下载' if item.get('serial') else '按 x 读取编号'))
            if item.get('history_supported') is False: state='历史格式待适配'
            progress=self.device_progress.get(item.get('serial'),{})
            if self.job:
                if progress.get('event')=='progress': state=f'接收 {progress.get("received",0)}/{progress.get("expected",0)}'
                elif progress.get('event')=='phase': state=PHASES.get(progress.get('phase'),state)
                elif progress.get('event') in ('completed','configured'): state=STATUSES.get(progress.get('status'),state)
                elif progress.get('event')=='error': state='未完成：'+progress.get('message','查看日志')
            location='附近' if item.get('nearby') else '—'
            location+= ' '*max(0,4-text_width(location))
            pending=item.get('pending_records')
            line=(f'{"*" if item.get("serial") in self.marked else " "}  {kind:<4} {serial:<18} {location} '
                  f'{item.get("database_records",item.get("committed_records","—")):>7}  '
                  f'{pending if pending is not None else "—":>7}  {state}')
            self.put(5+index-top,line,curses.A_REVERSE if index==self.selected else 0)
        if not values:
            self.put(6,'第一次使用：按 s 扫描附近设备',curses.A_BOLD)
            self.put(7,'选设备按 Enter 下载；编号未知时先按 x 核验')
            self.put(8,'已下载的数据可离线按 v 查看、e 导出；? 查看帮助',curses.A_DIM)
        row=6+visible
        for offset,line in enumerate(wrap_lines(self.message,width-3)[:2]): self.put(row+offset,line,curses.A_BOLD)
        self.put(row+2,self.progress or ('执行中 '+ '|/-\\'[int(time.monotonic()*4)%4] if self.job else '等待操作'))
        log_start=row+4
        for offset,line in enumerate(list(self.logs)[-max(1,height-log_start-3):]):self.put(log_start+offset,line,curses.A_DIM)
        self.put(height-2,'Enter 下载所选/当前  s 扫描  空格勾选  A 全选  a 下载所选  g 设置')
        self.put(height-1,'Ctrl+A 全选附近  u 清空  j 并发  b 备份  l 日志  ? 帮助  q 退出',curses.A_DIM)

    def draw_preview(self,height,width):
        self.screen.erase()
        self.put(0,'最近 100 条 · 按设备序号排列，最新在上 · Esc 返回',curses.A_BOLD)
        self.put(1,f'显示 {min(len(self.preview),self.preview_offset+1)}/{len(self.preview)} · ↑↓ / PgUp / PgDn / Home / End',curses.A_DIM)
        self.put(2,'设备记录时间            温度°C     湿度%     气压hPa')
        for row,item in enumerate(self.preview[self.preview_offset:self.preview_offset+height-5],3):
            humidity=item['relative_humidity_pct'];pressure=item['station_pressure_hpa']
            self.put(row,f'{item["timestamp_device_local"]}   {item["temperature_c"]:>8}  '
                     f'{str(humidity) if humidity is not None else "--":>8}  '
                     f'{str(pressure) if pressure is not None else "--":>10}')
        if not self.preview:self.put(4,'没有可显示的正式记录')
        self.put(height-1,'-- 表示该型号没有此测量；时间为设备原始本地时间',curses.A_DIM)

    def draw_text(self,height,width):
        self.screen.erase()
        self.put(0,'帮助 / 任务日志 · ↑↓ / PgUp / PgDn · Esc 返回上一层',curses.A_BOLD)
        lines=self.wrapped_text()
        self.text_offset=min(self.text_offset,max(0,len(lines)-(height-4)))
        for row,line in enumerate(lines[self.text_offset:self.text_offset+height-4],2):self.put(row,line)
        self.put(height-1,f'第 {min(len(lines),self.text_offset+1)}/{len(lines)} 行 · Home / End 到首尾',curses.A_DIM)

    def draw_device(self,height,width):
        self.screen.erase()
        state=self.device_state;settings=state.get('settings') or {}
        self.put(0,f'设备 {state.get("serial")} · {state.get("model","")} · 配置 / 实时测量',curses.A_BOLD)
        wrap='未读到' if settings.get('wrap') is None else '开启' if settings.get('wrap') else '关闭'
        battery='未读到' if state.get('battery_pct') is None else str(state['battery_pct'])+'%（设备报告）'
        lines=[f'名称：{state.get("name") or "默认名称"}   电量：{battery}',
               f'固件：{state.get("firmware") or "未读到"}   硬件：{state.get("hardware") or "未读到"}',
               f'设备当前时间：{state.get("device_time") or "未读到"}（不自动校时）',
               f'1 采样间隔：{format_interval(settings.get("interval_seconds"))}',
               f'2 滚动覆盖：{wrap}（关闭时满后停止记录）',
               '3 校准设备时间（不改旧记录）   4 修改设备名称',
               f'5 实时更新间隔：{format_interval(settings.get("update_seconds"))}']
        if (state.get('device_type') or model_type(state.get('model')))=='D3':lines.append(f'6 气压参考海拔：{settings.get("reference_altitude_m")} 米')
        lines+=['r 重新读取   w 读实时测量   f 闪灯定位   b 数据库备份',
                '显示的是上次读取值，修改结果未确认：请先 r 回读' if state.get('read_stale') else self.message]
        if state.get('live'):
            readings=state['live']['measurements']
            fields=[('temperature_c','温度','°C'),('relative_humidity_pct','湿度','%'),('heat_index_c','热指数','°C'),
                    ('dew_point_c','露点','°C'),('station_pressure_hpa','气压','hPa'),('density_altitude_m','密度高度','m')]
            for offset in range(0,len(fields),2):
                lines.append('    '.join(f'{label}：{readings.get(key) if readings.get(key) is not None else "--"} {unit}' for key,label,unit in fields[offset:offset+2]))
        if state.get('read_errors'):lines.append('部分信息未读到，按 r 重试；l 查看日志')
        for row,line in enumerate(lines[:height-4],2):self.put(row,line)
        self.put(height-1,'修改：输入新值 → 预览 → 写入 / 回读 · l 日志 · ? 帮助 · Esc 返回',curses.A_DIM)

    def draw_form(self,height,width):
        self.screen.erase()
        form=self.form
        for row,line in enumerate(wrap_lines(form['title'],width-3)[:2]):self.put(row,line,curses.A_BOLD)
        if form.get('targets'):self.put(2,f"批量目标：{len(form['targets'])} 台；下一步核对完整名单",curses.A_DIM)
        elif form.get('target'):self.put(2,f'设备编号：{form["target"]}',curses.A_DIM)
        count=max(1,(height-8)//3);top=max(0,form['index']-count+1)
        cursor_screen=None
        for index,(label,value) in enumerate(form['fields'][top:top+count],top):
            row=4+(index-top)*3
            self.put(row,label)
            positions=form.get('cursors',[len(item[1]) for item in form['fields']])
            visible,column=input_window(value,positions[index],width-6)
            self.put(row+1,'> '+visible,curses.A_REVERSE if index==form['index'] else 0)
            if index==form['index']:cursor_screen=(row+1,min(width-2,3+column))
        help_text={
            'set_interval':'单位为秒：300=5分钟，600=10分钟。支持 2/5/10/20/30/60/120/300/360/600/1200/1800/3600/7200/21600/43200。',
            'set_wrap':'输入 on/开启 或 off/关闭；关闭后日志满时停止记录。',
            'set_time':'输入 now 使用电脑本地时间，或 YYYY-MM-DD HH:MM:SS；旧记录保留。',
            'batch_set_time':'now 在每台实际写入时取电脑本地时间；请先检查电脑时区，旧记录不变。',
            'batch_set_interval':'单位为秒；每台执行时读取旧值，仅修改采样间隔。',
            'batch_set_wrap':'on/开启 为满后覆盖，off/关闭 为满后停止记录。',
            'batch_set_update':'2–60 秒，实时测量更新间隔，与历史采样独立。',
            'set_name':'仅支持1–20个可打印ASCII字符，如 Lab-01；真实设备编号不会改变。',
            'set_update':'范围2–60秒，影响实时刷新，与历史采样间隔分开。',
            'set_altitude':'单位为米，可为负值；只用于气压计算，不改出厂校准。',
            'export':'不筛选时间可直接按 Enter 跳过；结束日期包含当天整天。'}
        start=4+min(count,len(form['fields']))*3
        for row,line in enumerate(wrap_lines(help_text.get(form['action'],''),width-3),start):
            if row>=height-5:break
            self.put(row,line,curses.A_DIM)
        for row,line in enumerate(wrap_lines(form['error'],width-3)[:2],height-5):self.put(row,line,curses.A_BOLD)
        self.put(height-2,'Enter 下一项 / 检查 / 提交 · Esc 取消 · Ctrl+U 清空')
        self.put(height-1,'←→ / Home / End 移动光标 · Backspace / Delete 编辑',curses.A_DIM)
        if cursor_screen and hasattr(self.screen,'move'):
            try:self.screen.move(*cursor_screen)
            except curses.error:pass

    def draw_review(self,height,width):
        self.screen.erase()
        review=self.review
        self.put(0,'检查设备变更',curses.A_BOLD)
        if review.get('targets'):
            self.put(1,f"{len(review['targets'])} 台 · {review['label']}",curses.A_BOLD)
            for row,line in enumerate(wrap_lines('准备写入：'+review['new'],width-3)[:2],2):self.put(row,line)
            for row,line in enumerate(wrap_lines(review['impact'],width-3)[:2],4):self.put(row,line,curses.A_DIM)
            self.draw_targets(review,6,max(1,height-12))
            self.put(height-5,'↑↓ / PgUp / PgDn / Home / End 核对全部目标')
        else:
            lines=[f'设备编号：{review["serial"]}',f'修改项目：{review["label"]}',
                   f'上次读取：{review["old"]}',f'准备写入：{review["new"]}','',review['impact'],
                   '写入前会重新核验设备，并以设备最新设置为基础只修改此项。']
            if '--time' in review['arguments'] and 'now' in review['arguments']:
                lines.append('预览时电脑本地时间：'+datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z'))
            wrapped=[line for text in lines for line in wrap_lines(text,width-3)]
            for row,line in enumerate(wrapped[:height-5],2):self.put(row,line)
        if review.get('error'):self.put(height-4,review['error'],curses.A_BOLD)
        self.put(height-2,'Enter 写入并回读验证 · Esc 返回修改',curses.A_BOLD)
        self.put(height-1,self.message if self.job else '仅此步骤会发送配置；关闭预览不会改变设备',curses.A_DIM)

    def draw_result(self,height,width):
        self.screen.erase()
        view=self.result_view;outcomes=view['outcomes']
        selected=min(view.get('selected',0),max(0,len(outcomes)-1))
        incomplete=sum(item.get('status') in ('pending','failed','cancelled_unknown','not_attempted') for item in outcomes)
        self.put(0,'下载结果' if view['action']=='download' else '操作结果',curses.A_BOLD)
        self.put(1,f'本次处理 {len(outcomes)} 台 · 未完成 {incomplete} 台 · 退出码 {view["code"]}')
        self.put(2,f'第 {min(len(outcomes),selected+1)}/{len(outcomes)} 项 · ↑↓ / PgUp / PgDn / Home / End 显示全部结果',curses.A_DIM)
        count=max(1,height-13);top=max(0,selected-count+1)
        self.put(3,'设备编号          配置状态（逐台回读）' if view['action']=='set-batch' else '设备编号          状态              新增     累计   未确认',curses.A_BOLD)
        for index,item in enumerate(outcomes[top:top+count],top):
            label=STATUSES.get(item.get('status'),item.get('status','未完成'))
            label=clip(label,16)+' '*max(0,16-text_width(clip(label,16)))
            line=f'{item["serial"]:<16} {label}' if view['action']=='set-batch' else f'{item["serial"]:<16} {label} {item.get("added_records",0):>6} {item.get("total_records",0):>8} {item.get("pending_records",0):>6}'
            self.put(4+index-top,line,curses.A_REVERSE if index==selected else 0)
        item=outcomes[selected] if outcomes else None
        failure=(item.get('error') if item else None) or next((f for f in view['failures'] if item and f.get('serial')==item['serial']),None)
        if failure is None:failure=next((f for f in view['failures'] if not f.get('serial')),None)
        lines=[]
        if item and item.get('status')!='failed' and view['action']!='set-batch':
            lines.append(f'本次新增 {item.get("added_records",0):,} 条 · 累计 {item.get("total_records",0):,} 条 · 未确认 {item.get("pending_records",0):,} 条')
        if failure:
            lines.append(failure.get('title') or TITLES.get(failure.get('code'),'操作未完成'))
            lines.extend(wrap_lines('原因：'+failure.get('message','查看日志'),width-3)[:1])
            lines.extend(wrap_lines('建议：'+failure.get('action','按 l 查看日志'),width-3)[:2])
        elif item and view['action']=='set-batch':
            lines.append('配置已逐项回读验证并保存审计。' if item['status']=='verified' else STATUSES.get(item['status'],item['status']))
            if item.get('time_write_target_local'):lines.append('实际校时目标：'+item['time_write_target_local'])
            if item['status']!='verified':lines.append('先读取本台设置核对，再决定是否重新配置。')
        elif item and item.get('status')=='pending':lines.append('仍有数据未完成确认；请保留档案，等待新样本后重试。')
        elif item and item.get('status')=='empty':lines.append('设备暂无记录；未伪造观测。')
        elif item and item.get('status')=='dry_run':lines.append('仅预演，未访问蓝牙或建立档案。')
        elif item:
            lines.append('记录已正式保存，可查看、导出或备份。')
            if item.get('last_timestamp'):lines.append('末条设备记录时间：'+item['last_timestamp'])
        for row,line in enumerate(lines[:5],height-8):self.put(row,line)
        retry=any(f.get('retryable') for f in view['failures'])
        self.put(height-2,('r 仅重试未完成项 · ' if retry else '')+'l 日志 · Esc / Enter 返回')
        self.put(height-1,'g 读取当前设备核对 · l 日志 · b 数据库备份' if view['action']=='set-batch' else 'v 查看当前设备记录 · e 导出 · b 数据库备份',curses.A_DIM)

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
