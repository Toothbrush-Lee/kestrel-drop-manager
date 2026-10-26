"""Cancelable CLI worker and validated JSON-lines transport for the TUI."""
import json
import math
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import threading


def validate_event(event):
    if not isinstance(event,dict) or type(event.get('version')) is not int or event['version']!=1:
        raise ValueError('unsupported event version')
    kind=event.get('event')
    required={
        'phase':('phase',), 'progress':('received','expected','rate','elapsed','eta'),
        'log':('message',), 'error':('code','message','action','retryable'),
        'completed':('serial','status','added_records','total_records','pending_records'),
        'result':('data',), 'sample':('serial','measurements'), 'finished':('code',)}
    if kind not in required or any(key not in event for key in required[kind]):
        raise ValueError('incomplete event')
    for key in ('serial','phase','message','action','status'):
        if key in event and event[key] is not None and not isinstance(event[key],str):
            raise ValueError('invalid text field '+key)
    for key in ('received','expected','added_records','total_records','pending_records'):
        if key in event and (type(event[key]) is not int or event[key]<0):
            raise ValueError('invalid count '+key)
    if kind=='progress':
        for key in ('rate','elapsed','eta'):
            value=event[key]
            if key=='eta' and value is None: continue
            if type(value) not in (int,float) or not math.isfinite(value) or value<0:
                raise ValueError('invalid progress '+key)
    if kind=='error':
        if not isinstance(event['code'],str): raise ValueError('invalid error code')
        if type(event['retryable']) is not bool: raise ValueError('invalid retryability')
    elif kind in ('completed','finished') and 'code' in event:
        if type(event['code']) is not int: raise ValueError('invalid exit status')
        if kind=='completed' and event['code']<0: raise ValueError('invalid completion code')
    return event


def validate_result(action, data):
    if action in ('scan','list','records'):
        if not isinstance(data,list) or any(not isinstance(item,dict) for item in data):
            raise ValueError('invalid list result')
    elif not isinstance(data,dict):
        raise ValueError('invalid object result')
    if action=='download':
        outcomes=data.get('downloads')
        if not isinstance(outcomes,list): raise ValueError('missing download outcomes')
        for item in outcomes:
            if not isinstance(item,dict) or not isinstance(item.get('serial'),str) or not isinstance(item.get('status'),str):
                raise ValueError('invalid device outcome')
            for key in ('added_records','total_records','pending_records'):
                if key in item and (type(item[key]) is not int or item[key]<0): raise ValueError('invalid result count')
    if action=='set' and (not isinstance(data.get('before'),dict) or not isinstance(data.get('after'),dict)):
        raise ValueError('configuration result lacks before/after state')
    return data


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
                    validate_event(event)
                    if event['event']=='result': validate_result(self.arguments[0],event['data'])
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
        if self.cancelled:
            return
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
