"""Versioned JSON-lines events for UI workers; plain CLI output stays available."""
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from contextvars import ContextVar
import json

_sink = ContextVar('event_sink', default=None)
_context = ContextVar('event_context', default={})


def emit(event, **fields):
    sink = _sink.get()
    if sink is None:
        return False
    sink({'version': 1, 'event': event, **_context.get(), **fields})
    return True


def result(data):
    if not emit('result', data=data):
        print(json.dumps(data, ensure_ascii=False, indent=2))


@contextmanager
def context(**fields):
    token = _context.set({**_context.get(), **fields})
    try:
        yield
    finally:
        _context.reset(token)


class LogStream:
    def __init__(self, level):
        self.level, self.buffer = level, ''

    def write(self, text):
        self.buffer += text.replace('\r', '\n')
        while '\n' in self.buffer:
            line, self.buffer = self.buffer.split('\n', 1)
            if line:
                emit('log', level=self.level, message=line)
        return len(text)

    def flush(self):
        if self.buffer:
            emit('log', level=self.level, message=self.buffer)
            self.buffer = ''

    def isatty(self):
        return False


@contextmanager
def stream(output):
    def write(event):
        output.write(json.dumps(event, ensure_ascii=False, allow_nan=False) + '\n')
        output.flush()
    token = _sink.set(write)
    stdout, stderr = LogStream('info'), LogStream('error')
    try:
        with redirect_stdout(stdout), redirect_stderr(stderr):
            yield
            stdout.flush()
            stderr.flush()
    finally:
        _sink.reset(token)
