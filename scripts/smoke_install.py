"""Test a wheel/native install away from checkout, including real TUI worker IPC."""
import argparse
import errno
import fcntl
import struct
import termios
import re
import json
import os
from pathlib import Path
import pty
import select
import subprocess
import tempfile
import time


def smoke(prefix):
    with tempfile.TemporaryDirectory(prefix='kestrel-install-smoke-') as temporary:
        root = Path(temporary)
        env = {key: value for key, value in os.environ.items() if key not in ('PYTHONPATH','PYTHONHOME')}
        env.update(KESTREL_DATA_DIR=str(root / 'data'), KESTREL_CONFIG_FILE=str(root / 'settings.json'),
                   TERM='xterm-256color', LANG='en_US.UTF-8', PYTHONUTF8='1')
        def run(*args):
            return subprocess.run([*prefix,*args], cwd=root, env=env, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True, timeout=30).stdout
        assert re.search(r'\d+\.\d+\.\d+', run('--version'))
        stream = [json.loads(line) for line in run('--events-json','list').splitlines()]
        assert any(item['event']=='result' and item['data']==[] for item in stream), stream
        assert stream[-1]['event']=='finished' and stream[-1]['code']==0, stream
        master, slave = pty.openpty()
        fcntl.ioctl(slave, termios.TIOCSWINSZ, struct.pack('HHHH', 32, 120, 0, 0))
        process = subprocess.Popen(prefix, cwd=root, env=env, stdin=slave, stdout=slave, stderr=slave)
        os.close(slave)
        def wait_for(text):
            buffer=b''; deadline=time.monotonic()+30
            while time.monotonic()<deadline:
                if not select.select([master],[],[],0.2)[0]:continue
                try: chunk=os.read(master,65536)
                except OSError as exc:
                    if exc.errno==errno.EIO:break
                    raise
                if not chunk:break
                buffer+=chunk
                if text.encode() in buffer:return
            raise RuntimeError(f'TUI did not show {text!r}: {buffer[-4000:]!r}')
        try:
            wait_for('已读取 0 个设备')
            os.write(master,b'?');wait_for('帮助 / 任务日志')
            os.write(master,b'\x1b');wait_for('KESTREL DROP')
            os.write(master,b'q')
            deadline = time.monotonic() + 15
            while process.poll() is None and time.monotonic() < deadline:
                if select.select([master], [], [], 0.2)[0]:
                    try: os.read(master, 65536)
                    except OSError as exc:
                        if exc.errno != errno.EIO: raise
            assert process.wait(timeout=1) == 0
        finally:
            if process.poll() is None:
                process.terminate()
                try: process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill(); process.wait(timeout=2)
            os.close(master)
        assert not (root/'data').exists(), 'Read-only smoke created data directory'
    print('Install smoke passed: CLI, platform imports, JSON events, TUI worker, help, exit')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    group=parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--executable',type=Path)
    group.add_argument('--python',type=Path)
    args=parser.parse_args()
    prefix=[str(args.executable.resolve())] if args.executable else [str(args.python.absolute()),'-m','kestrel_drop']
    smoke(prefix)


if __name__=='__main__':main()
