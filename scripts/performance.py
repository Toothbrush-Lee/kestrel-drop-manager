"""Offline performance measurements; temporary databases, no BLE or configuration writes.

Run from any directory with Python 3.11+. --snapshot reads a consistent SQLite
copy of an existing archive; all measurements operate on that disposable copy.
"""
import argparse
from contextlib import closing
import cProfile
import hashlib
import io
import json
from pathlib import Path
import platform
import pstats
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
import tracemalloc

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kestrel_drop import __version__
from kestrel_drop.database import Store
from kestrel_drop.layouts import profile
from kestrel_drop.manager import Library
from kestrel_drop.tui import Terminal

RAW = bytes.fromhex('00140e160aea07f506990dfa0500d000c227360300')
LAYOUT = profile('D3')


def measure(operation, repeats=5):
    operation()  # Warm cache; initialization and cold OS caches are not measured.
    wall, cpu = [], []
    for _ in range(repeats):
        before, process = time.perf_counter(), time.process_time()
        operation()
        cpu.append((time.process_time() - process) * 1000)
        wall.append((time.perf_counter() - before) * 1000)
    return {'p50_ms': statistics.median(wall), 'max_ms': max(wall),
            'cpu_p50_ms': statistics.median(cpu), 'samples_ms': wall}


def python_peak(operation):
    tracemalloc.start()
    try:
        operation()
        return tracemalloc.get_traced_memory()[1] / 1024 / 1024
    finally:
        tracemalloc.stop()


def seed(path, devices, records):
    store = Store(path / 'library.sqlite', create=True)
    row = LAYOUT.decode(RAW, block=0, position=0)
    # Synthetic repeated timestamps are intentional: record identity is its cursor.
    # Direct inserts keep fixture creation out of the API timings; no real data.
    with store.transaction() as connection:
        for index in range(devices):
            serial = str(9100000 + index)
            connection.execute('INSERT INTO devices(serial,model) VALUES(?,?)', (serial, 'Kestrel DROP 3'))
            history = connection.execute('INSERT INTO histories(serial,first_cursor,next_cursor) VALUES(?,0,?)',
                                         (serial, records)).lastrowid
            store._insert(connection, history, 0, [(RAW, row)] * records)
    return Library(path)


def profile_text(operation):
    profiler = cProfile.Profile()
    profiler.runcall(operation)
    stream = io.StringIO()
    pstats.Stats(profiler, stream=stream).strip_dirs().sort_stats('cumulative').print_stats(12)
    return stream.getvalue()


def scenario(library):
    state = library.status()
    serials = [item['serial'] for item in state]
    serial = serials[0]
    total = state[0]['committed_records']
    output = {'devices': len(state), 'formal_records': sum(item['committed_records'] for item in state),
              'database_mib': library.database_path.stat().st_size / 1024 / 1024}
    operations = {
        'fleet_status': library.status,
        'one_device_summary': lambda: library.store.device(serial),
        'one_completion': lambda: library.complete_download(serial, previous=total),
        'recent_100_records': lambda: library.records(serial, limit=100),
        'device_overview': library.store.overview,
    }
    for name, operation in operations.items():
        output[name] = measure(operation)
    output['completion_sweep'] = measure(lambda: [library.complete_download(item) for item in serials], repeats=3)
    output['recent_100_peak_python_mib'] = python_peak(operations['recent_100_records'])
    output['full_device_query'] = measure(lambda: library.store.query(serial), repeats=3)
    output['full_device_query_peak_python_mib'] = python_peak(lambda: library.store.query(serial))
    export_index = 0
    def export():
        nonlocal export_index
        export_index += 1
        library.export(serial, library.root / 'exports' / f'test-{export_index}.csv')
    output['export_one_device'] = measure(export, repeats=3)
    output['cli_list_process'] = measure(lambda: subprocess.run(
        [sys.executable, '-B', str(ROOT / 'kestrel.py'), '--data-dir', str(library.root), 'list'],
        check=True, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE), repeats=3)
    output['completion_profile'] = profile_text(operations['one_completion'])
    return output


class NullScreen:
    """Measure Python rendering/layout only; excludes real terminal I/O."""
    def getmaxyx(self): return 24, 100
    def erase(self): pass
    def refresh(self): pass
    def addstr(self, *args): pass


def interface(path):
    terminal = Terminal(NullScreen(), Library(path))
    terminal.devices = {str(i): {'serial': str(i), 'device_type': 'D3', 'nearby': True} for i in range(100)}
    result = {'dashboard_100_devices': measure(terminal.draw, repeats=20)}
    for size in (100, 1000):
        terminal.text_view = [('设备日志 / ' + '中A' * size)[:size] for _ in range(500)]
        result[f'log_500_lines_x_{size}_characters'] = measure(terminal.draw, repeats=10)
    result['long_log_profile'] = profile_text(terminal.draw)
    return result


def transfer(path):
    library = seed(path, 2, 1000)
    store = library.store
    serial = '9100000'
    tid = store.begin_transfer(serial, 1000, 2000)
    all_rows, append_ms = [], []
    for block in range(40):
        rows = [LAYOUT.decode(RAW, block=block, position=i) for i in range(50)]
        started = time.perf_counter()
        store.append_block(tid, rows, [{'block': block, 'records': 50}])
        append_ms.append((time.perf_counter() - started) * 1000)
        all_rows.extend(rows)
    pending = measure(library.status)
    prof = profile_text(library.status)
    started = time.perf_counter()
    store.finish_transfer(tid, next_cursor=3000, elapsed_seconds=1, maxrows=20000, received_rows=all_rows)
    finish_ms = (time.perf_counter() - started) * 1000
    check = store.check()  # Verify that synthetic transfer publication is consistent.
    return {'rows': 2000, 'block_rows': 50, 'blocks': 40,
            'durable_append_p50_ms': statistics.median(append_ms), 'durable_append_max_ms': max(append_ms),
            'durable_append_sum_ms': sum(append_ms), 'pending_status': pending,
            'pending_profile': prof, 'finish_ms': finish_ms, 'verified_records': check['records']}


def pending_fleet(path):
    library = seed(path, 100, 1000)
    payload = RAW * 50
    digest = hashlib.sha256(payload).hexdigest()
    result = {}
    # Build only valid model-width, hashed pending blocks on temporary data.
    # This avoids timing fixture fsyncs as if they were part of status queries.
    for index in range(8):
        tid = library.store.begin_transfer(str(9100000 + index), 1000, 10000)
        with library.store.transaction() as connection:
            connection.executemany('INSERT INTO pending_blocks VALUES(?,?,?,?)',
                                   [(tid, block, payload, digest) for block in range(200)])
        if index in (1, 7):
            result[f'{index+1}_active_devices_x_10000_pending'] = {
                'fleet_status': measure(library.status, repeats=3),
                'completion_of_other_device': measure(lambda: library.complete_download('9100099'), repeats=3),
                'one_device_summary': measure(lambda: library.store.device('9100099'), repeats=3)}
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='New JSON file; existing files are never overwritten')
    parser.add_argument('--snapshot', type=Path, help='Optional existing library.sqlite, opened read-only')
    parser.add_argument('--small', action='store_true', help='Skip million-record scenario')
    args = parser.parse_args()
    if args.output.exists(): parser.error('output already exists')
    result = {'software': __version__, 'python': platform.python_version(), 'platform': platform.platform(),
              'sqlite': sqlite3.sqlite_version, 'method': 'Warm-cache wall and CPU samples; synthetic fixtures; no BLE; no terminal I/O; subprocess CPU is parent-only; tracemalloc reports Python allocations, not RSS'}
    with tempfile.TemporaryDirectory(prefix='kestrel-performance-') as temporary:
        directory = Path(temporary)
        if args.snapshot:
            source = args.snapshot.expanduser().resolve()
            digest = hashlib.sha256(source.read_bytes()).hexdigest()
            destination = directory / 'snapshot'
            destination.mkdir()
            with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as original, closing(sqlite3.connect(destination / 'library.sqlite')) as copy:
                original.backup(copy)
            result['archive_snapshot'] = scenario(Library(destination))
            result['source_main_file_hash_unchanged_after_copy'] = hashlib.sha256(source.read_bytes()).hexdigest() == digest
        result['scenarios'] = {}
        sizes = [(4, 10000), (100, 1000)] + ([] if args.small else [(100, 10000)])
        for count, rows in sizes:
            print(f'Measuring {count} devices × {rows} records', file=sys.stderr, flush=True)
            path = directory / f'{count}x{rows}'; path.mkdir()
            result['scenarios'][f'{count}x{rows}'] = scenario(seed(path, count, rows))
        result['interface'] = interface(directory / 'interface')
        result['transfer'] = transfer(directory / 'transfer')
        result['pending_fleet'] = pending_fleet(directory / 'pending-fleet')
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x', encoding='utf-8') as stream:
        json.dump(result, stream, ensure_ascii=False, indent=2)
        stream.write('\n')
    print(str(args.output))


if __name__ == '__main__':
    main()
