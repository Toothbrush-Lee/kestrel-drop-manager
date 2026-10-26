"""Data location, directory writer lock, and explicit legacy migration.

Legacy recovery runs only on a disposable copy. Source files are retired only
after a verified current-schema database is durably published, and only if unchanged.
"""
from contextlib import contextmanager, ExitStack, closing
from datetime import datetime
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import sys
import tempfile
import uuid

from . import protocol
from .database import Store, SCHEMA_VERSION, MEASUREMENTS, checked_rows

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def config_path():
    return Path(os.environ.get('KESTREL_CONFIG_FILE', Path.home() / '.config' / 'kestrel-drop' / 'settings.json')).expanduser()


def default_root():
    if os.environ.get('KESTREL_DATA_DIR'):
        return Path(os.environ['KESTREL_DATA_DIR']).expanduser()
    settings = config_path()
    if settings.exists():
        return Path(json.loads(settings.read_text())['data_dir']).expanduser()
    # Existing installs keep their location until the user explicitly relocates.
    legacy = PROJECT_ROOT / 'archive'
    if (legacy / 'library.sqlite').exists() or any(legacy.glob('*.csv.sync-state.json')):
        return legacy
    return (Path.home() / 'Library' / 'Application Support' / 'KestrelDrop' if sys.platform == 'darwin'
            else Path(os.environ.get('XDG_DATA_HOME', Path.home() / '.local' / 'share')) / 'kestrel-drop')


def configure(path):
    destination = Path(path).expanduser().resolve()
    settings = config_path()
    settings.parent.mkdir(parents=True, exist_ok=True)
    protocol._json_atomic(settings, {'data_dir': str(destination)})
    return destination


@contextmanager
def writer_lock(root):
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    fd = os.open(root, os.O_RDONLY)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError('Another download or migration is using this data directory') from None
        yield
    finally:
        os.close(fd)


def legacy_archives(root):
    result = {}
    root = Path(root)
    for path in sorted(root.glob('*.csv.sync-state.json')) + sorted(root.glob('*/history.csv.sync-state.json')):
        state = json.loads(path.read_text())
        serial = str(state['serial'])
        if serial in result:
            raise RuntimeError(f'设备 {serial} 存在重复旧档案')
        result[serial] = Path(str(path).removesuffix('.sync-state.json'))
    return result


def legacy_files(root):
    root = Path(root)
    files = []
    for entry in root.iterdir():
        if entry.name in ('legacy', 'exports', 'backups') or entry.name.startswith('library.sqlite'):
            continue
        if entry.is_symlink():
            raise RuntimeError(f'旧档案存在符号链接，迁移已停止：{entry}')
        if entry.is_file() and ('.csv' in entry.name or entry.name.endswith('.lock')):
            files.append(entry)
        elif entry.is_dir() and (entry.name in ('.downloads', '.imports') or entry.name.isdigit()):
            for path in entry.rglob('*'):
                if path.is_symlink(): raise RuntimeError(f'旧档案存在符号链接：{path}')
                if path.is_file(): files.append(path)
    return files


def _snapshot_database(source, target):
    with closing(sqlite3.connect(source.as_uri() + '?mode=ro', uri=True)) as src, closing(sqlite3.connect(target)) as dst:
        src.backup(dst)
        dst.execute('PRAGMA journal_mode=DELETE')
        if dst.execute('PRAGMA integrity_check').fetchone()[0] != 'ok' or dst.execute('PRAGMA foreign_key_check').fetchall():
            raise RuntimeError('旧数据库完整性检查失败')
    with target.open('rb') as stream: os.fsync(stream.fileno())


def _retire(root, snapshot, manifest):
    """Idempotent cosmetic cleanup; never remove a subsequently changed file."""
    leftovers = []
    for name, digest in manifest.items():
        relative = Path(name)
        if relative.is_absolute() or '..' in relative.parts:
            raise RuntimeError('迁移清单包含非法路径，停止清理')
        source = root / relative
        frozen = snapshot / 'source' / relative
        if not source.resolve().is_relative_to(root) or not frozen.resolve().is_relative_to(snapshot / 'source'):
            raise RuntimeError('迁移清单路径离开数据目录，停止清理')
        if not source.exists(): continue
        if source.is_symlink() or protocol._sha256(source) != digest or protocol._sha256(frozen) != digest:
            leftovers.append(name)
            continue
        source.unlink()
    for path in sorted(root.rglob('*'), key=lambda p: len(p.parts), reverse=True):
        if path.is_dir() and path != snapshot and (path.name in ('.downloads', '.imports') or path.relative_to(root).parts[0] in ('.downloads', '.imports') or path.relative_to(root).parts[0].isdigit()):
            try: path.rmdir()
            except OSError: pass
    protocol._fsync_directory(root)
    return leftovers


def migrate(root):
    root = Path(root).expanduser().resolve()
    database = root / 'library.sqlite'
    if database.is_symlink(): raise RuntimeError('数据库是符号链接，请先独立核验位置')
    with writer_lock(root), ExitStack() as locks:
        if database.exists():
            with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as connection:
                version = connection.execute('PRAGMA user_version').fetchone()[0]
            if version in (2, 3, SCHEMA_VERSION):
                result = Store(database, create=version in (2, 3)).check()
                leftovers = []
                for manifest_path in (root / 'legacy').glob('migration-*/manifest.json'):
                    meta = json.loads(manifest_path.read_text())
                    # Cleanup belongs only to the database produced by this run.
                    with Store(database).connect() as connection:
                        found = connection.execute('SELECT 1 FROM transfers WHERE validation_json LIKE ?', ('%"migration_id": "' + meta['migration_id'] + '"%',)).fetchone()
                    if found: leftovers.extend(_retire(root, manifest_path.parent, meta['files']))
                return dict(result, integrity='ok', already_migrated=True, retained_changed_files=leftovers)
            if version != 1: raise RuntimeError('未知数据库版本，拒绝迁移')
        archives = legacy_archives(root)
        # Synchronize with old releases too. A busy legacy writer aborts migration.
        serials = set(archives)
        for directory in (root / '.downloads').glob('*'):
            if directory.is_dir() and directory.name.isascii() and directory.name.isdigit(): serials.add(directory.name)
        if database.exists():
            with closing(sqlite3.connect(database.as_uri() + '?mode=ro', uri=True)) as connection:
                serials.update(str(r[0]) for r in connection.execute('SELECT serial FROM devices'))
        for serial in sorted(serials):
            if not serial.isascii() or not serial.isdigit(): raise RuntimeError('旧档案序列号无效')
            locks.enter_context(protocol._archive_lock(root / f'.device-{serial}'))
        for path in archives.values(): locks.enter_context(protocol._archive_lock(path))
        run_id = uuid.uuid4().hex
        legacy = root / 'legacy'
        legacy.mkdir(exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix='.migration-', dir=legacy))
        frozen = legacy / ('migration-' + datetime.now().strftime('%Y%m%d-%H%M%S') + '-' + run_id[:8])
        try:
            sources = staging / 'source'
            sources.mkdir()
            manifest = {}
            for source in legacy_files(root):
                relative = source.relative_to(root)
                target = sources / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(source, target)
                with target.open('rb') as stream: os.fsync(stream.fileno())
                digest = protocol._sha256(target)
                if digest != protocol._sha256(source): raise RuntimeError('迁移期间旧档案发生变化')
                manifest[str(relative)] = digest
            if database.exists(): _snapshot_database(database, staging / 'library-v1.sqlite')
            work = staging / 'work'
            shutil.copytree(sources, work)
            target_store = Store(staging / 'new.sqlite', create=True)
            imported = {}
            to_import = legacy_archives(work)
            for candidate in sorted((work / '.downloads').glob('*/*/history.csv')):
                evidence = Path(str(candidate) + '.json')
                if not evidence.exists(): continue
                state = protocol.adopt_archive(candidate, evidence)
                serial = state['serial']
                if serial in to_import:
                    raise RuntimeError('同一设备同时存在正式档案和已完成首次下载，请独立核验')
                to_import[serial] = candidate
            for serial, output in to_import.items():
                paths = protocol._archive_paths(output)
                state = protocol._check_archive_or_recover(paths, protocol._state_loaded(paths))
                names, rows = protocol._rows_checked(output)
                if len(rows) != state['row_count'] or names != state['fieldnames']:
                    raise RuntimeError('旧档案记录数或表头与状态不一致')
                target_store.import_rows(serial, state['model'], rows, state['next_cursor'],
                    {'migration_id': run_id, 'source_sha256': state['csv_sha256']})
                journal, pending = protocol._pending_loaded(paths, state)
                if pending:
                    offset = 0
                    for session in journal['transfer_sessions']:
                        saved = session['saved_records']
                        batch = pending[offset:offset+saved]
                        transfer_id = target_store.begin_transfer(serial, session['requested_cursor'], saved)
                        for seq in range(session['validated_blocks']):
                            block = [dict(r, block=int(r['block']), position_in_block=int(r['position_in_block'])) for r in batch if int(r['block']) == seq]
                            target_store.append_block(transfer_id, block, [{'block': seq, 'records': len(block)}])
                        offset += saved
                    if offset != len(pending): raise RuntimeError('旧待确认记录的会话证据不完整')
                imported[serial] = rows
            # A v1-only backup has enough committed cursor/raw evidence to migrate.
            old_db = staging / 'library-v1.sqlite'
            if old_db.exists():
                with closing(sqlite3.connect(old_db)) as old:
                    old.row_factory = sqlite3.Row
                    if old.execute('SELECT count(*) FROM observations').fetchone()[0] != old.execute('SELECT coalesce(sum(row_count),0) FROM devices').fetchone()[0]:
                        raise RuntimeError('旧数据库存在孤立记录或统计冲突')
                    for device in old.execute('SELECT * FROM devices'):
                        saved = old.execute('SELECT * FROM observations WHERE serial=? ORDER BY absolute_cursor', (device['serial'],)).fetchall()
                        if (len(saved) != device['row_count'] or len(saved) != device['next_cursor'] - device['first_cursor'] or
                                any(r['absolute_cursor'] != device['first_cursor'] + i for i, r in enumerate(saved))):
                            raise RuntimeError('旧数据库记录数或游标不连续')
                        rows = []
                        for r in saved:
                            row = json.loads(r['row_json'])
                            decoded = protocol.decode_record(r['record_raw'], block=r['block'], position=r['position_in_block'])
                            if any(r[k] != decoded[k] for k in (*MEASUREMENTS, 'timestamp_device_local', 'unknown_byte')) or row['record_hex'] != r['record_raw'].hex():
                                raise RuntimeError('旧数据库解析字段或原始字节冲突')
                            rows.append(row)
                        checked = checked_rows(rows)
                        if any(any(d[k] != r[k] for k in ('block','position_in_block')) for (_, d), r in zip(checked, saved)):
                            raise RuntimeError('旧数据库行信息与块位置冲突')
                        if device['next_cursor'] - device['first_cursor'] != device['row_count']:
                            raise RuntimeError('旧数据库起止游标与记录数不一致')
                        if device['serial'] not in imported:
                            target_store.import_rows(device['serial'], device['model'], rows, device['next_cursor'], {'migration_id': run_id, 'source': 'v1 database snapshot'})
                        else:
                            new = target_store.query(device['serial'])
                            if len(new) < len(saved) or any(n['absolute_cursor'] != r['absolute_cursor'] or n['record_hex'] != r['record_raw'].hex() for n, r in zip(new, saved)):
                                raise RuntimeError('旧数据库与 CSV 历史冲突，迁移已停止')
            # Unknown initial cursor evidence is frozen, never assigned guessed IDs.
            target_store.check()
            target_store.backup(staging / 'ready.sqlite')
            (staging / 'manifest.json').write_text(json.dumps({'migration_id': run_id, 'files': manifest}, ensure_ascii=False, indent=2))
            for path in staging.rglob('*'):
                if path.is_file():
                    with path.open('rb') as stream: os.fsync(stream.fileno())
            for directory in sorted((p for p in staging.rglob('*') if p.is_dir()), key=lambda p: len(p.parts), reverse=True):
                protocol._fsync_directory(directory)
            protocol._fsync_directory(staging)
            shutil.rmtree(work)
            for suffix in ('', '-wal', '-shm'):
                (staging / ('new.sqlite' + suffix)).unlink(missing_ok=True)
            os.rename(staging, frozen)
            protocol._fsync_directory(legacy)
            if database.exists():
                # Replacing a live WAL main file is unsafe. Require exclusive mode
                # change after checkpoint; a reader/writer from an old app aborts.
                with closing(sqlite3.connect(database, timeout=0)) as old:
                    if old.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0]:
                        raise RuntimeError('旧数据库仍被使用，请关闭其他程序再迁移')
                    if old.execute('PRAGMA journal_mode=DELETE').fetchone()[0] != 'delete':
                        raise RuntimeError('旧数据库仍被使用')
                    old.execute('BEGIN EXCLUSIVE')
                    def logical_digest(connection):
                        digest = hashlib.sha256()
                        for statement in connection.iterdump():
                            digest.update(statement.encode('utf-8')); digest.update(b'\n')
                        return digest.digest()
                    with closing(sqlite3.connect((frozen / 'library-v1.sqlite').as_uri() + '?mode=ro', uri=True)) as snapshot:
                        if logical_digest(old) != logical_digest(snapshot):
                            raise RuntimeError('迁移期间旧数据库发生变化，未切换，请重新迁移')
                    os.replace(frozen / 'ready.sqlite', database)
            else:
                os.replace(frozen / 'ready.sqlite', database)
            protocol._fsync_directory(root)
            leftovers = _retire(root, frozen, manifest)
            for path in frozen.rglob('*'):
                if path.is_file(): path.chmod(0o444)
            result = Store(database).check()
            return dict(result, integrity='ok', legacy=str(frozen), retained_changed_files=leftovers)
        except BaseException:
            # Keep failed migration evidence for diagnosis; sources stay untouched.
            raise


def restore(source, root):
    """Restore a verified snapshot into an empty location, never over live data."""
    source, root = Path(source).expanduser().resolve(), Path(root).expanduser().resolve()
    Store(source).check()
    with writer_lock(root):
        if any(root.iterdir()):
            raise FileExistsError('恢复目标必须是空目录；请选择新的 --data-dir')
        destination = Store(source).backup(root / 'library.sqlite')
        return dict(Store(destination).check(), integrity='ok', restored=str(destination))
