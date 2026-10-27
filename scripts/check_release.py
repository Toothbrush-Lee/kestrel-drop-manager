"""Validate versions and package contents; prepare checksums and release notes."""
import argparse
import hashlib
from pathlib import Path, PurePosixPath
import re
import sys
import tarfile
import tomllib
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def version(tag=None):
    project = tomllib.loads((ROOT / 'pyproject.toml').read_text())['project']
    value = project['version']
    package = re.search(r"__version__\s*=\s*['\"]([^'\"]+)", (ROOT / 'kestrel_drop/__init__.py').read_text()).group(1)
    lock = tomllib.loads((ROOT / 'uv.lock').read_text())
    locked = next(p['version'] for p in lock['package'] if p['name'] == project['name'])
    if not re.fullmatch(r'\d+\.\d+\.\d+', value) or value != package or value != locked:
        raise ValueError(f'Inconsistent versions: project={value}, package={package}, lock={locked}')
    if tag is not None and tag != f'v{value}':
        raise ValueError(f'Tag {tag!r} does not match v{value}')
    return value


def audit(directory):
    value = version()
    expected = {f'kestrel_drop_manager-{value}-py3-none-any.whl',
                f'kestrel_drop_manager-{value}.tar.gz'}
    files = {p.name for p in directory.iterdir() if p.is_file() and p.name != '.gitignore'}
    if files != expected:
        raise ValueError(f'Unexpected Python artifacts: {files}')
    forbidden = {'archive', 'data', 'KestrelDocuments', 'KestrelDocument', '.git', '.venv', '__pycache__'}
    for name in sorted(expected):
        path = directory / name
        if name.endswith('.whl'):
            with zipfile.ZipFile(path) as stream:
                members = stream.namelist()
                entry = stream.read(f'kestrel_drop_manager-{value}.dist-info/entry_points.txt').decode()
                if 'kestrel-drop = kestrel_drop.manager:main' not in entry:
                    raise ValueError('Missing console entry point')
        else:
            with tarfile.open(path) as stream:
                entries = stream.getmembers()
                if any(m.issym() or m.islnk() for m in entries):
                    raise ValueError('Source distribution contains links')
                members = [m.name for m in entries]
        for member in members:
            parts = PurePosixPath(member).parts
            if forbidden.intersection(parts) or '..' in parts or member.startswith('/') or member.endswith(('.sqlite', '.db', '.pdf', '.pyc')):
                raise ValueError(f'Private/generated content in package: {member}')
        if not any(m.endswith('kestrel_drop/manager.py') for m in members):
            raise ValueError('Package missing manager')
    print('Python package contents verified')


def prepare(directory, tag, notes):
    value = version(tag)
    expected = {f'KestrelDrop-{value}-{target}{suffix}' for target, suffix in
                [('macos-arm64', '.zip'), ('macos-x86_64', '.zip'), ('linux-x86_64', '.tar.gz')]}
    expected.update({f'kestrel_drop_manager-{value}-py3-none-any.whl', f'kestrel_drop_manager-{value}.tar.gz'})
    actual = {p.name for p in directory.iterdir()}
    if actual != expected or any(not p.is_file() or p.stat().st_size == 0 for p in directory.iterdir()):
        raise ValueError(f'Release assets mismatch: expected={expected}, actual={actual}')
    lines = []
    for name in sorted(expected):
        with (directory / name).open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        lines.append(f'{digest}  {name}\n')
    (directory / 'SHA256SUMS').write_text(''.join(lines), encoding='utf-8')
    changelog = (ROOT / 'CHANGELOG.md').read_text()
    match = re.search(rf'^## {re.escape(value)}[^\n]*\n(.*?)(?=^## |\Z)', changelog, re.M | re.S)
    if not match:
        raise ValueError('Missing changelog entry')
    notes.write_text(
        f'## Kestrel DROP {value}\n\n' + match.group(1).strip() +
        '\n\n## 下载与运行\n\n'
        '- Apple Silicon Mac：macos-arm64.zip；Intel Mac：macos-x86_64.zip。解压后打开 Kestrel.command。\n'
        '- Linux x86_64：linux-x86_64.tar.gz；解压后运行 ./start.sh（Ubuntu 22.04 或更新兼容环境）。\n'
        '- 独立运行包无需 Python；whl/tar.gz 是 Python 用户的安装包/源码包。\n'
        '- Mac 包已做本地 ad-hoc 签名校验，但没有 Apple Developer ID 签名及公证；首次打开可能被系统拦截。\n'
        '- SHA256SUMS 用于校验下载完整性。软件不包含观测数据或官方协议文件。\n'
        '- CI 验证启动、TUI 后台通信与模拟测试；不替代实物蓝牙测试。\n', encoding='utf-8')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='command', required=True)
    check = sub.add_parser('version'); check.add_argument('--tag')
    check = sub.add_parser('audit'); check.add_argument('directory', type=Path)
    check = sub.add_parser('prepare'); check.add_argument('directory', type=Path)
    check.add_argument('--tag', required=True); check.add_argument('--notes', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'version': print(version(args.tag))
    elif args.command == 'audit': audit(args.directory)
    else: prepare(args.directory, args.tag, args.notes)


if __name__ == '__main__':
    main()
