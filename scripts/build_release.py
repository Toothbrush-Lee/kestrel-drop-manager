"""Build a native console distribution from source; temporary intermediates only."""
import argparse
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from kestrel_drop import __version__


def native_platform():
    system = {'darwin': 'macos', 'linux': 'linux'}.get(sys.platform)
    architecture = {'arm64': 'arm64', 'aarch64': 'arm64', 'x86_64': 'x86_64', 'AMD64': 'x86_64'}.get(platform.machine())
    if system is None or architecture is None:
        raise ValueError('Native packages support macOS/Linux on arm64 or x86_64')
    return f'{system}-{architecture}'


def build(output, expected):
    target = native_platform()
    if target != expected:
        raise ValueError(f'Runner architecture mismatch: expected {expected}, got {target}')
    name = f'KestrelDrop-{__version__}-{target}'
    output = output.resolve(); output.mkdir(parents=True, exist_ok=True)
    archive = output / (name + ('.zip' if sys.platform == 'darwin' else '.tar.gz'))
    if archive.exists(): raise FileExistsError(archive)
    with tempfile.TemporaryDirectory(prefix='kestrel-native-build-') as temporary:
        work = Path(temporary)
        subprocess.run([sys.executable, '-m', 'PyInstaller', '--noconfirm', '--clean',
                        '--distpath', str(work / 'dist'), '--workpath', str(work / 'work'),
                        str(ROOT / 'packaging/kestrel.spec')], check=True, cwd=ROOT,
                        env={**os.environ, 'PYINSTALLER_CONFIG_DIR': str(work / 'cache')})
        stage = work / name; stage.mkdir()
        runtime = stage / 'runtime'; runtime.mkdir()
        if sys.platform == 'darwin':
            shutil.copytree(work / 'dist/KestrelDrop.app', runtime / 'KestrelDrop.app', symlinks=True)
            executable = runtime / 'KestrelDrop.app/Contents/MacOS/kestrel-drop'
            launcher = stage / 'Kestrel.command'
            launcher.write_text('#!/bin/zsh\nset -eu\napp_dir=${0:A:h}\nexec "$app_dir/runtime/KestrelDrop.app/Contents/MacOS/kestrel-drop" "$@"\n')
        else:
            shutil.copytree(work / 'dist/kestrel-drop', runtime / 'kestrel-drop', symlinks=True)
            executable = runtime / 'kestrel-drop/kestrel-drop'
            launcher = stage / 'start.sh'
            launcher.write_text('#!/bin/sh\nset -eu\napp_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)\nexec "$app_dir/runtime/kestrel-drop/kestrel-drop" "$@"\n')
        launcher.chmod(0o755)
        shutil.copy2(ROOT / 'README.md', stage)
        shutil.copytree(ROOT / 'docs', stage / 'docs', ignore=shutil.ignore_patterns('__pycache__', '.DS_Store'))
        (stage / 'INSTALL.txt').write_text(
            'Kestrel DROP ' + __version__ + '\n\n'
            '无需安装 Python 或 uv。macOS 解压后双击 Kestrel.command；Linux 在终端运行 ./start.sh。\n'
            '保留整个文件夹，runtime/ 是运行依赖，不要单独搬动程序。\n'
            'macOS 包尚未使用 Apple Developer ID 签名/公证；系统阻止时按系统设置中的“仍要打开”处理。\n'
            '首次连接允许终端/程序的蓝牙权限，并先断开手机 LiNK。\n'
            '数据保存到用户数据目录，与安装文件分开；旧源码版 archive/ 用户先在原程序用 p 保存绝对数据位置。\n'
            '详见 docs/user-guide.md；可把 --help 参数传给启动脚本。\n', encoding='utf-8')
        subprocess.run([sys.executable, '-B', str(ROOT / 'scripts/smoke_install.py'),
                        '--executable', str(executable)], check=True)
        if sys.platform == 'darwin':
            subprocess.run(['codesign', '--verify', '--deep', '--strict', str(runtime / 'KestrelDrop.app')], check=True)
            subprocess.run(['ditto', '-c', '-k', '--sequesterRsrc', '--keepParent', str(stage), str(archive)], check=True)
        else:
            with tarfile.open(archive, 'w:gz') as stream: stream.add(stage, arcname=name)
    print(archive)
    return archive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--platform', required=True, choices=['macos-arm64', 'macos-x86_64', 'linux-x86_64'])
    args = parser.parse_args()
    build(args.output, args.platform)


if __name__ == '__main__':
    main()
