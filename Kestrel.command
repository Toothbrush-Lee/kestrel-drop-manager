#!/bin/zsh
# Double-click in Finder, or run this launcher from any directory.
set -eu
cd -- "${0:A:h}"
if command -v uv >/dev/null 2>&1; then
    exec uv run kestrel.py "$@"
elif [[ -x /opt/homebrew/bin/uv ]]; then
    exec /opt/homebrew/bin/uv run kestrel.py "$@"
else
    print '未找到 uv。请先安装 uv，再重新运行 Kestrel.command。'
    read -r '?按 Enter 退出'
    exit 1
fi
