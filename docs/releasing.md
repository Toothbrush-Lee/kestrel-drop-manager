# 自动构建与发布

## 普通用户

在仓库 Releases 页面选择电脑对应的 ZIP / tar.gz 独立运行包。Mac 分 M 系列（arm64）和 Intel（x86_64）；Linux 提供 x86_64。解压后保留整个文件夹，Mac 双击 Kestrel.command，Linux 运行 ./start.sh，不需要安装 Python。whl 是 Python 安装包，另一个小体积 tar.gz 是源码包，普通用户无需下载这两个。

仓库现已公开，可直接下载。发布文件不含采集数据、配置、账号凭据、官方协议文件。GitHub 自动附加的 Source code 也只包含 Git 中的源文件。

Mac 包有 ad-hoc 签名以满足运行时要求，但没有 Apple Developer ID 签名和公证；不等同于系统信任的已公证应用。首次打开可能需在系统设置“隐私与安全性”允许打开。蓝牙操作还需授权，手机 LiNK 应先断开。当前 CI 在 macOS 15 和 Ubuntu 22.04 检查启动，不能据此保证更旧系统可用。Linux 还需要系统 BlueZ 服务和蓝牙访问权限。

升级前备份 SQLite；只替换程序文件夹即可，用户数据目录不随程序包删除。旧源码版如果使用项目 archive/，请先在旧版按 p 保存其绝对路径，然后启动新程序并核对顶部目录。保存位置不会自动迁移文件。

每次发布附 SHA256SUMS。在下载目录执行 macOS 的 `shasum -a 256 -c SHA256SUMS`，或 Linux 的 `sha256sum -c SHA256SUMS`；只下载一个包时其他未下载文件会提示缺失，核对自己下载的文件为 OK 即可。校验用于发现文件损坏，不替代发布来源的验证。

## 自动化规则

- 推送任意分支或提交 Pull Request：CI 在 Linux Python 3.11/3.13/3.14、macOS Python 3.14 运行隔离测试；另构建 wheel 和源码包，在独立环境安装 wheel 并测试真实 TUI 后台通信。
- 推送数字版本标签，例如 v0.12.0：Release 先调用同样的 CI，再构建 macOS arm64、macOS x86_64、Linux x86_64 独立运行包，使用 Python 3.13 和锁定的构建依赖。
- Actions → Release → Run workflow：仅测试并构建，产物在该次运行的 Artifacts，保留 7 天，不创建 Release。
- 标签发布要求 pyproject.toml、kestrel_drop/__init__.py、uv.lock 与标签版本一致；必须有 CHANGELOG 对应条目。
- 全部任务成功后检查五个发布包齐全，生成 SHA256SUMS，先上传到草稿，完成后才发布。
- 发布失败可重跑失败任务，复用同一运行的成功产物。过了 7 天产物过期需重跑整个流程。仅允许补全草稿，不覆盖已经发布的版本。修复已发布版本必须升级版本号，不移动旧标签。
- 流程使用仓库自带 GITHUB_TOKEN；只有发布任务有 contents: write，其他任务只读。不需要 PAT、PyPI token，也不会上传到 PyPI。官方 Actions 固定提交 SHA。
- 三个平台仅在标签发布或手动构建时打包；分支与 PR 先做单元测试、基础静态检查和 wheel 安装冒烟。

## 维护者发布步骤

先更新 pyproject.toml、kestrel_drop/__init__.py 的版本和 CHANGELOG，再更新锁文件并测试：

```sh
uv lock
uv run --locked python -B scripts/check_release.py version
uv run --locked python -B tests/run.py -q
```

提交到分支，推送，等 CI 通过后合入 main。确认 main 已包含正确版本，然后：

```sh
git switch main
git pull --ff-only
git tag v0.12.0
git push origin v0.12.0
```

示例版本需替换为本次新版本。到 Actions 检查 Release 所有任务通过，再到 Releases 检查三个平台包、wheel、源码包、SHA256SUMS 共六个附件。不能只看到标签存在就认为已发布。

本地复现：

```sh
uv sync --locked --group build
uv build --no-build-isolation --out-dir /tmp/kestrel-python-dist
uv run --locked python -B scripts/check_release.py audit /tmp/kestrel-python-dist
uv run --locked --group build python -B scripts/build_release.py --output /tmp/kestrel-native-dist --platform macos-arm64
```

最后一项根据本机选择 macos-arm64、macos-x86_64 或 linux-x86_64，不能跨平台编译。已有同名输出时会拒绝覆盖；使用新的空输出目录。构建中间文件和 PyInstaller 缓存位于临时目录，结束后自动清理。dist/ 和 build/ 已被 Git 忽略，正常工作目录无需保留构建产物。

## 验证范围

自动测试只使用临时数据和模拟蓝牙。安装冒烟在源码目录外运行，检查版本、JSON 设备列表、真实 PTY 中 TUI 的后台加载、帮助和退出；不会扫描或修改设备。独立运行入口同时加载平台 BLE 后端，能发现缺失依赖，但不能确认真实蓝牙授权和射频连接。发布后如改动设备协议，仍须现场验证 D1/D2/D3 的下载、续传和配置回读。硬件验证应保留独立记录，不由 CI 成功代替。
