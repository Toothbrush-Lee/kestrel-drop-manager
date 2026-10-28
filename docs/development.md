# 开发与维护规范

当前版本 0.13.0，SQLite schema 4。用户操作见[用户手册](user-guide.md)，命令见[CLI](cli.md)。本规范用于后续迭代，避免代码、观测和研究材料重新混杂。

## 目录职责

| 位置 | 内容与维护原则 |
| --- | --- |
| kestrel.py、Kestrel.command | 日常启动入口，不放业务逻辑 |
| kestrel_drop/ | 可导入 Python 源码 |
| tests/ | 离线模拟、故障注入与交互回归 |
| scripts/ | 协议研究和诊断入口，不替代日常管理器 |
| docs/ | 用户、开发、协议、存储文档和各版本验收 |
| data/originals/ | 冻结原始资料，保留校验和，不追加观测 |
| data/analysis/主题-日期/ | 同次研究的报告、日志和核查结果 |
| data/screenshots/ | 本地界面截图 |
| KestrelDocuments/ | 官方原始文档和配套资料，不是运行依赖 |
| archive/ 或用户配置位置 | 运行数据库、导出、备份与迁移原件 |
| .venv/ | uv 管理的依赖环境，保留以复用 |
| /tmp/ | 一次性测试、构建和界面探针 |

根目录只放入口、总览与配置。保留现有证据路径，不因整理目录搬动 legacy、原件、数据库或备份。Finder 的 .DS_Store、项目源码 __pycache__ 可删除再生；不清理依赖环境内部文件。禁止把“看起来旧”当作删除依据。

## 模块边界

| 模块 | 职责 |
| --- | --- |
| manager.py | CLI 参数、业务编排、批次与 Library 门面 |
| devices.py | 广播、型号、GATT 身份核验与定位 |
| protocol.py、layouts.py | 帧校验、传输、型号记录布局；保留旧格式诊断能力 |
| control.py | 官方设置、校时与实时测量 |
| database.py | 原始记录、正式观测、暂存及审计的一致存储 |
| storage.py | 位置、目录写锁、迁移、备份恢复 |
| events.py、errors.py | 结构化事件、错误分类与用户建议 |
| tui.py | 页面状态、键盘路由与渲染 |
| worker.py | 可取消后台进程、JSON Lines 校验及回收 |
| batch_settings.py | 明确目标、逐台设置、隔离失败、取消报告及复用目录写锁 |
| ui_actions.py | 配置预览和失败重试规划，不做 I/O |
| ui_text.py | 中文终端宽度、折行与编辑窗口，不依赖 curses |

已知地址用 Bleak 的 find_device_by_filter，在目标出现时结束；保留同一轮广播供地址失效时回退。没有地址的编号查询完整扫描以检查歧义，不能为提速选择第一个同编号广播。进程间不序列化 native BLEDevice，不持久化它冒充可复用连接。

界面不直接操作蓝牙或数据库写事务，通过后台 CLI 取得结果。绘制层不得发起 I/O。页面绘制与键盘优先级一致：配置确认 → 表单 → 帮助/日志 → 结果 → 记录 → 批量设置 → 设备页 → 列表。主列表 Esc 不退出，q 才退出；后台取消必须等连接清理。配置结果不确定时要求回读，不自动重试写入。

## 本地开发和验证

```bash
uv sync --locked
uv run --locked python -B tests/run.py
python3 -B tests/run.py -q
```

统一使用 tests/run.py：它在临时目录中设置 KESTREL_DATA_DIR 与 KESTREL_CONFIG_FILE；不要用可能沿用真实 archive 的裸测试发现命令。新测试必须显式使用临时 Store/Library，或只构造不执行 I/O 的界面状态。禁止为了测试方便下载到正式库、改设备时间、重复配置或清空设备。

测试覆盖协议错误、持久化/ACK 顺序、恢复边界、迁移回滚、备份恢复、并发隔离、配置回读，以及最小终端、百台结果、中文编辑和失败重试。新增测试应验证实际风险与行为契约，不复刻实现细节。模拟通过不能替代所有固件的实机验证；报告须区分模拟、PTY 界面检查、只读库检查和硬件实测。

修改文档后检查相对链接，修改参数后核对 help；涉及存储需确认旧库兼容和失败回滚。测试完成保留摘要到 docs/validation-版本.md；大日志/实机证据进入本地 data/analysis，不提交到 Git。

## 版本和 Git

代码、测试、锁定依赖和 Markdown 文档进入 Git。数据库、备份、导出、抓包、官方原件和个人资料已被 .gitignore 排除；Git 不承担观测备份。

```bash
git status --short
git diff --check
git diff
git log --oneline -8
```

功能分支使用 codex/ 前缀；提交前核对文件清单，避免 git add . 意外加入新资料类型。版本同步更新 pyproject.toml、uv.lock、kestrel_drop/__init__.py，CHANGELOG 描述用户可见结果。仅新增 UI 与文档无需提升数据库 schema。源码发布和数据备份分别完成；没有远程仓库时，本地提交不会自动上传。

依赖由 uv.lock 固定，复现用 --locked；已缓存时可加 --offline。不把 .venv 提交到仓库，不为整理工作目录重复安装依赖。本地提供的官方 EXE/MSI 是资料，不执行它们来验证本项目。

批量范围不能通过空集合隐式扩大。空格和 A/Ctrl+A 是选择操作；Enter/a 是执行操作，所有目标在提交表单前固定。重新扫描清空旧勾选。批量配置复用同一个目录锁，单台配置调用在同一 collection 上下文中跳过二次抢锁，仍保留逐台身份核验与审计。

0.11 性能边界：status(serial) 与全库查询共享同一校验路径，仅缩小设备范围；iter_query 在一个只读事务中按 1,000 行提取，消费者关闭时释放连接。TUI 只绘制 active_drawer 对应的顶层页面，以 needs_redraw 标记输入、事件和窗口变化；文本缓存按内容及宽度失效，关闭文本时释放。设备稳定字段只在单次配置连接内复用，不跨设备/连接缓存。

0.12 的完整校验按 1,000 条一批验证原始字节与解析值，逐段检查游标连续性；pending 逐块解码并校验摘要。恢复旧备份只读检查时不改变 journal 模式，初始化为运行库时再启用 WAL；备份保持 DELETE 单文件。schema 2/3 与 4 的对象检查按版本区分。新增字段或索引时同步维护 SCHEMA 和对应的兼容测试。

基础静态检查使用 `uv run --locked --group lint ruff check .`；lint 依赖独立分组，普通用户启动不安装。CI 同时覆盖发布使用的 Python 3.13。BleSession 必须显式传型号；旧 CSV 诊断仅通过 LegacyBleSession 使用旧方言，不能作为新采集功能入口。
