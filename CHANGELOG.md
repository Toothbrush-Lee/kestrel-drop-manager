# CHANGELOG

## 0.5.0 — 2026-10-23

- 合并本轮 0.4.1 可靠性补强与 0.5 下载结果交互：接收时即检查从零开始的连续块序号、记录顺序、CRC 错误和基本完成边界，异常块不落入可续传范围，也不确认接收。
- 增量暂存增加 `safety_version=2`、按会话记录的接收证据，以及 fsync 后发布的字节数/摘要/条数/游标检查点；检查点发布前不向设备 ACK。
- 写入超出已发布检查点的尾部会先保留到 `.unconfirmed-*.bin`，再恢复已验证边界。旧版没有检查点的非空未完成暂存拒绝猜测续传位置。
- 覆盖创建空暂存、提交状态后清理 journal 等崩溃窗口；原子元数据写入使用唯一临时文件并同步目录。
- 新增 `--events-json` JSON Lines 事件协议，TUI 按事件显示扫描、连接、核对、接收、校验和入库阶段，不再解析进度日志文本。
- 下载完成后显示新增、累计、未确认条数及时间范围；错误提供中文原因、处理建议和显式重试入口，不自动重试不明写入。
- 官方 CSV 比对保留重复时间戳的每次出现，不复用同一参考记录，缺失测量值不能仅凭时间戳通过验证。
- 新增 34 项测试，共 94 项；实体 D3 经 CLI / TUI 两次增量分别接收 2 条和 1 条，累计 6,113 条、游标 114431、未确认 0 条，真实终端结果页显示正常。

## 0.4.0 — 2026-10-23

- 新增 `archive/library.sqlite`：集中保存设备、带绝对游标的观测记录及入库批次。
- 从已校验提交的 CSV 自动事务入库；重复刷新不会重复插入，历史冲突拒绝覆盖，中断后可重放补入库。
- 记录预览与 CSV 导出改用 SQLite；导出包含序列号和绝对游标，结束日期只写日期时包含当天整天。
- 新增 `db` 校验入库与 `backup` 一致性备份命令，TUI 增加 `b` 备份按键。
- 保留现有 CSV/JSON 下载恢复机制；数据库备份可独立预览导出，继续下载需要完整恢复档案。
- 补充新手使用、备份恢复和存储设计说明；新增 12 项数据库测试，共 60 项。
- 初始化本地 Git 管理，排除个人观测、抓包、数据库、备份和环境缓存。

## 项目整理 — 2026-10-23

- 将管理、TUI 和协议源码统一收进 `kestrel_drop/`；根目录保留日常启动入口。
- 将底层诊断入口移至 `scripts/`，技术说明拆分到 `docs/protocol.md`。
- 将原始下载与抓包分析结果分别整理至 `data/originals/`、`data/analysis/`，添加来源说明和原始文件校验和。
- 保持运行档案 `archive/` 的路径和内容不变，同步更新导入、后台任务及测试路径。

## 0.3.0 — 2026-10-23

- Added `kestrel.py` with a Chinese curses TUI and scan/list/status/download/import/records/export commands.
- Added automatic per-serial library discovery, compatibility with existing flat archives, and sequential fleet synchronization.
- Added cancellable background TUI jobs, live progress, record preview, filtered CSV export, and scrollable help/logs.
- First full downloads now fsync validated blocks before ACK; interrupted first-download evidence is retained while retries conservatively restart from cursor zero.
- Publish new archives as a complete directory and recover verified first downloads after interruption before publication.
- Added a macOS double-click launcher and 17 offline integration tests (48 total).

## 0.2.0 — 2026-10-22

- Added per-device incremental `adopt`, `sync`, and `status` commands.
- CRC-checked blocks flushed/fsynced to a recoverable CSV spool before protocol ACK.
- Atomic canonical CSV publication with SHA-256 manifest and recovery journal.
- Persists confirmed absolute log cursor; does not collapse legitimate duplicate timestamps.
- Supports serial-based BLE discovery on macOS without mandatory explicit UUID.
- Added 31 integration/unit tests including BLE simulation, exclusive file locking and crash recovery.
- Note: live incremental sync still requires direct D3 hardware verification.
