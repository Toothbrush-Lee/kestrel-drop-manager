# CHANGELOG

## 未发布 — 项目目录整理

- 根目录截图归入本地 `data/screenshots/`，D3 抓包分析产物按设备与日期归档；所有资料按字节校验后移动。
- 清理生成缓存和 Finder 元数据，补充项目目录、资料用途及文档索引；数据库、原件、导出、备份与迁移证据保持原位和内容。
- 修正 0.7 验收记录：2、3 月的记录是此前保留且尚未覆盖的历史，不能据此判断当前设备时钟状态。

## 0.7.0 — D1/D2 独立历史格式与混合采集

- 根据 D1/D2 实机字段声明与首块载荷，分别实现 9 / 16 字节解析；D3 保持 21 字节。下载开始前核对完整字段声明，异常格式拒绝确认和发布。
- schema 3 支持不同长度原始记录与缺失测量 NULL；按设备型号核验正式数据、待确认块和解码版本。schema 2 首次写入前自动备份，事务升级保留原有数据与恢复边界，失败整体回滚。
- TUI 预览缺失字段显示 --；未勾选的批量任务只处理本次扫描到的可采集设备，离线档案仍保留，可明确勾选重试。CLI --all 仍表示全部登记设备。
- 默认读取上限从 6,220 提高到 20,000，覆盖实机 D1/D2 当前历史数量。采样间隔、校时、清空日志等配置命令仍未开放。
- 新增多型号采集、NULL 字段、错误 schema、备份续传和 schema 2 升级回滚测试；详见 [验收记录](docs/validation-0.7.md)。

## 0.6.1 — 多型号身份识别与多设备采集审查

- 修正只解析 D3 编号及漏掉部分 D1 广播的问题；统一 D1/D2/D3 型号识别、只读 GATT 编号核验与本地登记。
- TUI 显示型号、编号来源、身份冲突和历史采集能力；增加 x 读编号、空格勾选、按当前设备列表批量采集。
- 批量包含空档案和首次中断设备，共享扫描快照，逐台采集，保留每台临时地址并校验重复编号/地址映射。
- 修复单台 pending 损坏阻断其他设备状态和采集的问题；强化型号匹配，取消批量后不启动后续设备。
- 174 项测试覆盖混合设备、100 台任务、地址变化与复用、身份冲突、串库防护和中断恢复。D1 2509793、D2 2525425、D3 3087666 标准 GATT 身份核验和登记实测通过。
- D1/D2 的历史记录格式仍未验证，明确阻止套用 D3 解析器。数据库保持 schema 2，无需迁移现有观测。


## 0.6.0 — SQLite 唯一存储

- 原生下载直接将 CRC 核验块事务写入 SQLite，块和最终发布均在 ACK 前持久化。
- 正式记录、下载会话、可信续传边界和暂存载荷由同一个数据库维护；删除 row_json 与运行期 CSV/JSON 双状态。
- 显式 migrate 在副本上恢复并核验旧档案，保留旧库快照、原文件和摘要清单，原子切换后冻结旧材料；清理可重试。
- 查询与刷新只读；db 单独执行完整检查。数据库备份覆盖待确认下载，restore 可恢复到空目录并继续同步。
- 新安装默认使用用户数据目录，支持保存位置、环境变量覆盖；旧安装保留原路径。TUI 增加迁移、位置及打开目录入口。
- 134 项测试在 Python 3.14 / 3.15 上通过；真实 6,116 条档案迁移逐字节一致，D3 增量新增 9 条至 6,125 条，TUI 预览、导出、备份恢复通过。详见 [验收记录](docs/validation-0.6.md)。D1/D2 仍未适配。


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
