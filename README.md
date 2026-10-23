# Kestrel DROP 电脑下载管理

在电脑上扫描 Kestrel DROP、下载历史、保存到一个 SQLite 档案库，并按需要导出成 Excel 能打开的 CSV。

**日常操作：打开软件 → 选设备 → 按 Enter 下载 → 按 e 导出。** 不需要操作数据库或拼接文件。

当前版本 **0.6.1**。D1/D2/D3 均可扫描、只读核验型号与编号并登记；**历史下载当前仅验证 DROP D3**，D1/D2 会明确显示“历史待适配”，不会套用 D3 记录格式。蓝牙协议来自逆向分析，仍属于实验性实现。适用 macOS / Linux，Windows 暂不支持 curses 界面和目录锁。

## 第一次打开

电脑需要 Python 3.11 或更新版本，以及 [uv](https://docs.astral.sh/uv/)。在终端进入本项目文件夹：

```bash
uv run kestrel.py
```

macOS 也可以双击 `Kestrel.command`。首次运行 uv 会准备依赖。终端最低需要 68 列 × 18 行，建议 100 列 × 24 行。

下载前把设备放在电脑附近，打开电脑蓝牙，在手机 LiNK 中断开设备。系统询问终端蓝牙权限时允许访问。

1. 按 `s` 扫描，列表会显示 D1/D2/D3 型号、编号和采集能力。广播编号只是提示。
2. 型号或编号未知时，选择设备按 `x`，软件连接后只读核验并登记；也可以在未知编号行直接按 Enter。已识别的 D3 按 Enter 开始下载。
3. 等待结果页出现，检查“本次新增、累计、未确认、时间范围”。
4. 按 `v` 查看最近记录，或按 `e` 导出 CSV。

连接后软件会再次核对真实序列号和型号，通过后才发送历史读取指令。未命名候选设备可以按 `x` 读取型号和编号，或按 `n` 输入机身序列号。软件没有清空设备日志的命令。

已有旧版数据的用户，应先完成下一节的迁移。

## 从 0.5 或更早版本升级

**先关闭旧版 TUI 和下载程序。** 新版不会在查看数据时自动改写旧档案。

在 TUI 主界面按 `m`，或者执行：

```bash
uv run kestrel.py migrate
uv run kestrel.py db
```

迁移依次完成：

1. 保留旧 CSV、JSON、暂存证据和旧数据库快照。
2. 在副本上恢复旧版未完成的文件提交，校验每条原始记录、测量值和绝对游标。
3. 将正式记录及证据完整的增量待确认块写入新数据库。
4. 检查新库，通过后原子切换 `library.sqlite`。
5. 将旧材料归档到 `legacy/migration-日期-编号/`，文件设为只读；日常不再更新它们。

旧数据库与 CSV 冲突、记录损坏、暂存缺少可信检查点时，迁移会停止，保留原文件供检查。软件不会靠丢弃有问题的记录来完成迁移。首次未完成下载的起始绝对游标未知，其原文件保存在 legacy，下一次从设备最早仍保留的历史重新读取。

迁移可以重复运行；如果数据库已经切换、清理旧文件时中断，再执行会继续清理。迁移后发现旧文件被外部修改，会保留该文件并在 `retained_changed_files` 中报告。

只有旧版数据库备份时，也可以把它放到一个单独目录并命名为 `library.sqlite`，用 `--data-dir` 指向这个目录执行 `migrate`。它只能恢复这份旧备份当时已入库的数据；未包含的下载暂存不会凭空恢复。

```bash
uv run kestrel.py --data-dir /实际路径/旧档案目录 migrate
```

## 每天下载、查看和导出

选择设备后按 Enter：首次读取最早仍保留的历史，以后自动从保存的绝对游标增量同步。多设备先用空格勾选，再按 `a` 顺序采集；没有勾选时处理当前列表中所有有编号且身份无冲突的设备，也包括刚扫描到的新设备。单台失败后继续下一台；未适配型号报告具体原因，整体返回未全部完成。初次下载中断的设备也可以再次批量处理。

接收进度达到 100% 后仍需等待完成校验。结果页中：

- **累计 / 已入库**：已正式确认、可以查询导出的记录。
- **未确认 / 未完成**：已安全存入数据库，但整个下载尚未确认完成的记录。
- **已经是最新**：本次没有新增记录。

出现可重试的错误，结果页会提供 `r`；软件不会自行反复连接。设备不匹配和数据冲突需要先核对原因。按 `l` 查看完整日志。

按 `v` 显示最近 100 条正式记录，最新在上方，↑↓ 滚动。相同时间戳可以是不同观测，不会按时间去重。

按 `e` 后依次填写输出路径、开始时间、结束时间。不需要筛选就将时间留空。时间可写 `2026-10-22` 或 `2026-10-22 12:30:00`；**结束时间只写日期时，包含整天**。设备时间没有时区，软件不猜测 UTC 偏移。

CSV 使用 UTF-8 BOM，适合 Excel。已有文件不会覆盖，换个名称即可。删除导出的 CSV 不影响数据库和下次下载。

| 按键 | 作用 |
| --- | --- |
| ↑ / ↓ | 选择设备或滚动记录 |
| Enter / d | 首次下载或增量同步 |
| s / x | 扫描并显示型号 / 只读核验编号并登记 |
| 空格 / a | 勾选设备 / 依次采集勾选项，无勾选时处理列表设备 |
| v / e / b | 查看记录 / 导出 CSV / 备份 |
| r | 主界面只读刷新；结果页按提示重试 |
| m | 迁移旧版档案，先备份和校验 |
| p / o | 保存数据目录位置 / 打开数据文件夹 |
| n / i | 手动添加设备 / 导入旧完整下载 |
| c / l / ? / q | 取消 / 日志 / 帮助 / 返回或退出 |

表单中用 Tab 或 ↑↓ 切换字段，Enter 下一项或提交，Ctrl+U 清空，Esc 返回。

## 数据保存在哪里

TUI 顶部会显示实际路径，按 `o` 打开文件夹。

新安装的默认位置：macOS 为 `~/Library/Application Support/KestrelDrop/`，Linux 为 `$XDG_DATA_HOME/kestrel-drop/`（未设置时使用 `~/.local/share/kestrel-drop/`）。现有项目中发现 `archive/` 档案时，继续使用它，避免升级后看不到旧数据。

```text
数据目录/
├── library.sqlite       所有设备的正式记录和下载恢复状态
├── exports/             按需导出的 CSV
├── backups/             生成的数据库备份
└── legacy/              升级时冻结的旧材料，新安装没有
```

SQLite 运行时可能产生 `-wal`、`-shm` 或 `-journal` 配套文件，由数据库管理，不要手动删除。当前版通过操作系统锁定数据目录防止并发写入，不再产生额外 `.lock` 文件。查询可以与下载同时进行。

数据目录优先级：`--data-dir` → `KESTREL_DATA_DIR` 环境变量 → 保存的位置 → 默认位置。按 `p` 或运行以下命令保存位置：

```bash
uv run kestrel.py configure ~/KestrelData
```

**保存位置不会搬动已有数据。** 换目录时，先备份，再恢复到新目录，最后保存新位置：

```bash
uv run kestrel.py backup /实际路径/kestrel-backup.sqlite
uv run kestrel.py --data-dir ~/KestrelData restore /实际路径/kestrel-backup.sqlite
uv run kestrel.py configure ~/KestrelData
```

设置文件在 `~/.config/kestrel-drop/settings.json`，只保存数据目录，不包含观测。程序测试可通过 `KESTREL_CONFIG_FILE` 指定独立设置文件。

项目中的 `data/originals/` 和 `data/analysis/` 是历史研究资料，不参与日常同步，迁移不会改动它们。

## 数据怎样管理

**SQLite 是唯一运行档案，CSV 是导出物。** 正式观测保存原始 21 字节和解析出的测量值，不再额外保存整行 JSON，也不再维护一份不断增长的正式 CSV。

```text
设备数据块 → CRC / 顺序 / 时间校验 → 数据库待确认块事务 → 回复 ACK
设备完成消息 → 数量 / 游标 / 增长校验 → 一个事务发布正式记录并清理暂存 → 回复完成 ACK
正式记录 → 只读预览 / 按需导出
整个数据库 → 一致备份 → 恢复后继续下载
```

五张表分别保存设备、连续历史段、下载会话、待确认块和正式观测。记录身份是“设备 + 历史段 + 绝对游标”。同一设备重置后的历史不会自动与现有历史拼接；当前遇到缺口或游标冲突会停止，尚未提供手动创建新历史段的操作。

正式游标和已接收边界是不同事实：中断时保留待确认块，但它们不进入正式查询。增量重试从可信边界继续，首次下载因为尚不知道设备实际起始游标，会重新请求游标零。完整检查使用 `db`，日常刷新、预览和导出不会扫描旧 CSV 或修改数据库。

详细表结构与事务说明见 [存储设计](docs/storage.md)。

导出包含设备 `serial`、历史段 `history_generation`、记录编号 `absolute_cursor`、设备本地时间、温湿度气压、热指数、露点、密度高度，以及 `record_hex` 原始字节。`block` 和 `position_in_block` 表示接收块位置，`unknown_byte` 是尚未确认含义的字段。`station_pressure_hpa` 是站点气压，不能当作海平面修正气压。

## 备份与换电脑

按 `b` 或运行 `backup`，默认生成在数据目录的 `backups/`。备份使用 SQLite 备份接口，验证数据库结构、原始记录和恢复边界后发布；已有文件不会覆盖。

**0.6 格式的单个备份包含正式数据和当时的下载暂存，恢复后可以继续同步。** CSV 导出、legacy 旧材料和位置设置不包含在数据库备份内；如果需要长期保留原始迁移证据，请另外复制 legacy。

```bash
uv run kestrel.py backup /实际路径/kestrel-backup.sqlite
# 新电脑：恢复目标必须为空目录
uv run kestrel.py --data-dir ~/KestrelData restore /实际路径/kestrel-backup.sqlite
uv run kestrel.py --data-dir ~/KestrelData db
uv run kestrel.py configure ~/KestrelData
```

恢复不会覆盖已有档案。先检查条数，再继续下载。不要用运行中直接复制主数据库文件代替 `backup`，未合并的 WAL 也可能包含数据。

## 导入旧完整下载

按 `i`，输入本工具早期下载的完整 CSV；元数据留空时使用旁边的 `.csv.json`：

```bash
uv run kestrel.py import /实际路径/my_history.csv
```

源文件不改动，通过校验后一次事务入库，不产生日常 CSV 副本。同一序列号已有档案时拒绝覆盖。LiNK 官方 CSV 缺少这里需要的原始记录和完成游标，不能作为可续传档案直接导入。

## 常见问题

- **扫描不到设备**：断开手机 LiNK，检查电脑蓝牙、终端权限，靠近设备重试。`scan --timeout 30` 可延长扫描。
- **D1/D2 没显示编号**：选择对应行按 `x` 读取标准 GATT 序列号字段，不会发送配置指令。若设备拒绝读取，软件保留型号并显示失败原因，不会把蓝牙地址冒充编号。
- **重复编号或型号冲突**：保留两个候选项并阻止按编号盲选，用 `x` 按地址逐台核验。广播名称不能覆盖已有档案的设备身份。
- **下载中断**：保留数据库，再次下载。首次中断会从最早仍保留的历史重读；增量中断从可信接收边界续传。
- **有待确认数据，设备报告零条新增**：缺少完整完成确认，数据保持待确认；等设备产生新样本后重试。
- **提示校验失败或游标冲突**：保留数据库和 legacy，查看日志，不手改游标。设备环形缓冲可能覆盖旧记录，及时下载有助于避免缺口。
- **提示正在使用 / 数据库锁定**：等待另一个写入或迁移任务结束。迁移前需关闭旧版本程序。
- **数据库损坏或丢失**：使用备份恢复到空目录。新版不会再通过日常 CSV 自动重建数据库；导出的 CSV 也不包含下载暂存。
- **Git 能备份观测吗**：不能。Git 只管理代码，观测数据库、备份、导出、抓包和个人资料均被忽略。

## 命令行与测试

全局 `--data-dir` 放在子命令之前；不加 `--execute` 的下载只预演，不访问蓝牙、不创建数据目录。

```bash
uv run kestrel.py list
uv run kestrel.py status 2866855
uv run kestrel.py db
uv run kestrel.py scan --timeout 20
# 可选：逐台只读核验附近设备，不登记、不下载
uv run kestrel.py scan --timeout 20 --identify
# 批量登记发现的设备；只读硬件身份，本地保存登记
uv run kestrel.py scan --timeout 20 --register
# BLE地址取自扫描结果；identify只读，register额外保存本地设备登记
uv run kestrel.py identify --ble-id /替换为扫描得到的地址/
uv run kestrel.py register --ble-id /替换为扫描得到的地址/
uv run kestrel.py download --serial 2866855 --execute
uv run kestrel.py download --all --execute
# 明确指定多台；--all 包括本地已登记、空档案及初次中断的设备
uv run kestrel.py download --serials 2866855 3087666 --execute
uv run kestrel.py records 2866855 --limit 20
uv run kestrel.py export 2866855 /实际路径/oct22.csv --since 2026-10-22 --until 2026-10-22
uv run kestrel.py --events-json download --serial 2866855 --execute
```

JSON Lines 事件带 `version: 1`，包含阶段、进度、错误、设备结果和命令结束事件。`finished` 只表示命令结束，需要检查退出码；0 成功，2 错误或未完成，130 用户取消。批量下载任何一台失败，整体返回 2。下载还支持 `--timeout`、`--maxrows` 和单设备 `--ble-id`，默认每次最多接收 6,220 条，累计档案不受这个限制。

```bash
uv run --locked python -B -m unittest discover -s tests -v
# 离线模拟测试也可以直接使用本机 Python：
python3 -B -m unittest discover -s tests -v
```

174 项测试在 Python 3.14 / 3.15 上通过，覆盖 CRC、缺块、重复块、乱序、写入失败、首次和增量中断、ACK 前持久化、最终事务回滚、强制终止进程、旧格式迁移、迁移切换中断、数据库备份恢复续传、时间筛选、结构化事件及 TUI 交互。故障注入属于模拟验证，不等同于全部在真实硬件上断电测试。真实 D3 迁移后新增 9 条，累计 6,125 条，TUI 导出和备份恢复均通过，详见 [本次验收记录](docs/validation-0.6.md)。

`devices.py` 统一型号、广播、GATT 身份与连接选择，`manager.py` 协调 CLI，`tui.py` 管理界面，`database.py` 实现唯一运行存储，`storage.py` 管理位置、迁移和恢复，`protocol.py` 包含 D3 协议与旧格式恢复工具。底层 `scripts/kestrel_protocol.py` 仍保留给协议研究，维护旧 CSV 时不会自动更新新版数据库，不用于日常同步。

本地 Git 管理代码和文档，没有远程仓库，也不会自动上传数据。查看修改用 `git status` / `git diff`，版本历史用 `git log --oneline`。数据备份与代码提交分别进行。

批量采集先共享一次广播扫描，再顺序连接；未出现在扫描快照中的设备会重新寻找。自定义名称的设备可在 TUI 中扫描、核验后选择采集，连接地址会按设备传给后台。CLI 可用与 `--serials` 等长的 `--ble-ids` 指定地址（`-` 表示按编号寻找）；地址只作为临时定位提示，连接后始终核对真实序列号。

更多说明：[多设备审查与验收](docs/multi-device.md)、[存储设计](docs/storage.md)、[底层协议](docs/protocol.md)、[变更记录](CHANGELOG.md)。
