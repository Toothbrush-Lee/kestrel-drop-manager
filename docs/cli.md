# 命令参考

适用于 0.15.0。以下命令在项目目录运行，把示例设备编号、BLE 地址和输出路径换成自己的值。路径含空格时加引号。完整参数以 `uv run kestrel.py --help` 和 `uv run kestrel.py 子命令 --help` 为准。

## 基本约定

```bash
uv run kestrel.py                         # 打开 TUI，与 tui 子命令相同
uv run kestrel.py --version
uv run kestrel.py --data-dir ~/KestrelData list
```

全局 `--data-dir`、`--events-json` 必须放在子命令之前。数据目录优先级：命令参数 → `KESTREL_DATA_DIR` → 保存的位置 → 默认位置。`configure` 只保存以后使用的位置，不移动数据。

设备历史读取、配置写入、闪灯均需要显式 `--execute`；缺少它时只预演。`device`、`live` 和身份读取本身会连接蓝牙，无须此开关。批量下载不要通过多个进程同时操作同一目录。

## 发现与身份

```bash
uv run kestrel.py scan                   # 默认 15 秒扫描
uv run kestrel.py scan --timeout 20      # 较多设备或漏扫时延长
uv run kestrel.py scan --timeout 20 --identify
uv run kestrel.py scan --timeout 20 --register
uv run kestrel.py identify --ble-id '扫描得到的地址'
uv run kestrel.py register --ble-id '扫描得到的地址'
uv run kestrel.py identify --serial 2509793 --timeout 30
```

普通 scan 默认收集 15 秒广播，不连接、不登记；可重复扫描或显式延长。`--identify` 逐台只读核验；`--register` 核验后额外保存本地登记。identify/register 必须在 `--serial` 和 `--ble-id` 中选一个。D1/D2 广播可能没有编号，用扫描地址读取标准 GATT 身份；地址只是临时连接位置，不能当作永久设备编号。已知地址的配置、身份读取和单台下载定位会在目标广播出现时立即结束；只有编号时仍完整扫描检查重复身份。地址失效时使用同一轮已看到的广播按唯一编号回退，不额外扫描。`--timeout` 是定位等待上限，目标提前出现就提前结束；它不是整个配置操作的总耗时。

## 下载与批量地址

```bash
# 默认只预演：不访问蓝牙，不创建数据目录
uv run kestrel.py download --serial 2509793
uv run kestrel.py download --serial 2509793 --execute
uv run kestrel.py download --serial 2509793 --ble-id '扫描得到的地址' --execute
uv run kestrel.py download --serials 2509793 2525425 --parallel 2 --execute
uv run kestrel.py download --all --execute
```

`--serial`、`--serials`、`--all` 三选一。`--all` 包括全部本地登记设备、空档案和首次中断项，可能包含离线设备；TUI 则始终使用明确勾选；无勾选时 a 不启动，Enter 只处理当前设备。

批量指定地址时，`--ble-ids` 与 `--serials` 必须逐项对应，`-` 表示按编号寻找。整条命令在同一行输入，不能把 UUID 在中间断开。下面有三台设备、三个定位项：

```bash
uv run kestrel.py download --serials 2509793 2525425 3087666 --ble-ids '第一台扫描地址' - '第三台扫描地址' --parallel 2 --execute
```

| 参数 | 默认 / 行为 |
| --- | --- |
| --timeout | 20 秒，设备查找/连接相关超时 |
| --maxrows | 20000，单次接收记录上限；不是累计库容量 |
| --parallel | 1，可选 1–8；建议先尝试 2 |
| --execute | 真正发送历史读取命令 |

批量共享初始定位：全部目标都有 BLE 地址时，在全部地址出现后提前结束；含仅编号目标时完整扫描。未找到地址时保留同一窗口的广播用于旧地址回退；连接后仍核验身份。一台失败后其他设备继续。并发时未在初始扫描中出现的设备不会另起扫描干扰传输，重新扫描后重试。首次完整读取可能需要十多分钟；增量按保存的游标继续。达到接收进度 100% 后仍需等最终校验。

## 本地查询、导出与检查

```bash
uv run kestrel.py list
uv run kestrel.py status
uv run kestrel.py status 2509793
uv run kestrel.py overview
uv run kestrel.py records 2509793 --limit 100
uv run kestrel.py export 2509793 ~/Desktop/drop-oct22.csv --since 2026-10-22 --until 2026-10-22
uv run kestrel.py db
```

list/status 返回档案状态；overview 把设备登记、正式条数、下载边界和最近下载时间关联起来。SQLite `devices` 是身份表，观测条数来自观测及同步相关表，不应通过手动修改身份表更新条数。

records/export 只读正式记录，不包含待确认块。导出起止时间均包含边界，结束只写日期包含当天整天；设备时间没有时区。CSV 不覆盖已有文件。db 完整检查数据库、原始记录和恢复边界，数据量大时比日常状态查询耗时。

## 设备配置与实时测量

```bash
uv run kestrel.py device 2509793
uv run kestrel.py set 2509793 --interval 600 --wrap on
uv run kestrel.py set 2509793 --interval 600 --wrap on --execute
uv run kestrel.py set 2509793 --time now --execute
uv run kestrel.py set 2509793 --name 'DROP-001' --execute
uv run kestrel.py live 2509793 --samples 5 --period 2
uv run kestrel.py blink 2509793 --execute
uv run kestrel.py actions 2509793 --limit 20
```

这些设备命令支持 `--ble-id` 和 `--timeout`。set 可组合多项；TUI 为单项修改，CLI 组合写入仍逐项核验，不能把整个硬件写入视为可回滚事务。

| set 参数 | 值 |
| --- | --- |
| --interval | 秒数：2, 5, 10, 20, 30, 60, 120, 300, 360, 600, 1200, 1800, 3600, 7200, 21600, 43200 |
| --wrap | on 覆盖最早设备记录，off 满后停止记录 |
| --update-seconds | 2–60 秒，实时测量更新间隔 |
| --reference-altitude | D3 气压计算参考海拔，整数米，有效范围 -32766 至 32767 |
| --name | 1–20 个可打印 ASCII 字符 |
| --time | now 或不带时区的本地时间，例如 `"2026-10-27 09:00:00"` |

校时不改变旧历史；now 在每台通知通道准备好、发送校时命令前分别取电脑本地时间；手填固定时间则原样使用。写入取消、断线或失败后，先 device 回读再决定是否重试。device/set/blink/live 的成功结果另含 timings_seconds，记录已执行阶段的定位、连接、身份、读旧值、写入回读、审计及总耗时；单位秒，子阶段可能缺省，total 包括清理等时间，不应当作 BLE 吞吐率。set-batch 成功项也包含各自的阶段计时。actions 保存配置审计；started 不是成功证据。live 的测量不会入历史库，period 是电脑读取间隔，不能保证设备每次产生新值。

## 明确目标的批量设置

```bash
# 默认预演，不连接蓝牙、不创建数据目录
uv run kestrel.py set-batch --serials 2509793 2525425 --time now
# 检查目标与电脑时间后执行；每台实际写入时取当前时间
uv run kestrel.py set-batch --serials 2509793 2525425 --time now --execute
uv run kestrel.py set-batch --serials 2509793 2525425 --interval 600 --wrap on --execute
```

必须显式提供 --serials；不支持 --all，以免把历史离线登记自动纳入配置。可选 --ble-ids 与编号逐项对应，- 表示按编号寻找，--timeout 默认 20 秒。支持 --interval、--wrap、--update-seconds 和 --time，范围与单台一致；不批量改名称和参考海拔。

逐台核验、读取本台旧值、写入、回读并保存独立审计。整批共用目录写锁，单台设备错误继续其他设备，存储等批次级错误会终止。下载并发数不适用于批量设置。配置不是跨设备原子事务，不会自动回滚或自行循环重试。TUI结果页r提供手动“仅重试未成功项”：保留原参数与地址对应，排除verified，失败/取消/未开始及缺少报告的原目标重新预览确认后执行；后台沿用逐台读取、同值普通参数跳过写入、回读和审计流程。CLI再次执行时应显式缩小--serials，勿将已成功设备一并重放。

JSON 结果 configurations 逐项列出 verified / failed / dry_run / cancelled_unknown / not_attempted，并返回独立 action_id，校时还返回 time_write_target_local、time_write_skipped；普通设置还返回settings_write_skipped及settings_written_fields，明确哪些字段实际写过。全部完成退出 0，单台失败退出 2，取消退出 130；取消时尽可能报告已完成、当前结果未知及未执行项。JSON Lines 新增 configured 事件，协议版本仍为 1。失败或取消后先用 device 回读、actions 看审计，再决定是否重新设置。

## 数据位置、备份、恢复和旧版导入

```bash
uv run kestrel.py backup
uv run kestrel.py backup ~/Desktop/kestrel-backup.sqlite
uv run kestrel.py --data-dir ~/KestrelData restore ~/Desktop/kestrel-backup.sqlite
uv run kestrel.py --data-dir ~/KestrelData db
uv run kestrel.py configure ~/KestrelData
uv run kestrel.py migrate
uv run kestrel.py import '/实际路径/history.csv' --metadata '/实际路径/history.csv.json'
```

backup 默认写到数据目录/backups，目标文件不能已存在。restore 目标必须为空数据目录，验证后才发布；configure 不搬数据。备份包含数据库内的正式观测、暂存、身份和配置审计，不含 CSV、位置设置或 legacy 原件。

migrate 用于旧 CSV/早期 SQLite，先保存和校验再切换；0.8–0.10→0.11 无需迁移。import 只支持本工具旧 D3 完整原始 CSV 及元数据，不是任意 CSV/LiNK 导出的通用导入；同编号已有档案时拒绝覆盖。详细恢复与升级见[用户手册](user-guide.md)。

## 自动化、日志和退出码

```bash
uv run kestrel.py --events-json download --serials 2509793 2525425 --parallel 2 --execute > /tmp/kestrel-events.jsonl
```

JSON Lines 每行一个事件，带 `version: 1`，包括 phase/progress/log/error/completed/configured/result/sample/finished。需要同时检查每台 completed/error、最终结果和进程退出码。finished 表示命令已结束，不等于全部成功。不要混入普通标准输出或用每一行都当作最终结果。

| 退出码 | 含义 |
| --- | --- |
| 0 | 成功，或预演完成 |
| 2 | 参数错误、失败或未全部完成；批量中一台未完成也返回 2 |
| 130 | 用户取消 |

Ctrl+C 取消时会清理连接并保留持久化边界；不能把强制杀进程当作正常取消方式。记录诊断文件放临时目录或本地 `data/analysis/主题-日期/`，避免散落根目录；输出可能包含设备身份和观测，不进入代码仓库。

0.12：db 结果包含 journal_mode；status/list 的 superseded_records 为首次旧尝试证据，resumable_records 为可复用的增量暂存数。download 的 saved_warning 表示完整入库后结束 ACK 发送失败，退出码仍为 0，warning 中保留原因与建议；pending 仍表示尚未发布。

## 时间诊断与校时确认

`timing [serial] --interval 300 --recent 100` 只读检查原始设备时间、整点相位和最近1000条配置审计中的校时事件，不访问蓝牙。省略编号检查所有设备；interval是诊断假设，不是对设备实际配置的读取，recent可为2–10000。

执行 `set SERIAL --time now --execute --allow-schedule-shift` 或批量 `set-batch ... --time now --execute --allow-schedule-shift` 才允许校时。仅--execute不足以确认相位风险；预演会返回sampling_risk。操作后sampling_risk.schedule_alignment_verified恒为false，必须另看新历史。time_write_skipped表示无需重复写，clock_correction_seconds记录重新读取RTC后计算的时钟差；computer_time_at_check包含电脑时区。

## 本机数据浏览

```sh
uv run --extra viewer kestrel.py browse
uv run --extra viewer kestrel.py --data-dir /绝对路径/数据目录 browse --port 8502 --no-open
```

`browse` 使用可选 Streamlit 组件，端口默认8501，仅监听127.0.0.1。`--no-open` 不自动打开浏览器；Ctrl+C关闭。保持终端运行，访问页面上显示的本机地址。只读已存在的 `library.sqlite`，不会建空库、迁移或连接设备；不支持 `--events-json`。独立采集运行包不含此扩展，详见[浏览指南](data-browser.md)。

## 本地设备资料与领用清单

`metadata 序列号 [--management-code 管理编号] [--participant-code 受试者代号]`：无修改参数时只读，带参数离线保存，显式空字符串仅清除受试者代号；管理编号首次保存后锁定，只允许相同值重传。`labels-import CSV路径` 只预览校验，加 `--execute` 原子导入已登记设备的编号；空白受试者列不覆盖已有代号。`labels-export 输出CSV路径` 导出当前资料清单，不覆盖旧文件。`actions 序列号` 可查看含 `requested.local_metadata` 的变更记录。完整规则见[设备资料](device-metadata.md)，当前代号不代表全部历史归属。

## 单台数据管理

```sh
uv run kestrel.py data-info 序列号
uv run kestrel.py data-preferences 序列号 --auto-sync on
uv run kestrel.py data-preferences 序列号 --auto-sync off
uv run kestrel.py device-count 序列号
uv run kestrel.py clear-history 序列号
uv run kestrel.py erase-device 序列号
```

data-info和偏好设置不连接蓝牙；自动同步默认关闭。device-count只核验硬件身份和读取总日志条数，不触发自动同步、清除或校时。设备设置读取、配置、闪灯、实时读取及明确登记连接在启用偏好后先同步一次；纯扫描和只读identify不触发。

两种清除默认预演，不清数据。实际执行必须同时传 `--execute --confirm-serial 完整序列号 --expected-revision 刚读取的data-info.revision`；不支持all/serials。revision防止确认后数据或领用信息变化。clear-history会先生成独立备份再清除本机该设备全部正式记录、暂存和传输历史；erase-device先同步、备份再使用官方命令1清设备日志，并用命令2核对0条，电脑历史与游标保留。备份失败或数据有未确认项时不清除；结果不明不得直接重试。建议普通用户使用h页的输入确认流程。erase-device和device-count支持--ble-id、--timeout。

清除范围、自动同步的短连接行为和采集中清设备的边界详见[数据管理指南](data-management.md)。
