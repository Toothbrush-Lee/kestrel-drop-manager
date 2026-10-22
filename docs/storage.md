# 存储设计与恢复边界

## 两层存储的职责

当前采用“经过验证的下载档案 + SQLite 查询账本”，没有把现有蓝牙恢复流程一次性改写掉。

| 层 | 权威范围 | 内容 |
| --- | --- | --- |
| CSV + JSON + pending | 蓝牙下载的提交、续传及崩溃恢复 | 原始字节、已提交校验和、下一游标、未完成块、设备完成证据 |
| SQLite | 已验证观测的统一查询、去重、批次管理、导出 | 设备、观测、来源校验和、入库批次 |
| 导出 CSV | 面向 Excel、共享和外部分析的快照 | 查询结果，额外包含序列号与绝对游标 |

数据库默认位于 `archive/library.sqlite`，使用 SQLite WAL 模式、`synchronous=FULL`、外键及 5 秒锁等待。每台设备和 CSV 仍使用现有非阻塞进程锁，避免两个传输同时写同一档案。

## 主键和表

`PRAGMA user_version = 1` 标记本版数据库结构。未知版本拒绝打开，不会重置或覆盖。

- `devices`：`serial` 主键；存储型号、档案路径、首游标、下一游标、条数、首末时间、CSV SHA-256 与原表头。
- `observations`：`(serial, absolute_cursor)` 复合主键，保留类型明确的测量列、21 字节 `record_raw` 和原 CSV 行的 JSON 表示；按设备与时间建立索引。
- `ingestion_batches`：每个新源快照一条记录，`(serial, csv_sha256)` 唯一；记录新增范围、条数与入库时间。

绝对首游标按 `state.next_cursor - 已提交行数` 推导。这里的 CSV 包含历次累计记录，不是设备此刻仍保留的那一小段环形缓冲区。

## 一次入库

1. 获取下载档案的独占锁。
2. 校验 CSV 与同步状态的 SHA-256，必要时按原有完成日志恢复 CSV 提交。
3. 从原始 21 字节重解码，核对时间、数值、条数和顺序。非有限数值如 NaN 不接受。
4. 开启 SQLite `BEGIN IMMEDIATE` 事务。
5. 检查既有数据库记录数、连续游标、原始字节、原始行及类型化测量值与 CSV 前缀完全一致。拒绝历史缩短、游标变化或内容冲突。
6. 仅追加尚未入库的尾部记录，同时更新设备状态和入库批次。
7. 一起提交；任一步失败则回滚 SQLite 全部更改。

记录不会按时间戳去重。同一设备同一游标只能有一份一致的内容；不同游标即使时间和原始内容都相同，也可以是不同物理观测。

## 跨文件失败怎样处理

CSV/JSON 的提交与 SQLite 的提交不是一个跨文件事务，因此不能承诺两个存储层在每个机器指令处都同步。

| 中断位置 | 保留的材料 | 下次操作 |
| --- | --- | --- |
| 首次下载未收到完成游标 | `.downloads/` 中的 partial 原始数据 | 原材料保留，从最早仍可读历史重新下载 |
| 增量接收途中 | pending CSV / JSON | 按已提交游标加暂存条数恢复 |
| CSV 替换后，JSON 状态未更新 | 完成 journal 与 CSV | 原协议恢复机制修复状态，不重复追加 |
| CSV 已提交，SQLite 事务未提交 | 完整 CSV / 状态，数据库旧快照 | 下次 `list` / `db` / 预览 / 导出补入库 |
| SQLite 入库途中 | CSV 完整；SQLite 自动回滚 | 原事务全撤回，下次重放 |
| SQLite 历史与 CSV 冲突 | 两边原件 | 停止、报告，不覆盖任何既有观测 |

原始 CSV/JSON 是下载恢复依据，不能在引入数据库后立即删除。数据库只有已确认提交的数据，不包含 pending；快照备份中也没有 pending。

## 查询与导出

预览和导出从 SQLite 查询；如果完整档案存在，查询前先校验并补入库，避免悄悄返回过期记录。已恢复的数据库副本没有 CSV 时仍可以查询，但不能继续蓝牙下载。

导出按绝对游标排序，记录预览取最新 N 条。时间筛选使用参数化 SQL。只给结束日期时用“下一天零点以前”表示整天；给定时间时包含终点。设备时间不附带时区，不推断或转换 UTC。

只读 SQL 示例（面向开发者；不要手动修改生产库）：

```sql
SELECT serial, model, row_count, first_cursor, next_cursor FROM devices;

SELECT serial, absolute_cursor, timestamp_device_local, temperature_c,
       relative_humidity_pct, station_pressure_hpa
FROM observations
WHERE serial = '2866855'
ORDER BY absolute_cursor DESC
LIMIT 20;

SELECT serial, imported_utc, added_records, next_cursor
FROM ingestion_batches
ORDER BY id DESC;
```

## 备份和 Git

`backup` 使用 Python `sqlite3.Connection.backup()` 调用 SQLite 备份接口。生成临时数据库、执行完整性检查，再以不覆盖方式发布目标；它是数据库快照，不是整个下载目录的备份。参见 [SQLite Online Backup API](https://www.sqlite.org/backup.html)。

要继续同步或迁移整套档案，需要停止所有相关程序后复制完整 `archive/`；不要在运行中只复制主数据库文件。数据库备份可以独立恢复预览和导出，传输恢复还需要原 CSV、状态与 pending。

Git 只管理源码、测试、文档和依赖锁。运行档案、原始数据、分析结果、数据库及备份都不进入 Git。数据库版本号、软件版本号、Git 提交号是不同概念，代码回退也不会自动回退观测数据库。

## 验证范围

数据库事务回滚、重复入库、类型字段冲突、缺失记录检测、CSV 提交后入库失败、pending 隔离、备份独立恢复等均有离线自动化测试。数据库不解决未经验证的 D1/D2 协议格式，也不修复设备已经覆盖的历史缺口。
