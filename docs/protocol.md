# 底层协议与数据恢复

从项目根目录运行本文命令。日常下载管理请使用[主入口](../README.md)。本文保留协议实现与验证背景；历史测试数量及现场验证描述对应 0.2.0。

0.6.0 日常入口已改为 [SQLite 唯一运行存储](storage.md)。本文的 `sync` / `adopt` 等命令维护旧 CSV 格式，仅用于协议研究与旧档案诊断；它们不会更新新版数据库。旧档案请通过统一入口的 `migrate` 迁移。


**Experimental reverse-engineered implementation, 2026-10-22.** This release builds on the previous `kestrel_protocol.py` and adds durable CSV spooling, cross-session incremental download by **absolute record cursor**, and verified publication. D3 only; D2 record format is not yet verified. It **never sends a clear-log command**, but it does use experimental BLE GATT writes when `--execute` is provided. Back up your data via Kestrel LiNK before first use.

## What changed

- `adopt`: bootstrap a proven full historical CSV and its original `.csv.json` sidecar. Checks each CSV row against its raw 21-byte record, CSV metadata (record count, start/end times), and final absolute cursor.
- `sync`: query starting at **the stored absolute `next_cursor`**, not from zero; save each CRC-validated BLE data block to `<csv>.pending.csv` and `fsync` it **before sending the application-level ACK**. On verified completion, atomically replace the main CSV with a combined snapshot, and update its state.
- `status`: display the committed total, current cursor, and number of **incomplete-but-durably-saved** pending rows.
- Automatic recovery if the process crashes *after* the main CSV was replaced but *before* the state was updated. No silent overwriting when the CSV hash differs from the checkpoint.
- Preserves duplicate timestamps: they occurred in real data and must **not** be used as deduplication keys.
- The previous `scan`, `probe`, `live`, and `decode` commands remain available. `sync` can identify the sensor by `D3 - <serial>` advertisement, so `--ble-id` is optional; actual serial is checked through GATT before any proprietary write.

### Tested against real files

The original full capture `data/originals/d3_2866855_progress.csv` contains **5,939 rows** and the confirmed sidecar states `next_cursor=114257`. Adoption was tested with a temporary copy of both real files. **31 automated tests** include BLE protocol simulation, per-block durable persistence, crash recovery between two atomic writes, corrupted-record rejection, and duplicate-timestamp preservation. **Not tested against a new live D3 sync in this environment.**

## 使用底层工具

日常使用统一入口，无需手动复制文件、建档或计算游标。底层工具供协议诊断和恢复使用：

```bash
uv run scripts/kestrel_protocol.py --help
uv run scripts/kestrel_protocol.py status --output archive/d3_2866855.csv
```

`adopt` 仅用于尚未初始化、已经验证的完整 CSV 与原始 `.csv.json`。已有档案不要重新 adopt，也不要用原始 CSV 覆盖正在增量更新的 CSV。首次导入请使用主入口的 `import` 命令，它会复制并验证数据。

支持 `scan`、`probe`、`live`、`decode`、`adopt`、`status` 和 `sync`。离线分析结果写入独立目录，例如：

```bash
uv run scripts/kestrel_protocol.py decode \
  --pklg /path/to/capture.pklg \
  --output data/analysis/new-capture
```

## Files and recovery

| File | What it is | Safe to delete? |
|---|---|---|
| `archive/d3_2866855.csv` | Complete committed observations across runs | No |
| `archive/d3_2866855.csv.sync-state.json` | Committed `next_cursor`, row count, checksum | No |
| `archive/d3_2866855.csv.pending.csv` | Every successfully decoded block from an uncommitted transfer | **No** while interrupted |
| `archive/d3_2866855.csv.pending.json` | Spool identity and completion evidence | No while pending |
| `archive/d3_2866855.csv.staging.csv` | Temporary atomic publication | Do not manually overwrite |

During a transfer, open the **pending CSV** if you need to inspect records already saved. Only after receiving and validating the device's completion cursor does the new data become part of the **main CSV**.

If the connection drops after 1,000 received records, those 1,000 records are kept in `.pending.csv`. When you run `sync` again, it resumes from the committed cursor **plus 1,000**, assuming the device has not overwritten the records. A retry is conservative: if the device now reports **zero** records after a partial transfer and the program never saw a final completion message, it leaves the pending CSV alone instead of silently publishing unconfirmed data. This rare finalization case currently requires an independent check or new logging activity; it is **not** claimed to be automatically resolved.

If a Mac or process crashes during final publication, the next `sync`/`status` can recover a staged, already-confirmed transaction. Never edit the canonical CSV or `.pending.csv` while syncing: the SHA-256 checkpoint will refuse unexpected changes.

The canonical CSV still contains the original field set, including `record_hex`, and the source `...csv.json` remains your immutable evidence from the first full download. CSV timestamps are **device-local without a timezone offset**; always document/normalize the local offset in your external study metadata.

## Validation and limitations

Each block is decoded only after a valid framed CRC. It is flushed before acknowledging the block. At completion, the code validates block ordering, time ordering (allowing equal timestamps), plausible additions during long transfers, and the absolute completion cursor. On-disk rows are re-decoded from their raw bytes before commit. The combined CSV and checkpoint are published with a recovery journal that covers a crash between the two updates.

- **IMPORTANT:** BLE downloads are write-based at the application protocol level; `--execute` is necessary. This implementation intentionally has no erase command.
- Cursor continuity is critical. If a device's ring buffer wraps after a long unattended deployment, newer data may no longer cover the requested cursor. Sync must stop rather than hide a gap.
- This is not a full transactional database. A production fleet of 20–100 devices should use a SQLite canonical ledger (`serial, absolute_cursor` as unique key), emit CSVs as exports, and integrate per-device sync metrics/automatic QC.
- Large `fsync` frequency can reduce throughput slightly. This version prioritizes durability over maximal BLE throughput.
- An exclusive non-blocking OS file lock is now enforced for each archive, avoiding two simultaneous `sync` writers on macOS/Linux. This is not a replacement for a multiuser database.
- No `asyncio` multicore/multi-device scheduler is in this release; it is **single-device**, with the data durability foundation needed for a later bounded parallel runner.

## Run tests

```bash
uv run --no-project python -m unittest discover -s tests -v
```

The tests are local simulations and do not require BLE hardware or access to the historical user data files.
