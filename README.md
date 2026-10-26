# Kestrel DROP 电脑下载管理

扫描设备、批量下载 D1/D2/D3 历史，统一保存在电脑的 SQLite 档案库，按需导出 Excel 可打开的 CSV。当前版本 **0.9.1**，面向日常采集与多设备管理。

## 快速开始

需要 macOS / Linux、Python 3.11+ 与 [uv](https://docs.astral.sh/uv/)。macOS 可双击 `Kestrel.command`，也可以在本项目目录运行：

```bash
uv run kestrel.py
```

首次启动会准备依赖。打开电脑蓝牙，在手机 LiNK 中断开设备，允许终端访问蓝牙。终端建议 100 列 × 24 行，最低 68 列 × 18 行。Windows 暂不支持当前界面和目录锁。

1. **s 快速扫描（5 秒）**，↑↓ 选设备；没有编号时按 **x 核验并登记**。
2. **Enter 下载**，首次读取仍保留的历史，以后自动增量同步。
3. 等待校验和结果页，检查新增、累计和未确认条数。
4. **v 预览、e 导出、b 备份**。按 Esc 返回，主界面 q 退出。

漏扫时按大写 **S** 长扫描 20 秒。已知地址进入配置时，发现目标广播即连接，不再固定等满扫描窗口。

多设备：空格勾选，Ctrl+A 勾选附近可采集项，u 清空；j 设置并发，a 开始。默认并发 1，建议先试 2。结果页可逐台浏览，r 按提示重试未完成项。此前已实测两台并发，更多连接数需要现场验证。

g 读取设备设置，可配置采样间隔、覆盖策略、名称、设备时间和实时更新间隔。修改先预览设备编号和前后值，再确认写入并回读。配置结果不确定时先重新读取。

## 数据在哪里

顶部显示实际数据目录，o 可打开。本项目现有安装继续使用 `archive/`；新安装 macOS 默认 `~/Library/Application Support/KestrelDrop/`，Linux 默认 `~/.local/share/kestrel-drop/`，也可使用保存位置或环境变量。

`library.sqlite` 是唯一运行档案；`exports/` 放导出，`backups/` 放备份，`legacy/` 保留旧版迁移证据。下载不会清空设备日志，Git 只管理代码。定期使用 b 备份；换电脑时恢复 SQLite 备份，不能只复制 CSV。p 保存新位置不会自动搬动旧数据。

## 文档入口

| 需求 | 文档 |
| --- | --- |
| 安装、百台采集、按键、设备设置、备份恢复、常见问题 | [详细用户手册](docs/user-guide.md) |
| 脚本、参数、批量地址映射、退出码 | [命令参考](docs/cli.md) |
| 目录分工、开发、测试和 Git | [开发与维护规范](docs/development.md) |
| 数据表、事务、续传和迁移 | [存储设计](docs/storage.md) |
| 官方协议依据及硬件验证范围 | [协议适配](docs/official-protocol.md) |
| 本轮测试与验证边界 | [0.9 验收记录](docs/validation-0.9.md) |
| 全部文档与版本变化 | [文档索引](docs/README.md) · [变更记录](CHANGELOG.md) |

0.9 保持 schema 4；从 0.8 升级无需迁移。更早版本请先阅读[升级流程](docs/user-guide.md)。

## 项目与验证

根目录只保存启动入口、项目配置与总览文档；源码在 `kestrel_drop/`，测试在 `tests/`，诊断在 `scripts/`，说明在 `docs/`。本地原件与分析在 `data/`，官方原始资料在 `KestrelDocuments/`，均与运行档案分开并排除出 Git。

```bash
uv run --locked python -B tests/run.py
```

测试入口使用临时默认档案和配置，不触碰工作中的观测数据。硬件功能和固件差异的实测边界见验收记录。
