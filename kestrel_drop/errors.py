"""Actionable failures shared by transport, CLI and terminal UI."""
import sqlite3

TITLES = {'operation_failed': '操作未完成', 'transfer_integrity': '传输校验未通过',
          'device_mismatch': '连接的设备与选择不符', 'bluetooth_unavailable': '蓝牙连接不可用',
          'cancelled': '任务已取消', 'already_exists': '目标文件或档案已经存在',
          'not_found': '未找到文件或设备档案', 'invalid_input': '输入内容需要检查',
          'dependency_missing': '运行依赖尚未准备好', 'busy': '另一个任务正在使用档案',
          'storage_failed': '保存数据时发生错误', 'archive_check_failed': '档案校验未通过',
          'completion_pending': '等待设备完成确认', 'worker_failed': '后台任务未正常完成'}


class AppError(RuntimeError):
    code = 'operation_failed'
    retryable = False
    action = '查看日志并保留档案文件；不要覆盖已有数据。'


class TransferError(AppError):
    code = 'transfer_integrity'
    retryable = True
    action = '已保存的连续边界保留。可重试下载；若反复出现，请保留日志检查设备。'


class DeviceMismatch(AppError):
    code = 'device_mismatch'
    action = '核对机身型号和序列号，重新选择正确的 D3。'


class BluetoothFailure(AppError):
    code = 'bluetooth_unavailable'
    retryable = True
    action = '断开手机 LiNK，检查电脑蓝牙和终端权限，再按 r 重试。'


def describe(error):
    if isinstance(error, AppError):
        code, retryable, action = error.code, error.retryable, error.action
    elif isinstance(error, KeyboardInterrupt):
        code, retryable, action = 'cancelled', True, '已取消；已落盘材料保留，可稍后重新下载。'
    elif isinstance(error, FileExistsError):
        code, retryable, action = 'already_exists', False, '已有文件不会被覆盖。导出请换文件名；已有设备请使用下载更新。'
    elif isinstance(error, FileNotFoundError):
        code, retryable, action = 'not_found', False, '检查文件路径，或先为设备建立档案。'
    elif isinstance(error, ValueError):
        code, retryable, action = 'invalid_input', False, '检查输入的序列号、日期范围和参数。'
    elif isinstance(error, ImportError):
        code, retryable, action = 'dependency_missing', False, '请使用 uv run kestrel.py 启动，让 uv 安装项目依赖。'
    elif 'Another download' in str(error) or getattr(error, 'sqlite_errorcode', None) in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
        code, retryable, action = 'busy', True, '等待另一个任务结束，或关闭另一个正在操作该档案的程序后重试。'
    elif isinstance(error, (sqlite3.Error, OSError)):
        code, retryable, action = 'storage_failed', False, '检查磁盘空间和目录权限，保留完整档案后再重试。'
    else:
        code, retryable, action = 'archive_check_failed', False, '保留 CSV、JSON 和数据库，查看日志；不要手动修改游标或覆盖文件。'
    return {'code': code, 'title': TITLES.get(code, '操作未完成'), 'message': str(error) or '操作被中断',
            'retryable': retryable, 'action': action}
