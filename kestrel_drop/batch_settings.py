"""Sequential, explicitly targeted device configuration with per-device audit."""
import asyncio
import math
from contextlib import nullcontext

from . import control, events
from .devices import serial_number
from .errors import describe


FIELDS = {'interval_seconds', 'wrap', 'update_seconds', 'time'}


def targets(serials, addresses=None):
    if not serials:
        raise ValueError('批量设置必须明确选择设备')
    if addresses is not None and len(addresses) != len(serials):
        raise ValueError('--ble-ids 必须与 --serials 数量相同')
    result = {}
    for serial, address in zip(serials, addresses or [None] * len(serials)):
        serial = serial_number(serial)
        address = None if address in (None, '-') else address.strip()
        if address == '':
            raise ValueError('BLE 地址不可为空；按编号查找请使用 -')
        if serial in result and result[serial] != address:
            raise ValueError('同一编号指定了不同连接地址')
        result[serial] = address
    locators = [address.casefold() for address in result.values() if address]
    if len(set(locators)) != len(locators):
        raise ValueError('多个编号不能共用同一连接地址')
    return result


async def configure(library, selected, changes, *, execute=False, timeout=20, allow_schedule_shift=False):
    changes = control.validate_changes(changes)
    if set(changes) - FIELDS:
        raise ValueError('批量设置仅支持采样间隔、滚动覆盖、实时更新间隔和时间')
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError('设备查询超时必须为正数')
    selected = targets(list(selected), list(selected.values()))
    if execute and 'time' in changes and allow_schedule_shift is not True:
        raise ValueError('批量校时可能改变各台采样相位；确认风险后使用 --allow-schedule-shift')
    outcomes, failures = [], []

    def publish(outcome):
        outcomes.append(outcome)
        events.emit('configured', **outcome)

    def result():
        return {'configurations': outcomes, 'incomplete': failures}

    # One directory owner for the complete batch; each device retains its own audit.
    with library.collection(True) if execute else nullcontext():
        for index, (serial, address) in enumerate(selected.items()):
            try:
                with events.context(serial=serial):
                    value = await library.device_operation(serial, ble_id=address, timeout=timeout,
                                                           changes=changes, execute=execute,allow_schedule_shift=allow_schedule_shift)
                if execute:
                    publish({'serial': serial, 'status': 'verified', 'changes': changes,
                             'action_id': value['action_id'], 'timings_seconds': value.get('timings_seconds', {}),
                             'time_write_target_local': value['after'].get('time_write_target_local'),
                             'sampling_risk':value['after'].get('sampling_risk'),
                             'time_write_skipped':value['after'].get('time_write_skipped')})
                else:
                    publish({'serial': serial, 'status': 'dry_run', 'changes': changes,'sampling_risk':control.clock_risk(changes)})
            except (asyncio.CancelledError, KeyboardInterrupt):
                failure = {'serial': serial, 'code': 'configuration_cancelled',
                           'message': '本台配置取消，可能部分生效。',
                           'action': '先读取设备设置核对，再决定是否重新配置。', 'retryable': False}
                failures.append(failure)
                events.emit('error', **failure)
                publish({'serial': serial, 'status': 'cancelled_unknown', 'error': failure})
                for remaining in list(selected)[index + 1:]:
                    publish({'serial': remaining, 'status': 'not_attempted'})
                events.result(result())
                raise
            except (OSError, RuntimeError, ValueError) as exc:
                failure = {'serial': serial, **describe(exc), 'retryable': False}
                failure['action'] = '先读取本台设置核对；' + failure['action']
                failures.append(failure)
                events.emit('error', **failure)
                publish({'serial': serial, 'status': 'failed', 'error': failure})
    return result()
