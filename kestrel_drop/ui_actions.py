"""Pure UI command planning; no BLE, filesystem or subprocess operations."""
from .control import validate_changes


def format_interval(value):
    if value is None:
        return '未读到'
    if value == 0:
        return '停止记录'
    if value % 3600 == 0:
        return f'{value // 3600} 小时（{value} 秒）'
    if value % 60 == 0:
        return f'{value // 60} 分钟（{value} 秒）'
    return f'{value} 秒'


def setting_change(action, value, state):
    mapping = {
        'set_interval': ('interval_seconds', '--interval', int, '采样间隔'),
        'set_wrap': ('wrap', '--wrap', lambda v: {'on': True, 'off': False}.get(v), '滚动覆盖'),
        'set_name': ('name', '--name', str, '自定义名称'),
        'set_time': ('time', '--time', str, '设备时间'),
        'set_update': ('update_seconds', '--update-seconds', int, '实时更新间隔'),
        'set_altitude': ('reference_altitude_m', '--reference-altitude', int, '参考海拔'),
    }
    field, flag, convert, label = mapping[action]
    if action=='set_wrap': value={'开启':'on','关闭':'off'}.get(value,value)
    converted = convert(value)
    validate_changes({field: converted})
    old = (state.get('settings') or {}).get(field) if field not in ('name', 'time') else state.get('device_time' if field == 'time' else 'name')
    def display(item):
        if item is None: return '未读到'
        if field in ('interval_seconds', 'update_seconds'): return format_interval(item)
        if field == 'wrap': return '开启' if item else '关闭'
        if field == 'reference_altitude_m': return f'{item} 米'
        return str(item) if item != '' else '默认名称'
    impact = '只修改此项，其余设置保持。写入后会回读核验并保存操作审计。'
    if field == 'time':
        impact = '校时可能改变采样相位：300秒不保证整5分钟，可能跳时或重复时间。逐台校时也不保证机群同步；需检查后续至少两条记录。'
    elif field == 'wrap':
        impact = '满后覆盖设备中最早的记录，电脑已保存的数据保留。' if converted else '设备日志满后停止保存新记录。'
    args = ['set', state['serial'], flag, value, '--execute']
    if field=='time': args+=['--allow-schedule-shift']
    if state.get('ble_id'): args += ['--ble-id', state['ble_id']]
    return {'serial': state['serial'], 'label': label, 'old': display(old),
            'new': '提交时的电脑本地时间' if field == 'time' and value == 'now' else display(converted),
            'impact': impact, 'arguments': args}


def retry_download_arguments(arguments, outcomes, failures):
    """Preserve options and address alignment; omit saved and nonretryable peers."""
    if '--serials' not in arguments:
        return list(arguments)
    def values(flag):
        if flag not in arguments: return []
        start = arguments.index(flag) + 1
        end = next((i for i in range(start, len(arguments)) if arguments[i].startswith('--')), len(arguments))
        return arguments[start:end]
    requested = values('--serials')
    saved = {item['serial'] for item in outcomes if item.get('status') in ('downloaded', 'saved_warning', 'up_to_date', 'empty', 'dry_run')}
    blocked = {item['serial'] for item in failures if item.get('serial') and not item.get('retryable')}
    remaining = [serial for serial in requested if serial not in saved and serial not in blocked]
    if not remaining: return None
    result = ['download', '--serials', *remaining]
    addresses = values('--ble-ids')
    if addresses:
        if len(addresses) != len(requested): raise ValueError('重试地址与设备数量不一致')
        result += ['--ble-ids', *(addresses[requested.index(serial)] for serial in remaining)]
    for flag in ('--parallel', '--timeout', '--maxrows'):
        if flag in arguments: result += [flag, *values(flag)]
    if '--execute' in arguments: result.append('--execute')
    return result


def selected_devices(devices, marked):
    """Resolve an explicit selection atomically; never widen an empty selection."""
    if not marked:
        raise ValueError('尚未勾选；空格选择设备，A / Ctrl+A 全选附近设备')
    by_serial = {item.get('serial'): item for item in devices.values() if item.get('serial')}
    invalid = [serial for serial in marked if serial not in by_serial or
               by_serial[serial].get('identity_conflict') or
               by_serial[serial].get('history_supported') is False]
    if invalid:
        raise ValueError('所选设备不可操作，请重新核验或取消勾选：' + '、'.join(sorted(invalid)))
    return [dict(item) for item in devices.values() if item.get('serial') in marked]


def batch_setting_change(action, value, targets):
    if not targets:
        raise ValueError('批量设置目标为空')
    single = setting_change(action.removeprefix('batch_'), value, targets[0])
    flag = single['arguments'][2:4]
    single.update(serial=f'{len(targets)} 台所选设备', old='执行时逐台读取，不沿用其他设备旧值',
                  targets=[dict(item) for item in targets], offset=0, action='set-batch',
                  arguments=['set-batch', '--serials', *[item['serial'] for item in targets],
                             '--ble-ids', *[item.get('ble_id') or '-' for item in targets],
                             *flag, '--execute'])
    if action == 'batch_set_time':single['arguments']+=['--allow-schedule-shift']
    if action == 'batch_set_time' and value == 'now':
        single['new'] = '每台实际写入时的电脑本地时间（逐台同步）'
    return single
