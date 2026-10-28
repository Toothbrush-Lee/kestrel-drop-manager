"""Read-only sampling diagnostics in sequence order; device-local times stay intact."""
from collections import Counter, deque
from datetime import datetime


def analyze(records, interval=300, recent=100):
    if type(interval) is not int or not 2 <= interval <= 86400:
        raise ValueError('诊断间隔必须为 2–86400 秒')
    if type(recent) is not int or not 2 <= recent <= 10000:
        raise ValueError('最近记录窗口必须为 2–10000 条')
    phases, deltas = Counter(), Counter()
    tail, changes = deque(maxlen=recent), deque(maxlen=30)
    previous = None
    count = duplicates = backwards = irregular = 0
    for row in records:
        stamp=datetime.fromisoformat(row['timestamp_device_local'])
        phase=(stamp.hour*3600+stamp.minute*60+stamp.second)%interval
        phases[phase]+=1;tail.append((phase,stamp));count+=1
        if previous is not None:
            old,old_stamp=previous;delta=int((stamp-old_stamp).total_seconds())
            deltas[delta]+=1
            duplicates+=delta==0;backwards+=delta<0;irregular+=delta!=interval
            if delta!=interval:
                changes.append({'previous_sequence':old['device_sequence'],'sequence':row['device_sequence'],
                                'previous_time':old['timestamp_device_local'],'time':row['timestamp_device_local'],
                                'delta_seconds':delta})
        previous=row,stamp
    recent_phases=Counter(phase for phase,_ in tail)
    recent_stamps=[stamp for _,stamp in tail]
    regular=len(recent_stamps)>=2 and all((b-a).total_seconds()==interval for a,b in zip(recent_stamps,recent_stamps[1:]))
    return {'recent_intervals_match':regular,'last_record_time':str(tail[-1][1]) if tail else None,'expected_interval_seconds':interval,'records':count,'recent_records':len(tail),
            'recent_grid_aligned':bool(tail) and recent_phases[0]==len(tail),
            'phase_counts':[{'offset_seconds':p,'records':n} for p,n in phases.most_common()],
            'recent_phase_counts':[{'offset_seconds':p,'records':n} for p,n in recent_phases.most_common()],
            'delta_counts':[{'seconds':d,'pairs':n} for d,n in deltas.most_common(20)],
            'duplicate_time_pairs':duplicates,'backward_time_pairs':backwards,'irregular_pairs':irregular,
            'recent_transitions':list(changes),
            'note':'按设备序号检查；网格基于设备原始本地时间。时间跳变不等于丢记录，重复时间不等于重复数据；不推断 UTC、不改写原始时间。'}


def format_report(report):
    lines=['采样时间诊断 · 只读原始本地时间，不访问蓝牙',
           '时间跳变不能直接判为丢点；按序号保留重复时间。','']
    for item in report['devices']:
        lines+=[f"设备 {item['serial']} · 预期 {item['expected_interval_seconds']} 秒 · 共 {item['records']} 条",
                f"最近 {item['recent_records']} 条："+('全部落在预期网格' if item['recent_grid_aligned'] else '未全部落在预期网格（或尚无记录）'),
                '最近间隔：'+('均符合预期间隔' if item['recent_intervals_match'] else '不全符合或样本不足')+'；最新记录：'+str(item['last_record_time']),
                '最近网格偏移：'+', '.join(f"{p['offset_seconds']}秒 × {p['records']}条" for p in item['recent_phase_counts']),
                f"全历史：重复时间 {item['duplicate_time_pairs']} 对，时间倒退 {item['backward_time_pairs']} 对，非预期间隔 {item['irregular_pairs']} 对。"]
        for change in item['recent_transitions'][-8:]:
            lines.append(f"序号 {change['previous_sequence']}→{change['sequence']}：{change['previous_time']} → {change['time']}（{change['delta_seconds']}秒）")
        lines+=['校时审计：']
        for action in item.get('clock_actions',[])[:5]:
            lines.append(f"{action['started_utc']}（电脑UTC） {action['status']}；设备前后时间见 CLI JSON 详细报告。")
        lines+=['设置间隔、校时或重装电池后，须检查新采样时间；软件不会把时间四舍五入到整点。','']
    return lines
