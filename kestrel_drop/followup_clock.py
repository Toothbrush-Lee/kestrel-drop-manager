"""Audited follow-up clock checks; history is downloaded before any optional correction."""
import time
import json
from datetime import datetime

from . import control,events

DRIFT_SECONDS=30
MANUAL_SECONDS=3600
LEVELS={'day':'偏差至少一天','half_day':'偏差至少半天','manual':'偏差至少1小时','drift':'检测到时钟漂移','read_failed':'设备时间读取失败','unreliable_read':'时间读取不够可靠'}


def computer_now():return datetime.now().astimezone()


def severity(seconds):
    difference=abs(seconds)
    return 'day' if difference>=86400 else 'half_day' if difference>=43200 else 'manual' if difference>=MANUAL_SECONDS else 'drift' if difference>=DRIFT_SECONDS else 'normal'


async def check(client,store,identity,*,phase):
    serial=identity['serial']
    metadata=store.data_info(serial)
    before={**identity,'management_code':metadata['management_code'],'participant_code':metadata['participant_code'],'computer_time':computer_now().isoformat()}
    action=store.begin_action(serial,{'followup_clock_check':True,'phase':phase},before)
    started=time.monotonic();wall_start=computer_now()
    try:
        value,wire=await control.clock(client)
        wall_end=computer_now();elapsed=time.monotonic()-started
        midpoint=wall_start+(wall_end-wall_start)/2
        delta=(datetime.fromisoformat(value)-midpoint.replace(tzinfo=None)).total_seconds()
        reliable=elapsed<=5 and abs((wall_end-wall_start).total_seconds()-elapsed)<=2 and wall_start.utcoffset()==wall_end.utcoffset()
        report={'device_time':value,'device_time_wire_hex':wire,'computer_time':midpoint.isoformat(),
                'offset_seconds':round(delta,3),'round_trip_seconds':round(elapsed,3),'uncertainty_seconds':round(elapsed/2+1,3),
                'severity':severity(delta) if reliable else 'unreliable_read','phase':phase,'action_id':action,'read_only':True}
        store.finish_action(action,after=report)
    except BaseException as exc:
        store.finish_action(action,error=f'{type(exc).__name__}: {exc}')
        if not isinstance(exc,Exception):raise
        report={'severity':'read_failed','phase':phase,'action_id':action,'error':str(exc),'read_only':True}
    if report['severity']!='normal':
        message=f"{serial} 时钟告警 [{LEVELS.get(report['severity'],report['severity'])}]：设备减电脑 {report.get('offset_seconds','未知')} 秒；已登记审计 {action}。"
        events.emit('log',level='warning',message=message)
        events.emit('phase',serial=serial,phase='clock_warning',message=message)
    return report


async def correct(client,store,identity,report):
    """Called only after confirmed download and explicit sampling-phase acknowledgement."""
    if report['severity']!='drift':return {'status':'held','reason':report['severity']}
    with store.connect() as connection:
        for previous in connection.execute('SELECT id,status,requested_json FROM device_actions WHERE serial=? ORDER BY id DESC',(identity['serial'],)):
            requested=json.loads(previous['requested_json'])
            if requested.get('time') and previous['status']=='verified':break
            if requested.get('followup_auto_clock') and previous['status']!='verified':
                return {'status':'held_unknown','action_id':previous['id']}
    action=store.begin_action(identity['serial'],{'time':'now','followup_auto_clock':True,'allow_schedule_shift':True},{**identity,'clock_check':report})
    try:
        after=await control.apply(client,{'time':'now'},{},allow_schedule_shift=True,clock_correction_limit=MANUAL_SECONDS)
        store.finish_action(action,after=after)
        return {'status':'verified','action_id':action,'after':after}
    except BaseException as exc:
        store.finish_action(action,error=f'{type(exc).__name__}: {exc}; do not automatically resend clock write')
        if not isinstance(exc,Exception):raise
        return {'status':'unknown','action_id':action,'error':str(exc)}


def warning(report,correction):
    if not report or report['severity']=='normal':return None
    message=LEVELS.get(report['severity'],'时钟异常')+f"；设备减电脑 {report.get('offset_seconds','未知')} 秒"
    if correction and correction['status']=='verified':message+='；历史下载后已按发送时电脑时间校准，采样相位可能改变'
    elif correction and correction['status'] in ('unknown','held_unknown'):message+='；存在未确认校时，禁止自动重发'
    else:message+='；未自动校准'
    return {'code':'followup_clock_warning','title':'设备时钟告警','message':message,'action':'核对电脑本地时间、时区及设备时钟，查看device_actions审计；历史时间戳原样保留。','retryable':False}
