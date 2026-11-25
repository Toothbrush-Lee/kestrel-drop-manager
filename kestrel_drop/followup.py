"""Arrival-driven follow-up collection, bounded concurrency and per-session deduplication."""
import asyncio
from datetime import datetime
import time
import math
import uuid

from . import devices,events,protocol
from .errors import describe


def validate(parallel,scan_seconds,max_attempts):
    if type(parallel) is not int or not 1<=parallel<=8:raise ValueError('随访并发数须为1–8')
    if type(scan_seconds) not in (int,float) or not math.isfinite(scan_seconds) or not 5<=scan_seconds<=60:raise ValueError('随访扫描窗口须为5–60秒')
    if type(max_attempts) is not int or not 1<=max_attempts<=5:raise ValueError('随访尝试次数须为1–5')


async def run(library,*,parallel=6,scan_seconds=8,max_attempts=3,rounds=None,execute=False,sync_clock=False,allow_schedule_shift=False):
    validate(parallel,scan_seconds,max_attempts)
    if sync_clock and not allow_schedule_shift:raise ValueError('自动校时可能改变采样相位，需 --allow-schedule-shift 明确确认')
    if rounds is not None and (type(rounds) is not int or rounds<1):raise ValueError('扫描轮数须为正整数')
    if not execute:return {'status':'dry_run','parallel':parallel,'scan_seconds':scan_seconds,'registered_only':True,'sync_clock':sync_clock}
    registered={d['serial']:d for d in library.status()}
    if not registered:raise ValueError('请先核验登记设备，再进入随访模式')
    session=library.root/'followups'/(datetime.now().strftime('%Y%m%d-%H%M%S')+'-'+uuid.uuid4().hex[:8]+'.json')
    session.parent.mkdir(parents=True,exist_ok=True)
    states={};active={};waiting={};downloads={};scan_round=0
    # Addresses from verified past operations also identify custom advertising names.
    known_addresses={}
    for serial in registered:
        for audit in library.store.actions(serial,limit=100):
            address=audit['before'].get('ble_id')
            if address:
                known_addresses.setdefault(address.casefold(),set()).add(serial)
                break
    def save():
        protocol._json_atomic(session,{'mode':'followup','parallel':parallel,'round':scan_round,'sync_clock':sync_clock,'devices':states,'downloads':list(downloads.values())})
    def publish(found):
        summary={'round':scan_round,'parallel':parallel,'active':sum(i['status']=='downloading' for i in states.values()),'waiting':len(waiting),
                 'saved':sum(i['status']=='saved' for i in states.values()),
                 'clock_warnings':sum(i.get('warning',{}).get('code')=='followup_clock_warning' for i in states.values()),
                 'failed':sum(i['status'] in ('retry','blocked','interrupted','not_attempted') for i in states.values()),'session':str(session)}
        events.result({'followup':summary,'devices':found,'downloads':list(downloads.values())})
    async def collect(serial,observation,snapshot):
        item=states[serial];item.update(status='downloading',attempts=item['attempts']+1,attempt_time=datetime.now().astimezone().isoformat());save()
        try:
            with events.context(serial=serial):
                code=await library.download(serial,execute=True,ble_id=observation['ble_id'],timeout=scan_seconds,discovery=snapshot,clock_policy={'sync':sync_clock,'allow_schedule_shift':allow_schedule_shift})
            outcome=library.download_results[serial];downloads[serial]=outcome
            item.update(status='saved' if not code else 'retry',last_status=outcome['status'])
            if outcome.get('warning'):item['warning']=outcome['warning']
            retry=bool(code)
        except (OSError,RuntimeError,ValueError) as exc:
            error=describe(exc);item.update(status='retry',error=error)
            events.emit('error',serial=serial,**error);retry=error['retryable']
            downloads[serial]={'serial':serial,'status':'failed','error':error}
        except asyncio.CancelledError:
            item['status']='interrupted';raise
        else:
            if not retry:item.pop('error',None)
        finally:save()
        if item['status']!='saved':
            item['status']='retry' if retry and item['attempts']<max_attempts else 'blocked'
            item['retry_after']=time.monotonic()+30*2**(item['attempts']-1)
        save();publish([])
    async def settle():
        for serial,task in list(active.items()):
            if task.done():
                del active[serial]
                await task
    save()
    with library.collection(True,parallel=parallel):
        try:
            while rounds is None or scan_round<rounds:
                await settle();scan_round+=1
                events.emit('phase',phase='followup_scanning')
                try:snapshot=await devices.scan_snapshot(scan_seconds)
                except (OSError,RuntimeError) as exc:
                    events.emit('error',**describe(exc));publish([])
                    await asyncio.sleep(5);continue
                await settle()
                found=devices.candidates(snapshot)
                for observation in found:
                    serial=observation.get('serial')
                    if not serial:
                        matches=known_addresses.get(observation['ble_id'].casefold(),set())
                        if len(matches)==1:serial=observation['serial']=next(iter(matches))
                    if serial not in registered or observation.get('identity_conflict') or registered[serial].get('history_supported') is False:continue
                    item=states.setdefault(serial,{'status':'waiting','attempts':0,'management_code':registered[serial].get('management_code',''),'participant_code':registered[serial].get('participant_code','')})
                    if serial not in active and item['status'] not in ('saved','blocked') and time.monotonic()>=item.get('retry_after',0):
                        waiting[serial]=(dict(observation),snapshot)
                for serial in list(waiting):
                    if len(active)>=parallel:break
                    observation,discovery=waiting.pop(serial)
                    active[serial]=asyncio.create_task(collect(serial,observation,discovery))
                save();publish(found)
            if active:await asyncio.gather(*active.values())
            active.clear();publish([])
        finally:
            for task in active.values():task.cancel()
            if active:await asyncio.gather(*active.values(),return_exceptions=True)
            for item in states.values():
                if item['status']=='waiting':item['status']='not_attempted'
            save()
    return {'followup':{'session':str(session)},'downloads':list(downloads.values()),'incomplete':[serial for serial,item in states.items() if item['status']!='saved']}
