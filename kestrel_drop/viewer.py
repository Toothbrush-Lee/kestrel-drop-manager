"""Optional Streamlit UI; all database access is through read-only Browser queries."""
import argparse
from datetime import datetime,timedelta
from pathlib import Path
import sqlite3


def render(root):
    import altair as alt
    import pandas as pd
    import streamlit as st
    from kestrel_drop.browse import Browser,METRICS

    st.set_page_config(page_title='Kestrel DROP 数据浏览',page_icon='📊',layout='wide')
    st.title('Kestrel DROP 数据浏览')
    st.caption('本机只读 SQLite · 设备原始本地时间 · 不校时、不改设置、不按时间戳去重')
    with st.sidebar:
        st.header('选择数据')
        st.caption(str(Path(root)/'library.sqlite'))
        if st.button('刷新数据',help='重新查询已下载的数据库；不会扫描或连接设备'):
            st.session_state.pop('csv',None)
    try:
        browser=Browser(root);catalog=browser.catalog()
    except (OSError,RuntimeError,sqlite3.Error) as exc:
        st.error(f'档案无法打开：{exc}');st.info('先用采集程序下载数据，再核对 --data-dir 指向的数据目录。');return
    if not catalog:st.info('档案中尚无设备；请先完成采集。');return
    by_serial={item['serial']:item for item in catalog}
    if 'devices' not in st.session_state:
        st.session_state.devices=[next((d['serial'] for d in catalog if d['records']),catalog[0]['serial'])]
    st.session_state.devices=[s for s in st.session_state.devices if s in by_serial][:100]
    def label(serial):
        d=by_serial[serial]
        return f"{d['management_code'] or '未编号'} · {serial} · {d['model']}"+(f" · 受试者 {d['participant_code']}" if d['participant_code'] else '')+(f" · {d['alias']}" if d['alias'] else '')
    with st.sidebar:
        a,b=st.columns(2)
        if a.button('全选设备'):st.session_state.devices=list(by_serial)[:100]
        if b.button('清空选择'):st.session_state.devices=[]
        serials=st.multiselect('设备（可多选）',list(by_serial),format_func=label,key='devices',max_selections=100,placeholder='选择需要对比的设备')
        st.caption('最多同时选择100台；可按编号、序列号或受试者代号搜索。')
    if not serials:st.info('请先选择一台或多台设备。');return
    serials=tuple(sorted(serials))
    st.caption(f'已选择 {len(serials)} 台设备 · 当前受试者代号仅表示当前领用，不代表历史观测归属。')
    populated=[by_serial[s] for s in serials if by_serial[s]['records']]
    if not populated:st.info('所选设备已登记，尚无正式观测。下载新记录后点击“刷新数据”。');return
    first=min(datetime.fromisoformat(d['first_time']) for d in populated).replace(second=0,microsecond=0)
    last=max(datetime.fromisoformat(d['last_time']) for d in populated).replace(second=0,microsecond=0)
    scope='|'.join(serials);start_key='start_'+scope;end_key='end_'+scope
    if start_key not in st.session_state:st.session_state[start_key]=max(first,last-timedelta(days=1))
    if end_key not in st.session_state:st.session_state[end_key]=last
    with st.sidebar:
        if st.button('全部历史'):
            st.session_state[start_key]=first;st.session_state[end_key]=last;st.session_state.pop('selection',None)
        if st.button('最近24小时'):
            st.session_state[start_key]=max(first,last-timedelta(days=1));st.session_state[end_key]=last;st.session_state.pop('selection',None)
        with st.form('filters'):
            start=st.datetime_input('开始时间',key=start_key,step=timedelta(minutes=1))
            end=st.datetime_input('结束时间（包含该分钟）',key=end_key,step=timedelta(minutes=1))
            submitted=st.form_submit_button('应用筛选',type='primary')
        st.caption('所有所选设备共用此范围，包含结束分钟的全部记录；默认最近24小时。')
    if start is not None:start=start.replace(second=0,microsecond=0)
    if end is not None:end=end.replace(second=59,microsecond=0)
    selection=st.session_state.get('selection')
    if submitted or not selection or selection[0]!=serials:
        selection=(serials,start,end)
        st.session_state.selection=selection;st.session_state.page=1;st.session_state.pop('csv',None)
    serials,start,end=selection
    selected_metrics=st.multiselect('显示指标',list(METRICS),default=['temperature_c'],format_func=lambda key:f'{METRICS[key][0]} ({METRICS[key][1]})',key='metrics')
    size=st.selectbox('每页记录数',[100,200,500],index=1)
    try:result=browser.read_many(serials,start,end,page=st.session_state.get('page',1),page_size=size)
    except (ValueError,OSError,RuntimeError,sqlite3.Error) as exc:st.error(f'查询未完成：{exc}');return
    st.session_state.page=result['page']
    a,b,c=st.columns(3)
    a.metric('符合条件的记录',f"{result['records']:,}")
    b.metric('有数据的设备',f"{sum(d['records']>0 for d in result['devices'])} / {len(serials)}")
    c.caption('实际记录时间范围');c.text(f"{result['first_time'] or '—'}\n{result['last_time'] or '—'}")
    st.caption(f'已应用范围：{start} 至 {end}（两端包含）。按设备原始时间对比，不自动对齐或修正设备时差。')
    labels={'management_code':'管理编号','serial':'序列号','participant_code':'当前受试者','records':'选区记录数',
            'history_generation':'历史段','device_sequence':'设备序号','timestamp_device_local':'设备原始本地时间',
            **{k:f'{v[0]} ({v[1]})' for k,v in METRICS.items()}}
    def frame(rows,columns):
        enriched=[{**row,'management_code':by_serial[row['serial']]['management_code'] or '未编号',
                   'participant_code':by_serial[row['serial']]['participant_code'] or '未分配',
                   'device_label':(by_serial[row['serial']]['management_code'] or '未编号')+' · '+row['serial']} for row in rows]
        data=pd.DataFrame(enriched).reindex(columns=columns)
        for field in ('device_sequence','history_generation'):
            if field in data:data[field]=data[field].map(lambda value:str(int(value)) if pd.notna(value) else '—')
        return data
    st.subheader('各设备最新数值')
    st.caption('显示每台设备在已应用范围内按采集顺序的末条原始记录；不是实时蓝牙读数，各台时间可能不同。空值表示选区无记录或该型号不支持该指标。')
    latest=[{'serial':d['serial'],**(d['latest'] or {}),'records':d['records']} for d in result['devices']]
    st.dataframe(frame(latest,['management_code','serial','participant_code','records','timestamp_device_local','history_generation','device_sequence',*selected_metrics]).style.format({key:str for key in selected_metrics},na_rep='—'),column_config=labels,hide_index=True,width='stretch')
    if not result['records']:st.info('此时间范围没有记录，请修改筛选范围。');return
    st.subheader('多设备对比')
    if result['stride']>1:st.info(f"概览按设备分别抽样，共 {len(result['points']):,} 点，保留每台首末记录；抽样可能漏掉极值，表格与CSV不抽样。")
    st.caption('颜色区分设备；拖动和缩放，悬停查看编号、时间、序号与数值。点图不插值，相同时间的不同序号仍是独立观测。')
    points=frame(result['points'],['device_label','serial','history_generation','device_sequence','timestamp_device_local',*METRICS])
    points['原始时间']=points['timestamp_device_local']
    color=alt.Color('device_label:N',title='设备',sort=[(by_serial[s]['management_code'] or '未编号')+' · '+s for s in serials])
    for field in selected_metrics:
        metric_label,unit=METRICS[field]
        if points[field].notna().sum()==0:st.caption(f'{metric_label}：所选设备或选区没有该指标。');continue
        chart=alt.Chart(points).mark_circle(size=24,opacity=.7).encode(
            x=alt.X('timestamp_device_local:T',title='设备原始本地时间'),
            y=alt.Y(field+':Q',title=f'{metric_label} ({unit})',scale=alt.Scale(zero=False)),color=color,
            tooltip=[alt.Tooltip('device_label:N',title='设备'),alt.Tooltip('原始时间:N',title='原始时间'),alt.Tooltip('history_generation:O',title='历史段'),alt.Tooltip('device_sequence:N',title='设备序号'),alt.Tooltip(field+':Q',title=metric_label)]).interactive()
        st.altair_chart(chart,width='stretch')
    st.subheader('原始记录')
    left,middle,right=st.columns([1,3,1])
    def move(step):st.session_state.page=max(1,min(result['pages'],result['page']+step))
    left.button('上一页',on_click=move,args=(-1,),disabled=result['page']==1)
    middle.caption(f"第 {result['page']} / {result['pages']} 页 · 按序列号、历史段、设备序号排列；列头排序只影响当前页。")
    right.button('下一页',on_click=move,args=(1,),disabled=result['page']==result['pages'])
    st.dataframe(frame(result['rows'],['management_code','serial','participant_code','history_generation','device_sequence','timestamp_device_local',*selected_metrics]).style.format({key:str for key in selected_metrics},na_rep='—'),column_config=labels,hide_index=True,width='stretch')
    st.caption('表格下载只含当前页。下方导出包含所有所选设备的整个已应用范围、全部指标及序列号，最多合计10万条；不将当前受试者回填为历史归属。')
    if st.button('准备导出筛选结果',disabled=result['records']>100000):
        try:
            body,count=browser.csv_many(serials,start,end)
            st.session_state.csv=(selection,body,count)
        except (ValueError,OSError,RuntimeError,sqlite3.Error) as exc:st.error(str(exc))
    exported=st.session_state.get('csv')
    if exported and exported[0]==selection:
        st.download_button(f'下载CSV（{exported[2]:,}条）',exported[1],file_name=f'kestrel-{serials[0] if len(serials)==1 else "multi-device"}.csv',mime='text/csv',on_click='ignore')
        st.caption('CSV是点击准备时的数据库快照；需要最新结果时重新准备。')
    if result['records']>100000:st.caption('选区超过浏览器导出上限，请缩小时间范围，或用CLI export流式导出。')


def main():
    from kestrel_drop.storage import default_root
    parser=argparse.ArgumentParser()
    parser.add_argument('--data-dir',type=Path,default=None)
    args=parser.parse_args()
    render(args.data_dir or default_root())


if __name__=='__main__':main()
