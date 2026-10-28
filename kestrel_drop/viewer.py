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

    st.set_page_config(page_title='Kestrel DROP 数据浏览',page_icon='📊',layout='wide',initial_sidebar_state='expanded')
    st.title('Kestrel DROP 数据浏览')
    st.caption('本机只读 SQLite · 设备原始本地时间 · 不校时、不改设置、不按时间戳去重')
    with st.sidebar:
        st.header('选择数据')
        st.caption(str(Path(root)/'library.sqlite'))
        st.button('刷新数据',help='重新查询已下载的数据库；不会扫描或连接设备')
    try:
        browser=Browser(root);catalog=browser.catalog()
    except (OSError,RuntimeError,sqlite3.Error) as exc:
        st.error(f'档案无法打开：{exc}');st.info('先用采集程序下载数据，再核对 --data-dir 指向的数据目录。');return
    if not catalog:st.info('档案中尚无设备；请先完成采集。');return
    by_serial={item['serial']:item for item in catalog}
    with st.sidebar:
        serial=st.selectbox('设备',list(by_serial),format_func=lambda key:f"{key} · {by_serial[key]['model']}"+(f" · {by_serial[key]['alias']}" if by_serial[key]['alias'] else ''),key='device')
    device=by_serial[serial]
    st.caption(f"设备 {serial} · {device['model']}")
    if not device['records']:st.info('此设备已登记，尚无正式观测。');return
    first=datetime.fromisoformat(device['first_time']).replace(second=0);last=datetime.fromisoformat(device['last_time']).replace(second=0)
    start_key='start_'+serial;end_key='end_'+serial
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
        st.caption('按设备本地时间筛选到分钟，包含结束分钟的全部记录；默认最近24小时。')
    if start is not None:start=start.replace(second=0,microsecond=0)
    if end is not None:end=end.replace(second=59,microsecond=0)
    selection=st.session_state.get('selection')
    if submitted or not selection or selection[0]!=serial:
        selection=(serial,start,end)
        st.session_state.selection=selection;st.session_state.page=1;st.session_state.pop('csv',None)
    serial,start,end=selection
    selected_metrics=st.multiselect('显示指标',list(METRICS),default=['temperature_c'],format_func=lambda key:f'{METRICS[key][0]} ({METRICS[key][1]})')
    size=st.selectbox('每页记录数',[100,200,500],index=1)
    try:result=browser.read(serial,start,end,page=st.session_state.get('page',1),page_size=size)
    except (ValueError,OSError,RuntimeError,sqlite3.Error) as exc:st.error(f'查询未完成：{exc}');return
    st.session_state.page=result['page']
    a,b,c=st.columns(3)
    a.metric('符合条件的记录',f"{result['records']:,}")
    b.caption('开始记录');b.text(result['first_time'] or '—')
    c.caption('结束记录');c.text(result['last_time'] or '—')
    st.caption(f'已应用范围：{start} 至 {end}（两端包含）。原始时间可能因校时倒退或重复；设备序号保留原始采集顺序。')
    if not result['records']:st.info('此时间范围没有记录，请修改筛选范围。');return
    st.subheader('数据概览')
    if result['stride']>1:st.info(f"概览每 {result['stride']} 条抽取一点，共 {len(result['points']):,} 点，包含首末记录；抽样可能漏掉极值。表格与CSV保留原记录。")
    st.caption('点图不插值、不取整；相同时间的不同序号仍是独立观测。拖动和缩放图表，悬停查看时间与序号。')
    points=pd.DataFrame(result['points'])
    points['device_sequence']=points['device_sequence'].map(str)
    points['原始时间']=points['timestamp_device_local']
    for field in selected_metrics:
        label,unit=METRICS[field]
        if points[field].notna().sum()==0:st.caption(f'{label}：此设备或选区没有该指标。');continue
        chart=alt.Chart(points).mark_circle(size=20,opacity=.65).encode(
            x=alt.X('timestamp_device_local:T',title='设备原始本地时间'),
            y=alt.Y(field+':Q',title=f'{label} ({unit})',scale=alt.Scale(zero=False)),
            tooltip=[alt.Tooltip('原始时间:N',title='原始时间'),alt.Tooltip('history_generation:O',title='历史段'),alt.Tooltip('device_sequence:N',title='设备序号'),alt.Tooltip(field+':Q',title=label)]).interactive()
        st.altair_chart(chart,width='stretch')
    st.subheader('原始记录')
    left,middle,right=st.columns([1,3,1])
    def move(step):st.session_state.page=max(1,min(result['pages'],result['page']+step))
    left.button('上一页',on_click=move,args=(-1,),disabled=result['page']==1)
    middle.caption(f"第 {result['page']} / {result['pages']} 页 · 默认按历史段和设备序号排列；列头排序只影响当前页。")
    right.button('下一页',on_click=move,args=(1,),disabled=result['page']==result['pages'])
    columns=['history_generation','device_sequence','timestamp_device_local',*selected_metrics]
    labels={'history_generation':'历史段','device_sequence':'设备序号','timestamp_device_local':'设备原始本地时间',**{k:f'{v[0]} ({v[1]})' for k,v in METRICS.items()}}
    table=pd.DataFrame(result['rows'])[columns]
    table['device_sequence']=table['device_sequence'].map(str)
    st.dataframe(table,column_config=labels,hide_index=True,width='stretch')
    st.caption('表格右上角下载只含当前页。下方“导出筛选结果”包含整个选区和全部测量字段，最多10万条。')
    if st.button('准备导出筛选结果',disabled=result['records']>100000):
        try:
            body,count=browser.csv(serial,start,end)
            st.session_state.csv=(selection,body,count)
        except (ValueError,OSError,RuntimeError,sqlite3.Error) as exc:st.error(str(exc))
    exported=st.session_state.get('csv')
    if exported and exported[0]==selection:
        st.download_button(f'下载CSV（{exported[2]:,}条）',exported[1],file_name=f'kestrel-{serial}.csv',mime='text/csv',on_click='ignore')
        st.caption('CSV为点击准备导出时的数据库快照；需要最新结果时重新准备导出。')
    if result['records']>100000:st.caption('选区超过浏览器导出上限，请缩小时间范围，或用CLI export流式导出。')


def main():
    from kestrel_drop.storage import default_root
    parser=argparse.ArgumentParser()
    parser.add_argument('--data-dir',type=Path,default=None)
    args=parser.parse_args()
    render(args.data_dir or default_root())


if __name__=='__main__':main()
