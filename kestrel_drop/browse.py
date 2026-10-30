"""Read-only, bounded SQLite queries and launcher for the optional data browser."""
from contextlib import contextmanager
from datetime import datetime
import csv
import importlib.util
import io
import math
from pathlib import Path
import subprocess
import sys
import time

from .database import Store
from .devices import serial_number

METRICS = {
    'temperature_c': ('温度', '°C'),
    'relative_humidity_pct': ('相对湿度', '%'),
    'station_pressure_hpa': ('站点气压', 'hPa'),
    'heat_index_c': ('热指数', '°C'),
    'dew_point_c': ('露点', '°C'),
    'density_altitude_m_raw': ('密度高度', 'm'),
}
COLUMNS = ('serial', 'history_generation', 'device_sequence', 'timestamp_device_local', *METRICS)
SELECT = 'h.serial,h.generation AS history_generation,o.absolute_cursor+1 AS device_sequence,o.timestamp_device_local,' + ','.join('o.'+m for m in METRICS)
FROM = ' FROM observations o JOIN histories h ON h.id=o.history_id'
ORDER = 'h.generation,o.absolute_cursor'


def timestamp(value):
    if isinstance(value,str): value=datetime.fromisoformat(value)
    if not isinstance(value,datetime) or value.tzinfo is not None or value.microsecond:
        raise ValueError('请选择精确到秒的设备本地时间，不附加时区')
    return value.isoformat(sep=' ',timespec='seconds')


def filters(serial,start,end):
    serial=serial_number(serial);start=timestamp(start);end=timestamp(end)
    if start>end:raise ValueError('开始时间不能晚于结束时间')
    return ' WHERE h.serial=? AND o.timestamp_device_local>=? AND o.timestamp_device_local<=?',(serial,start,end)


class Browser:
    def __init__(self,root):
        self.root=Path(root).expanduser().resolve()
        self.store=Store(self.root/'library.sqlite')

    @contextmanager
    def snapshot(self):
        with self.store.connect() as connection:
            deadline=time.monotonic()+15
            connection.set_progress_handler(lambda: time.monotonic()>deadline,10000)
            connection.execute('BEGIN')
            yield connection

    def catalog(self):
        with self.snapshot() as c:
            return [dict(r) for r in c.execute('SELECT '+self.store.metadata_columns(c)+''',d.serial,d.model,d.alias,
                count(o.absolute_cursor) AS records,min(o.timestamp_device_local) AS first_time,
                max(o.timestamp_device_local) AS last_time FROM devices d JOIN histories h USING(serial)
                LEFT JOIN observations o ON o.history_id=h.id GROUP BY d.serial ORDER BY d.serial''')]

    def read(self,serial,start,end,*,page=1,page_size=200,chart_limit=2000):
        if type(page) is not int or not 1<=page<=10000000:raise ValueError('页码无效')
        if type(page_size) is not int or not 1<=page_size<=500:raise ValueError('每页须为1–500条')
        if type(chart_limit) is not int or not 10<=chart_limit<=5000:raise ValueError('图表上限须为10–5000点')
        where,args=filters(serial,start,end)
        with self.snapshot() as c:
            self.store._history(c,serial_number(serial))
            summary=dict(c.execute('SELECT count(*) AS records,min(o.timestamp_device_local) AS first_time,max(o.timestamp_device_local) AS last_time'+FROM+where,args).fetchone())
            pages=max(1,math.ceil(summary['records']/page_size));page=min(page,pages)
            rows=[dict(r) for r in c.execute('SELECT '+SELECT+FROM+where+' ORDER BY '+ORDER+' LIMIT ? OFFSET ?',(*args,page_size,(page-1)*page_size))]
            stride=max(1,math.ceil(summary['records']/chart_limit))
            # The row-number sample retains distinct sequence IDs, including equal timestamps.
            sql='WITH numbered AS (SELECT '+SELECT+',row_number() OVER (ORDER BY '+ORDER+') AS n'+FROM+where+') SELECT '+','.join(COLUMNS)+' FROM numbered WHERE (n-1)%?=0 OR n=? ORDER BY n'
            points=[dict(r) for r in c.execute(sql,(*args,stride,summary['records']))]
            return {**summary,'rows':rows,'points':points,'page':page,'pages':pages,'stride':stride}

    def csv(self,serial,start,end,*,max_records=100000,max_bytes=32*1024*1024):
        where,args=filters(serial,start,end)
        with self.snapshot() as c:
            self.store._history(c,serial_number(serial))
            count=c.execute('SELECT count(*)'+FROM+where,args).fetchone()[0]
            if count>max_records:raise ValueError('浏览器单次导出上限10万条；请缩小范围，或使用CLI export导出完整历史')
            stream=io.StringIO(newline='');writer=csv.DictWriter(stream,fieldnames=COLUMNS)
            writer.writeheader()
            cursor=c.execute('SELECT '+SELECT+FROM+where+' ORDER BY '+ORDER,args)
            while batch:=cursor.fetchmany(1000):
                writer.writerows(dict(row) for row in batch)
                if stream.tell()>max_bytes:raise ValueError('导出内容超过浏览器容量；请缩小时间范围')
            body=stream.getvalue().encode('utf-8-sig')
            if len(body)>max_bytes:raise ValueError('导出内容超过浏览器容量；请缩小时间范围')
            return body,count


def launch(root,*,port=8501,open_browser=True):
    if not 1024<=port<=65535:raise ValueError('浏览端口须为1024–65535')
    Browser(root)  # Never silently create an empty database for a wrong path.
    if getattr(sys,'frozen',False):
        raise ValueError('此独立采集包不含可选浏览组件；请在源码目录运行 uv run --extra viewer kestrel.py browse')
    if importlib.util.find_spec('streamlit') is None:
        raise ValueError('请运行 uv run --extra viewer kestrel.py browse，首次会安装可选浏览组件')
    from . import viewer
    command=[sys.executable,'-m','streamlit','run',str(Path(viewer.__file__).resolve()),
             '--server.address','127.0.0.1','--server.port',str(port),
             '--server.headless',str(not open_browser).lower(),'--browser.gatherUsageStats','false','--client.toolbarMode','viewer',
             '--','--data-dir',str(Path(root).resolve())]
    print(f'只读数据浏览：http://127.0.0.1:{port} · Ctrl+C关闭；数据库：{Path(root)/"library.sqlite"}',flush=True)
    try:return subprocess.call(command)
    except KeyboardInterrupt:return 130
