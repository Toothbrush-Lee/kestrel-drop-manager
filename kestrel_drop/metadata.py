"""Local inventory labels and current participant codes, independent of BLE identity."""
import csv
import os
import tempfile
from pathlib import Path
import re

from .devices import serial_number, model_type


def clean(value,key):
    if key not in ('management_code','participant_code'):raise ValueError('不支持的设备资料字段')
    if not isinstance(value,str):raise ValueError('编号必须是文本')
    value=value.strip()
    maximum=32 if key=='management_code' else 64
    if value and not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,'+str(maximum-1)+'}',value):
        raise ValueError(f'编号须以字母或数字开始，只含字母、数字、下划线、点、短横线，最多{maximum}字符；留空表示未填写')
    return value.upper() if key=='management_code' else value


def label_plan(store,path):
    with Path(path).expanduser().open(encoding='utf-8-sig',newline='') as stream:
        reader=csv.DictReader(stream)
        if not {'设备序列号','管理编号'}<=set(reader.fieldnames or []):raise ValueError('CSV需要设备序列号、管理编号两列')
        if len(reader.fieldnames)!=len(set(reader.fieldnames)):raise ValueError('CSV表头包含重复列名')
        rows=list(reader)
    if not rows:raise ValueError('编号表没有设备')
    known={r['serial']:r for r in store.status()}
    updates=[];seen=set();codes=set()
    for line,row in enumerate(rows,2):
        if None in row:raise ValueError(f'CSV第{line}行：列数超出表头')
        serial=serial_number((row['设备序列号'] or '').strip());code=clean(row['管理编号'],'management_code')
        if serial in seen or not code or code in codes:raise ValueError(f'CSV第{line}行：序列号或管理编号为空/重复')
        seen.add(serial);codes.add(code)
        if serial not in known:raise ValueError(f'设备 {serial} 尚未登记，请先核验登记')
        if re.match(r'^D[123]-',code) and code[:2]!=model_type(known[serial]['model']):raise ValueError(f'设备 {serial} 管理编号型号前缀不符')
        if row.get('型号') and model_type(row['型号'])!=model_type(known[serial]['model']):raise ValueError(f'设备 {serial} 型号与数据库不一致')
        if row.get('编号') and row['编号'].strip().upper()!=f'{code}-{serial}':raise ValueError(f'设备 {serial} 组合编号不一致')
        if known[serial]['management_code'] and known[serial]['management_code']!=code:
            raise ValueError(f"设备 {serial} 的管理编号 {known[serial]['management_code']} 已锁定，CSV不能重新编号")
        fields={'management_code':code}
        # A labels-only import never silently clears an existing participant assignment.
        if (row.get('受试者代号') or '').strip():fields['participant_code']=clean(row['受试者代号'],'participant_code')
        updates.append((serial,fields))
    for serial,fields in updates:
        for other,item in known.items():
            if other!=serial and item.get('management_code')==fields['management_code']:
                raise ValueError(f"管理编号 {fields['management_code']} 已被 {other} 占用；已有标签不可转给其他设备")
    return updates


def export_labels(store,path):
    path=Path(path).expanduser().resolve()
    records=store.status()
    fd,temporary=tempfile.mkstemp(prefix='.kestrel-labels-',suffix='.csv',dir=path.parent)
    temporary=Path(temporary)
    try:
        with os.fdopen(fd,'w',encoding='utf-8-sig',newline='') as stream:
            writer=csv.writer(stream);writer.writerow(['编号','管理编号','设备序列号','型号','设备别名','受试者代号'])
            for item in records:
                code=item['management_code'];serial=item['serial']
                writer.writerow([f'{code}-{serial}' if code else serial,code,serial,item['model'],item['alias'],item['participant_code']])
            stream.flush();os.fsync(stream.fileno())
        os.link(temporary,path)
        from .protocol import _fsync_directory
        _fsync_directory(path.parent)
    finally:temporary.unlink(missing_ok=True)
    return {'output':str(path),'devices':len(records)}
