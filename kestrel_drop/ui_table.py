"""Shared terminal-cell column definitions for headers and rows; no I/O."""
from .ui_text import clip,text_width


def fit_cell(value,width,align='left'):
    text=str(value)
    text=clip(text,text_width(text))
    if text_width(text)>width:text=clip(text,max(0,width-1))+'…' if width else ''
    padding=' '*max(0,width-text_width(text))
    return padding+text if align=='right' else text+padding


def device_columns(width):
    if width<66:raise ValueError('设备表至少需要66个显示单元')
    columns=[['mark','选',2,'left'],['kind','型号',4,'left'],
             ['code','管理编号',8,'left'],['serial','序列号',8,'left'],
             ['participant','受试者',8,'left'],['location','发现',4,'left'],
             ['records','已存',6,'right'],['pending','未确认',6,'right'],
             ['status','状态/末条',12,'left']]
    extra=width-66
    # Preserve a complete timestamp first, then give participant identifiers room.
    for key,limit in (('status',19),('participant',16),('code',10),('serial',10),('records',8),('pending',8)):
        column=next(column for column in columns if column[0]==key)
        growth=min(extra,limit-column[2]);column[2]+=growth;extra-=growth
    columns[-1][2]+=extra
    return columns


PREVIEW_COLUMNS=(('time','记录时间',19,'left'),('temperature','温度°C',10,'right'),
                 ('humidity','湿度%',10,'right'),('pressure','气压hPa',11,'right'))


def table_row(columns,values=None):
    return ' '.join(fit_cell(title if values is None else values.get(key,''),width,align)
                    for key,title,width,align in columns)


def result_columns(width,*,configuration=False):
    columns=[['code','管理编号',8,'left'],['serial','序列号',8,'left'],
             ['participant','受试者',8,'left'],['status','状态',14,'left']]
    if not configuration:columns.extend([['added','新增',6,'right'],['total','累计',8,'right'],['pending','未确认',6,'right']])
    extra=width-sum(column[2] for column in columns)-len(columns)+1
    if extra<0:raise ValueError('结果表显示宽度不足')
    for key,limit in (('participant',16),('code',10),('serial',10)):
        column=next(column for column in columns if column[0]==key)
        growth=min(extra,limit-column[2]);column[2]+=growth;extra-=growth
    columns[3][2]+=extra
    return columns
