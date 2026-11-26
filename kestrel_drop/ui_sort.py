"""Natural inventory ordering, with empty assignments last and stable identity ties."""
import re

SORT_LABELS={'management':'设备编号','participant':'受试者编号'}


def natural(value):
    return tuple((0,int(part)) if part.isdecimal() else (1,part) for part in re.split(r'(\d+)',str(value or '').strip().casefold()) if part)


def management_key(value):
    match=re.fullmatch(r'D([123])-(\d+)',str(value or '').strip(),re.I)
    return (0,int(match[2]),natural(match[1])) if match else (1,0,natural(value))


def ordered_devices(devices,mode='management'):
    if mode not in SORT_LABELS:raise ValueError('不支持的设备排序')
    primary,secondary=('management_code','participant_code') if mode=='management' else ('participant_code','management_code')
    def key(pair):
        identity,item=pair
        return (not bool(item.get(primary)),(management_key(item.get(primary)) if primary=='management_code' else natural(item.get(primary))),
                not bool(item.get(secondary)),(management_key(item.get(secondary)) if secondary=='management_code' else natural(item.get(secondary))),
                not bool(item.get('serial')),natural(item.get('serial')),str(item.get('ble_id') or ''),str(identity))
    return dict(sorted(devices.items(),key=key))
