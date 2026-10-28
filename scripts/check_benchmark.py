"""Measure full validation on synthetic single-device and fleet archives.

No BLE, user settings or real archives are accessed. Intermediates are temporary.
Python allocation peak excludes fixture creation and is not process RSS.
"""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import sqlite3
import sys
import tempfile
import time
import tracemalloc

from performance import seed
from kestrel_drop import __version__


def measure(operation):
    started=time.perf_counter();operation();elapsed=time.perf_counter()-started
    tracemalloc.start()
    try:
        operation();peak=tracemalloc.get_traced_memory()[1]/1048576
    finally:tracemalloc.stop()
    return {'seconds_without_trace':round(elapsed,4),'peak_python_mib':round(peak,3)}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--records',type=int,nargs='+',default=[100000])
    parser.add_argument('--devices',type=int,default=100)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if args.devices<1 or any(n<args.devices or n%args.devices for n in args.records):
        parser.error('Each record count must be a positive multiple of --devices')
    if args.output.exists():raise FileExistsError(args.output)
    result={'version':__version__,'python':platform.python_version(),'sqlite':sqlite3.sqlite_version,
            'platform':platform.platform(),'database_source_sha256':hashlib.sha256((Path(__file__).resolve().parents[1]/'kestrel_drop/database.py').read_bytes()).hexdigest(),
            'note':'Synthetic repeated timestamps; allocation peak excludes seeding; not RSS or BLE throughput.', 'scenarios':{}}
    with tempfile.TemporaryDirectory(prefix='kestrel-check-benchmark-') as temporary:
        for total in args.records:
            for devices in dict.fromkeys((1,args.devices)):
                per_device=total//devices;name=f'{devices}x{per_device}'
                print(f'Checking {name}',flush=True)
                root=Path(temporary)/name;root.mkdir()
                library=seed(root,devices,per_device)
                metrics=measure(library.store.check)
                metrics['records']=total
                result['scenarios'][name]=metrics
    args.output.parent.mkdir(parents=True,exist_ok=True)
    with args.output.open('x',encoding='utf-8') as stream:json.dump(result,stream,ensure_ascii=False,indent=2)
    print(args.output)


if __name__=='__main__':main()
