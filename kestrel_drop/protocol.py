"""Kestrel DROP D3: verified offline decoder and experimental BLE history/sync client.

The original full download succeeded on one user-owned D3. New incremental sync
is validated through simulations, not yet field-tested on hardware.
Run: uv run scripts/kestrel_protocol.py --help
"""
from __future__ import annotations

import argparse
import asyncio
from collections import Counter, defaultdict
import csv
import hashlib
import fcntl
from contextlib import contextmanager
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
from statistics import median
import time
from pathlib import Path
import struct
import sys

SYNC = 0x7E
ESC = 0x7D
UP_UUID = "85920100-0338-4b83-ae4a-ac1d217adb03"
DOWN_UUID = "85920200-0338-4b83-ae4a-ac1d217adb03"
SERIAL_UUID = "00002a25-0000-1000-8000-00805f9b34fb"
MODEL_UUID = "00002a24-0000-1000-8000-00805f9b34fb"
RECORD_BYTES = 21


def crc16_x25(data: bytes) -> int:
    """CRC-16/IBM-SDLC (aka X-25): reflected poly 0x8408, init/xorout 0xffff."""
    result = 0xFFFF
    for byte in data:
        result ^= byte
        for _ in range(8):
            result = (result >> 1) ^ (0x8408 if result & 1 else 0)
    return result ^ 0xFFFF


def stuff(raw: bytes) -> bytes:
    return bytes(out for c in raw for out in ((ESC, c ^ 0x20) if c in (SYNC, ESC) else (c,)))


def unstuff(raw: bytes) -> bytes:
    result = bytearray()
    i = 0
    while i < len(raw):
        if raw[i] == ESC:
            if i + 1 >= len(raw):
                raise ValueError("Dangling escape")
            result.append(raw[i + 1] ^ 0x20)
            i += 2
        else:
            result.append(raw[i])
            i += 1
    return bytes(result)


@dataclass
class Frame:
    kind: int
    payload: bytes
    raw: bytes
    def to_json(self):
        return {'kind': f'0x{self.kind:02x}', 'payload_hex': self.payload.hex(),
                'payload_length': len(self.payload), 'raw_hex': self.raw.hex()}


def encode_frame(kind: int, payload: bytes) -> bytes:
    body = bytes((kind, 0)) + len(payload).to_bytes(2, 'little') + payload
    body += crc16_x25(body).to_bytes(2, 'little')
    return bytes((SYNC,)) + stuff(body) + bytes((SYNC,))


def decode_frame(body_escaped: bytes) -> Frame:
    body = unstuff(body_escaped)
    if len(body) < 6:
        raise ValueError('Short frame')
    if body[1] != 0:
        raise ValueError('Unexpected reserved byte')
    length = int.from_bytes(body[2:4], 'little')
    if len(body) != 6 + length:
        raise ValueError(f'Frame length mismatch: advertised {length}, received {len(body)-6}')
    actual = int.from_bytes(body[-2:], 'little')
    expected = crc16_x25(body[:-2])
    if actual != expected:
        raise ValueError(f'CRC mismatch: received {actual:04x}, expected {expected:04x}')
    return Frame(body[0], body[4:-2], bytes((SYNC,)) + body_escaped + bytes((SYNC,)))


class FrameStream:
    """Parser for GATT chunks padded to MTU length after terminating 0x7e."""
    def __init__(self):
        self.buffer = bytearray()
        self.started = False
        self.errors: list[str] = []

    def feed(self, packet: bytes) -> list[Frame]:
        frames = []
        for byte in packet:
            if byte == SYNC:
                if self.started and self.buffer:
                    try:
                        frames.append(decode_frame(bytes(self.buffer)))
                    except ValueError as exc:
                        self.errors.append(str(exc))
                    self.started = False
                    self.buffer.clear()
                else:
                    self.started = True
                    self.buffer.clear()
            elif self.started:
                self.buffer.append(byte)
                if len(self.buffer) > 1024 * 1024:
                    self.errors.append('Frame exceeded 1 MiB')
                    self.started = False
                    self.buffer.clear()
            # If not started: ignore zero padding after end delimiter.
        return frames


@dataclass
class Att:
    timestamp: str
    hci_handle: int
    direction: str
    opcode: int
    attr_handle: int | None
    value: bytes


def iter_packetlogger(path: Path):
    """Apple PacketLogger little-endian record: size u32, seconds u32,
    microseconds u32, kind u8, payload. kind 2/3 = sent/received ACL."""
    blob = path.read_bytes()
    offset = 0
    while offset < len(blob):
        if offset + 13 > len(blob):
            raise ValueError(f'Truncated PacketLogger record at offset {offset}')
        size, sec, micros = struct.unpack_from('<III', blob, offset)
        if size < 9 or offset + 4 + size > len(blob):
            raise ValueError(f'Invalid PacketLogger size {size} at {offset}')
        kind = blob[offset + 12]
        data = blob[offset + 13:offset + 4 + size]
        # PacketLogger timestamps may be rendered with a system-specific timezone;
        # this is a naive clock reading, not the sensor-record timestamp.
        stamp = datetime.fromtimestamp(sec,timezone.utc).strftime('%Y-%m-%d %H:%M:%S') + f'.{micros:06d}'
        yield stamp, kind, data
        offset += 4 + size


def extract_att(path: Path) -> list[Att]:
    packets = []
    for timestamp, direction_type, data in iter_packetlogger(path):
        if direction_type not in (2, 3) or len(data) < 9:
            continue
        word, size = struct.unpack_from('<HH', data)
        connection = word & 0x0FFF
        pb = (word >> 12) & 3
        if size != len(data)-4:
            continue
        # Only process unfragmented fixed ATT L2CAP frames. No guesswork when
        # fragmented: fail by omitting, reported by mismatch on comparison.
        if pb not in (0, 2) or len(data) < 8:
            continue
        l2len, cid = struct.unpack_from('<HH', data, 4)
        if cid != 4 or l2len != len(data) - 8:
            continue
        att = data[8:]
        opcode = att[0]
        has_handle = opcode in (0x0A, 0x12, 0x52, 0x1B, 0x1D)
        attr = int.from_bytes(att[1:3], 'little') if has_handle and len(att) >= 3 else None
        value = att[3:] if attr is not None else att[1:]
        packets.append(Att(timestamp,connection,'TX' if direction_type==2 else 'RX',opcode,attr,value))
    return packets


def signed24(data: bytes) -> int:
    x = int.from_bytes(data, 'little')
    return x - (1 << 24) if x >= (1 << 23) else x


def decode_record(rec: bytes, *, block: int, position: int) -> dict:
    if len(rec) != RECORD_BYTES:
        raise ValueError(f'Expected 21 bytes, got {len(rec)}')
    ss, mm, hh, day, month = rec[:5]
    year = int.from_bytes(rec[5:7], 'little')
    date = datetime(year, month, day, hh, mm, ss)
    temp = int.from_bytes(rec[7:9], 'little', signed=True)
    humidity = int.from_bytes(rec[9:11], 'little')
    heat = int.from_bytes(rec[11:13], 'little', signed=True)
    extra = rec[13]
    dew = int.from_bytes(rec[14:16], 'little', signed=True)
    pressure = int.from_bytes(rec[16:18], 'little')
    density = signed24(rec[18:21])
    return {
        'timestamp_device_local': date.isoformat(sep=' '),
        'temperature_c': temp / 100,
        'relative_humidity_pct': humidity / 100,
        'station_pressure_hpa': pressure / 10,
        'heat_index_c': heat / 100,
        'dew_point_c': dew / 100,
        'density_altitude_m_raw': density / 10,
        'density_altitude_m_official_integer_estimate': density // 10,
        'unknown_byte': extra,
        'block': block,
        'position_in_block': position,
        'record_hex': rec.hex(),
    }


def log_rows_from_frames(frames: list[Frame]) -> tuple[list[dict], list[dict]]:
    rows = []
    meta = []
    for f in frames:
        if f.kind != 3 or len(f.payload) < 2 or (len(f.payload)-2) % RECORD_BYTES:
            continue
        if len(f.payload) == 2:
            continue
        block = int.from_bytes(f.payload[:2], 'little')
        chunk = f.payload[2:]
        decoded = []
        for n in range(0, len(chunk), RECORD_BYTES):
            decoded.append(decode_record(chunk[n:n+RECORD_BYTES], block=block, position=n//RECORD_BYTES))
        meta.append({'block':block,'records':len(decoded),'length':len(f.payload)})
        rows.extend(decoded)
    if meta:
        seqs = [x['block'] for x in meta]
        if sorted(seqs) != list(range(min(seqs),max(seqs)+1)) or len(seqs)!=len(set(seqs)):
            raise ValueError('Missing or duplicate log blocks: '+str(seqs))
    return rows,meta


def read_official_csv(path: Path) -> tuple[dict,dict]:
    with path.open(encoding='utf-8-sig',newline='') as f:
        original=list(csv.reader(f))
    if len(original)<5:raise ValueError('Not a Kestrel LiNK CSV')
    meta={r[0]:r[1] for r in original[:3] if len(r)>1}
    header=original[3]
    result={}
    for row in original[5:]:
        if len(row)<7 or not row[0].startswith('20'):continue
        result[row[0]] = dict(zip(header,row))
    return meta,result


def compare(rows, ref):
    matches=[]
    fields=[('temperature_c','Temperature',0.051),
            ('relative_humidity_pct','Relative Humidity',0.051),
            ('station_pressure_hpa','Station Pressure',0.051),
            ('heat_index_c','Heat Index',0.051),
            ('dew_point_c','Dew Point',0.051)]
    result={'reference_rows':len(ref),'decoded_rows':len(rows), 'matched_timestamps':0,
            'missing_timestamps':[], 'tolerance_failures':[], 'fields':{}}
    for field,label,tolerance in fields:
        result['fields'][field]={'checked':0,'within_display_tolerance':0,'max_abs_difference':0.0}
    result['fields']['density_altitude_m_official_integer_estimate']={'checked':0,'exact_display_matches':0}
    for row in rows:
        key=row['timestamp_device_local']
        refrow=ref.get(key)
        if refrow is None:
            result['missing_timestamps'].append(key)
            continue
        result['matched_timestamps']+=1
        comparison={'timestamp':key}
        for field,label,tolerance in fields:
            source=refrow.get(label)
            if not source:continue
            diff=abs(row[field]-float(source))
            stat=result['fields'][field]
            stat['checked']+=1
            stat['max_abs_difference']=round(max(stat['max_abs_difference'],diff),6)
            if diff<=tolerance:stat['within_display_tolerance']+=1
            else:result['tolerance_failures'].append({'time':key,'field':field,'diff':diff})
            comparison[field+'_official']=float(source)
            comparison[field+'_absdiff']=round(diff,6)
        if refrow.get('Density Altitude'):
            stat=result['fields']['density_altitude_m_official_integer_estimate']
            stat['checked']+=1
            estimated=row['density_altitude_m_official_integer_estimate']
            actual=int(refrow['Density Altitude'])
            if estimated==actual:stat['exact_display_matches']+=1
            else:result['tolerance_failures'].append({'time':key,'field':'density_altitude','estimate':estimated,'official':actual})
            comparison['density_altitude_official_m']=actual
        matches.append(comparison)
    result['passed']=(result['matched_timestamps']==len(rows) and not result['tolerance_failures'])
    return result,matches


def write_csv(path, rows):
    if not rows:return
    with path.open('w',newline='',encoding='utf-8') as f:
        wr=csv.DictWriter(f,fieldnames=list(rows[0]),extrasaction='ignore')
        wr.writeheader();wr.writerows(rows)


def decode_capture(args):
    pklg=Path(args.pklg)
    out=Path(args.output)
    out.mkdir(parents=True,exist_ok=True)
    packets=extract_att(pklg)
    groups=defaultdict(list)
    for pkt in packets:
        if pkt.direction=='RX' and pkt.opcode==0x1D and pkt.attr_handle is not None:
            groups[(pkt.hci_handle,pkt.attr_handle)].append(pkt)
    if not groups:raise RuntimeError('No ATT indications found; check capture')
    streams={}
    for key,grp in groups.items():
        parser=FrameStream();f=[]
        for packet in grp:f+=parser.feed(packet.value)
        streams[key]=(f,parser.errors)
    valid_keys=[(key, sum(1 for frame in fs if frame.kind==3 and len(frame.payload)>4)) for key,(fs,errs) in streams.items()]
    valid_keys.sort(key=lambda x:x[1],reverse=True)
    if args.connection is not None:
        wanted=[item for item in valid_keys if item[0][0]==args.connection]
        if not wanted:raise ValueError(f'No matching indication stream in connection {args.connection}')
        key=wanted[0][0]
    else:key=valid_keys[0][0]
    frames,errors=streams[key]
    rows,blocks=log_rows_from_frames(frames)
    if not rows:raise RuntimeError(f'No log records decoded from connection={key[0]}, attr={key[1]:#x}')
    if errors:raise RuntimeError(f'Invalid frames detected: {errors}')
    refmeta=refrows={};comparison=None;comparisons=[]
    if args.reference:
        refmeta,refrows=read_official_csv(Path(args.reference))
        comparison,comparisons=compare(rows,refrows)
    conn,indicate_handle=key
    writes=[p for p in packets if p.hci_handle==conn and p.direction=='TX' and p.opcode in (0x12,0x52) and p.value.startswith(b'\x7e')]
    requests=[]
    for p in writes:
        raw=p.value
        try:
            f=decode_frame(raw[1:-1])
            request={'time':p.timestamp,'attr_handle':f'0x{p.attr_handle:04x}',**f.to_json()}
            if f.kind==0 and len(f.payload)>=6:
                request['operation']=int.from_bytes(f.payload[:2],'little')
                request['cursor_uint32_le']=int.from_bytes(f.payload[2:6],'little')
            requests.append(request)
        except ValueError:
            pass
    # Find completion control messages emitted by the device.
    completions=[{'operation':int.from_bytes(f.payload[:2],'little'),
                  'next_cursor':int.from_bytes(f.payload[2:6],'little')}
                 for f in frames if f.kind==0 and len(f.payload)>=6]
    statuses=[int.from_bytes(f.payload,'little') for f in frames if f.kind==3 and len(f.payload)==4]
    report={
        'pklg':str(pklg),'reference':str(args.reference) if args.reference else None,
        'reference_device_meta':refmeta,
        'connection_handle':conn,'indication_attr_handle':f'0x{indicate_handle:04x}',
        'capture_total_att_packets':len(packets),'indication_count':len(groups[key]),
        'decoded_valid_frames':len(frames),'frame_types':dict(Counter(f'0x{f.kind:02x}' for f in frames)),
        'decoded_log_blocks':blocks,'decoded_rows':len(rows),
        'first_record':rows[0]['timestamp_device_local'],'last_record':rows[-1]['timestamp_device_local'],
        'status_record_counts':statuses,'completion_messages':completions,
        'captured_commands':requests,'comparison':comparison,
        'decoder_limitations':['Record layout and all values validated on one D3 capture only',
                               'Unknown 1-byte record field not assigned meaning',
                               'D2 layout unverified',
                               'Incremental sync has simulated tests but awaits D3 field verification']}
    write_csv(out/'decoded_history.csv', rows)
    write_csv(out/'comparison.csv', comparisons)
    write_csv(out/'att_timeline.csv',[{'timestamp_packetlogger':p.timestamp,
       'connection':p.hci_handle,'direction':p.direction,'opcode':hex(p.opcode),
       'att_handle':hex(p.attr_handle) if p.attr_handle is not None else '',
       'payload_hex':p.value.hex()} for p in packets if p.hci_handle==conn and p.opcode in (0x12,0x52,0x1D,0x1B)])
    (out/'report.json').write_text(json.dumps(report,indent=2,ensure_ascii=False),encoding='utf-8')
    print(json.dumps({'connection':conn,'attr_handle':hex(indicate_handle), 'blocks':len(blocks),
                      'rows':len(rows),'status_counts':statuses, 'completion':completions,
                      'comparison_passed':comparison['passed'] if comparison else None,
                      'out':str(out)},indent=2,ensure_ascii=False))
    if comparison and not comparison['passed']:return 1
    return 0


class TransferProgress:
    """Compact live progress bar, no third-party dependencies.

    Progress percentage uses the count reported BEFORE downloading; the DROP
    may append new samples while transferring, so 100% of the initial count
    does not imply that the transfer has completed. Only the device completion
    message followed by verification and file publishing marks success.
    """
    def __init__(self, expected: int, *, stream=None, clock=None,
                 terminal: bool | None = None, refresh_seconds: float = 1.0,
                 log_seconds: float = 15.0):
        self.expected = max(1, expected)
        self.stream = stream if stream is not None else sys.stderr
        self.clock = clock or time.monotonic
        self.terminal = (bool(self.stream.isatty()) if terminal is None else terminal)
        self.refresh_seconds = refresh_seconds
        self.log_seconds = log_seconds
        self.started = self.clock()
        self.last_print = float('-inf')
        self.received = 0
        self.closed = False
        self.update(0, force=True)

    @staticmethod
    def duration(seconds: float | None) -> str:
        if seconds is None or not math.isfinite(seconds):
            return '--:--'
        secs = max(0, int(seconds))
        hours, rem = divmod(secs, 3600)
        minutes, remaining = divmod(rem, 60)
        return (f'{hours:02d}:{minutes:02d}:{remaining:02d}'
                if hours else f'{minutes:02d}:{remaining:02d}')

    def update(self, received: int, *, force: bool = False, status: str = '') -> None:
        if self.closed:
            return
        self.received = received
        now = self.clock()
        interval = self.refresh_seconds if self.terminal else self.log_seconds
        if not force and now - self.last_print < interval:
            return
        self.last_print = now
        elapsed = max(0.0, now - self.started)
        rate = received / elapsed if elapsed > 0 and received else 0.0
        ratio = min(1.0, received / self.expected)
        width = 20
        solid = min(width, int(ratio * width))
        bar = '[' + '#' * solid + '-' * (width - solid) + ']'
        extra = received - self.expected
        extra_label = f' (+{extra} new)' if extra > 0 else ''
        eta = (self.expected - received) / rate if rate > 0 and received < self.expected else None
        if received >= self.expected and status == '':
            status = 'awaiting end'
        suffix = f' | {status}' if status else ''
        line = (f'  {bar} {ratio * 100:5.1f}%  '
                f'{received:,}/{self.expected:,}{extra_label}  '
                f'{rate:5.1f} rec/s  elapsed {self.duration(elapsed)}  '
                f'ETA {self.duration(eta)}{suffix}')
        if self.terminal:
            self.stream.write('\r' + line + '\x1b[K')
        else:
            self.stream.write(line + '\n')
        self.stream.flush()

    def finish(self, *, success: bool, partial_saved: bool = False) -> None:
        if self.closed:
            return
        self.update(self.received, force=True,
                    status=('saved and verified' if success else
                            'stopped; partial CSV retained' if partial_saved else
                            'stopped / not saved'))
        if self.terminal:
            self.stream.write('\n')
            self.stream.flush()
        self.closed = True


class BleSession:
    """Reverse-engineered GATT client; field-tested on D3 full downloads only."""
    def __init__(self,client):
        self.client=client
        self.parser=FrameStream()
        self.queue:asyncio.Queue[Frame]=asyncio.Queue()

    def callback(self,_ch,payload):
        for frame in self.parser.feed(bytes(payload)):
            self.queue.put_nowait(frame)

    async def send(self,kind:int,payload:bytes):
        await self.client.write_gatt_char(DOWN_UUID,encode_frame(kind,payload),response=True)

    async def next(self,timeout=25) -> Frame:
        try:
            return await asyncio.wait_for(self.queue.get(),timeout=timeout)
        except asyncio.TimeoutError:
            raise RuntimeError('Timed out waiting for next device frame') from None

    async def ack(self,operation:int):
        await self.send(4,operation.to_bytes(2,'little'))

    async def query_count(self,cursor:int) -> int:
        await self.send(0,(3).to_bytes(2,'little')+cursor.to_bytes(4,'little')+b'\x00'*4)
        while True:
            fr=await self.next()
            if fr.kind==4:continue
            if fr.kind==3 and len(fr.payload)==4:
                count=int.from_bytes(fr.payload,'little')
                await self.ack(3)
                return count
            raise RuntimeError(f'Unexpected response during query: {fr.to_json()}')

    async def download_from_cursor(self,cursor:int,expected:int,maxrows:int,
                                   progress: TransferProgress | None = None, on_block=None):
        await self.send(0,(5).to_bytes(2,'little')+cursor.to_bytes(4,'little')+b'\x00'*4)
        rows=[];blocks=[];next_cursor=None
        seen=set()
        while True:
            frame=await self.next(timeout=45)
            if frame.kind==4:
                continue
            if frame.kind==1:
                # The observed original LiNK client acknowledges the field schema
                # using opcode 05 before blocks are sent.
                await self.ack(5)
                continue
            if frame.kind==3 and len(frame.payload)>=2 and (len(frame.payload)-2)%21==0:
                new,meta=log_rows_from_frames([frame])
                seq=meta[0]['block']
                if seq in seen:raise RuntimeError(f'Duplicate block {seq}')
                if len(rows)+len(new)>maxrows:raise RuntimeError('Exceeded configured record limit')
                if on_block is not None:
                    on_block(new, meta)  # durably persist this CRC-checked block BEFORE ACK
                seen.add(seq);rows.extend(new);blocks.extend(meta)
                if progress is not None:
                    progress.update(len(rows))
                await self.ack(5)
                continue
            if frame.kind==0 and len(frame.payload)>=6 and frame.payload[:2]==b'\x12\x00':
                next_cursor=int.from_bytes(frame.payload[2:6],'little')
                await self.ack(18)
                break
            raise RuntimeError('Unexpected frame while reading logs: '+str(frame.to_json()))
        if self.parser.errors:raise RuntimeError(f'Failed frame integrity: {self.parser.errors}')
        # The device can continue logging while a long download is running.
        # Count from the preceding query is a snapshot, NOT a fixed transfer length.
        # Final validation below checks the completed cursor, block continuity,
        # chronology and whether extra rows are plausible for elapsed time.
        if next_cursor is None:
            raise RuntimeError('Missing final completion cursor; will not treat download as complete')
        if next_cursor != cursor + len(rows):
            # DROP keeps a monotonically increasing absolute log index. A
            # request at cursor 0 can mean "oldest still retained" when the
            # log has already rotated. The completion cursor then is NOT
            # necessarily equal to the requested zero + returned row count.
            # This is a hypothesis informed by the real iPhone capture; it
            # must be reported explicitly instead of throwing away all rows.
            if cursor != 0 or next_cursor < len(rows):
                raise RuntimeError(
                    f'Unexpected completion cursor: device says {next_cursor}, '
                    f'expected {cursor + len(rows)} for requested cursor {cursor}'
                )
        return rows,blocks,next_cursor



def verify_completed_transfer(*, rows, blocks, initial_count: int, requested_cursor: int,
                              next_cursor: int, elapsed_seconds: float, maxrows: int) -> dict:
    """Check a completed BLE log transfer, allowing fresh records appended during sync.

    Accepting a larger row count is deliberate and bounded by the observed
    logging cadence, elapsed time and the firmware's final completion cursor.
    This does NOT mask missing blocks, incomplete transfers, or invalid frames.
    """
    actual = len(rows)
    if actual == 0:
        raise RuntimeError('Completion contained no historical records')
    if actual > maxrows:
        raise RuntimeError(f'Completed transfer exceeded --maxrows ({actual} > {maxrows})')
    if actual < initial_count:
        raise RuntimeError(
            f'Incomplete transfer: initial query reported {initial_count}, '
            f'but only {actual} records arrived. Refusing to label as complete.')

    ids = [int(block['block']) for block in blocks]
    if not ids or ids != list(range(ids[0], ids[0] + len(ids))):
        raise RuntimeError(f'Missing, duplicated or out-of-order log blocks: {ids[:12]}...')
    if sum(block['records'] for block in blocks) != actual:
        raise RuntimeError('Number of records does not match block metadata')

    timestamps = [datetime.fromisoformat(row['timestamp_device_local']) for row in rows]
    if timestamps != sorted(timestamps):
        raise RuntimeError('History timestamps are out of chronological order')
    sampled_times = timestamps[-129:]
    differences = [(b-a).total_seconds() for a,b in zip(sampled_times, sampled_times[1:])]
    positive_intervals = [x for x in differences if x > 0]
    period = median(positive_intervals) if positive_intervals else None

    if next_cursor is None or next_cursor < actual:
        raise RuntimeError(f'Invalid final cursor {next_cursor} for {actual} records')
    inferred_first = next_cursor - actual
    if requested_cursor != 0 and inferred_first != requested_cursor:
        raise RuntimeError(f'Final cursor disagrees with requested cursor: '
                           f'{next_cursor} - {actual} = {inferred_first}, '
                           f'but requested {requested_cursor}')

    extra = actual - initial_count
    plausible_extra_limit = None
    if period is not None:
        # A small safety margin handles an update near the count-query boundary.
        plausible_extra_limit = max(2, math.ceil(max(0, elapsed_seconds) / period) + 2)
        if extra > plausible_extra_limit:
            raise RuntimeError(
                f'Unexpected growth: {extra} additional records over '
                f'{elapsed_seconds:.1f}s, with ~{period:g}s logging cadence '
                f'(plausible limit {plausible_extra_limit}). '
                'Do not silently accept these records.')

    return {
        'query_count_at_start': initial_count,
        'actual_received_records': actual,
        'new_records_during_transfer': extra,
        'download_elapsed_seconds': round(elapsed_seconds, 3),
        'estimated_record_interval_seconds': period,
        'plausible_extra_records_limit': plausible_extra_limit,
        'requested_cursor': requested_cursor,
        'inferred_first_cursor': inferred_first,
        'next_cursor': next_cursor,
        'cursor_zero_clamped_to_oldest': bool(requested_cursor == 0 and inferred_first != 0),
        'verification': 'complete_with_growth' if extra else 'complete',
    }


def publish_live_files(output: Path, rows: list[dict], metadata: dict) -> None:
    """Stage both files before publishing; never silently overwrite past exports."""
    meta = output.with_suffix(output.suffix + '.json')
    if output.exists() or meta.exists():
        raise FileExistsError(f'CSV or metadata already exists: {output} / {meta}')
    tmp_csv = output.with_name(output.name + '.tmp')
    tmp_json = meta.with_name(meta.name + '.tmp')
    if tmp_csv.exists() or tmp_json.exists():
        raise FileExistsError('Previous unfinished temp output exists; investigate before retrying')
    try:
        write_csv(tmp_csv, rows)
        tmp_json.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding='utf-8')
        os.replace(tmp_csv, output)
        os.replace(tmp_json, meta)
    finally:
        tmp_csv.unlink(missing_ok=True)
        tmp_json.unlink(missing_ok=True)


async def resolve_ble_device(args):
    """Find D3 right before connecting; return a BLEDevice, never a stale UUID.

    On macOS, providing only a UUID to BleakClient triggers an implicit BLE
    discovery. Reusing the scanner's BLEDevice is more reliable. We also match
    the advertised D3 serial name if CoreBluetooth gives a new local UUID.
    """
    from bleak import BleakScanner

    advertisements = await BleakScanner.discover(
        timeout=args.scan_timeout, return_adv=True
    )
    target = (args.ble_id or "").casefold()
    expected_name = f"D3 - {args.serial}"
    nearby = []
    exact = []
    by_serial_name = []
    for device, adv in advertisements.values():
        name = (adv.local_name or device.name or '').strip()
        nearby.append((name, device.address, adv.rssi))
        if target and device.address.casefold() == target:
            exact.append(device)
        elif name.casefold() == expected_name.casefold():
            by_serial_name.append(device)

    if exact:
        print(f"Found target in fresh scan: {exact[0].address}")
        return exact[0]
    if by_serial_name:
        print(f"Found {expected_name} under current macOS UUID "
              f"{by_serial_name[0].address}; previous UUID {args.ble_id} "
              "will not be used. Device serial is checked before writing.")
        return by_serial_name[0]

    near_drops = [f"{name or '(unnamed)'} ({address}, {rssi} dBm)"
                  for name, address, rssi in nearby
                  if name.lower().startswith(('d2', 'd3'))]
    details = '; '.join(near_drops[:8]) if near_drops else 'none'
    raise RuntimeError(
        f"D3 serial {args.serial} not seen during a {args.scan_timeout:g}s "
        f"BLE scan ({len(nearby)} devices discovered; nearby DROP: {details}). "
        "Disconnect D3 in iPhone Kestrel LiNK and close the app, move the "
        "D3 close to the Mac, and retry. No GATT commands were sent."
    )


# ---------------- Durable incremental CSV synchronization ----------------
# The main CSV is a committed snapshot. Each validated 5-record BLE block is
# flushed/fsynced to <csv>.pending.csv BEFORE the device receives its ACK.
# Only a confirmed end-of-transfer cursor permits publication to the main CSV.
ARCHIVE_VERSION = 1


@contextmanager
def _archive_lock(output: Path):
    """Fail fast when another process owns the same CSV archive (macOS/Linux)."""
    path = Path(str(output) + '.lock')
    fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as e:
            raise RuntimeError(f'Another download is already using {output}; '
                               'do not run two sync jobs against one archive') from e
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _archive_paths(output: Path) -> dict[str, Path]:
    name = str(output)
    return {
        'csv': output,
        'state': Path(name + '.sync-state.json'),
        'pending': Path(name + '.pending.csv'),
        'journal': Path(name + '.pending.json'),
        'staged': Path(name + '.staging.csv'),
    }


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for buf in iter(lambda: stream.read(1 << 20), b''):
            digest.update(buf)
    return digest.hexdigest()


def _json_atomic(path: Path, obj: dict) -> None:
    temp = path.with_name(path.name + '.writing')
    if temp.exists():
        raise RuntimeError(f'Previous incomplete metadata write: {temp}. Inspect before retrying.')
    try:
        with temp.open('x', encoding='utf-8') as fd:
            json.dump(obj, fd, indent=2, ensure_ascii=False)
            fd.write('\n')
            fd.flush()
            os.fsync(fd.fileno())
        os.replace(temp, path)
    finally:
        temp.unlink(missing_ok=True)


def _rows_checked(path: Path) -> tuple[list[str], list[dict]]:
    with path.open('r', encoding='utf-8-sig', newline='') as f:
        reader = csv.DictReader(f)
        names = list(reader.fieldnames or [])
        if not names or not {'timestamp_device_local', 'record_hex'} <= set(names):
            raise RuntimeError(f'Invalid DROP CSV header in {path}')
        rows = list(reader)
    for i, row in enumerate(rows, 1):
        if None in row or not row.get('record_hex'):
            raise RuntimeError(f'Incomplete/corrupt CSV row {i} in {path}; kept unchanged')
        try:
            raw = bytes.fromhex(row['record_hex'])
            decoded = decode_record(raw, block=0, position=0)
            if row['timestamp_device_local'] != decoded['timestamp_device_local']:
                raise ValueError('timestamp differs from raw record')
            for key in ('temperature_c','relative_humidity_pct','station_pressure_hpa',
                        'heat_index_c','dew_point_c','density_altitude_m_raw'):
                if abs(float(row[key]) - decoded[key]) > 0.000001:
                    raise ValueError(f'{key} differs from raw record')
        except (ValueError, TypeError, KeyError) as e:
            raise RuntimeError(f'CSV record {i} failed integrity check in {path}: {e}') from e
    return names, rows


def _check_order(rows: list[dict]) -> None:
    times = [datetime.fromisoformat(row['timestamp_device_local']) for row in rows]
    if times != sorted(times):
        raise RuntimeError('Record timestamps go backwards; will not commit')
    # Equal timestamps are legitimate observations; never deduplicate on time.


def adopt_archive(output: Path, metadata: Path) -> dict:
    paths = _archive_paths(output)
    if paths['state'].exists() or paths['pending'].exists() or paths['journal'].exists():
        raise RuntimeError('An archive state or pending transfer already exists; refusing to replace it')
    if not output.exists():
        raise FileNotFoundError(f'Existing verified full CSV not found: {output}')
    meta = json.loads(metadata.read_text(encoding='utf-8'))
    validation = meta.get('validation') or {}
    if 'next_cursor' not in validation:
        raise RuntimeError('Full-download metadata lacks validation.next_cursor')
    names, rows = _rows_checked(output)
    if not rows or len(rows) != meta.get('rows'):
        raise RuntimeError('CSV row count does not match downloaded metadata')
    _check_order(rows)
    if rows[0]['timestamp_device_local'] != meta.get('first_record_time') or \
       rows[-1]['timestamp_device_local'] != meta.get('last_record_time'):
        raise RuntimeError('CSV first/last time does not match metadata')
    next_cursor = int(validation['next_cursor'])
    if next_cursor < len(rows) or next_cursor - len(rows) != validation.get('inferred_first_cursor'):
        raise RuntimeError('Full download cursor inconsistent with CSV row count')
    state = {
        'version': ARCHIVE_VERSION,
        'serial': str(meta.get('serial')),
        'model': str(meta.get('model')),
        'next_cursor': next_cursor,
        'row_count': len(rows),
        'last_record_time': rows[-1]['timestamp_device_local'],
        'last_record_hex': rows[-1]['record_hex'],
        'csv_sha256': _sha256(output),
        'fieldnames': names,
        'adopted_from': str(metadata),
        'successful_incremental_syncs': 0,
    }
    if not state['serial'].isdigit() or 'DROP 3' not in state['model'].upper():
        raise RuntimeError('This sync implementation has been tested for D3 only')
    _json_atomic(paths['state'], state)
    return state


def _state_loaded(paths: dict) -> dict:
    if not paths['state'].exists():
        raise RuntimeError('Archive not initialized. Run `adopt` using a validated full CSV and its .csv.json metadata.')
    state = json.loads(paths['state'].read_text(encoding='utf-8'))
    if state.get('version') != ARCHIVE_VERSION:
        raise RuntimeError('Unsupported state version')
    return state


def _pending_loaded(paths: dict, state: dict) -> tuple[dict | None, list[dict]]:
    if not paths['journal'].exists() and not paths['pending'].exists():
        return None, []
    if not paths['journal'].exists() or not paths['pending'].exists():
        raise RuntimeError('Only one of pending CSV/journal exists; preserve files for recovery, do not delete them')
    journal = json.loads(paths['journal'].read_text(encoding='utf-8'))
    if (journal.get('serial'),journal.get('base_cursor'), journal.get('base_csv_sha256')) != (
         state['serial'],state['next_cursor'],state['csv_sha256']):
        raise RuntimeError('Pending transfer is from a different archive version/device; cannot resume')
    names, rows = _rows_checked(paths['pending'])
    if names != state['fieldnames']:
        raise RuntimeError('Pending CSV header differs from canonical archive')
    _check_order(rows)
    if rows and journal.get('base_last_time'):
        if datetime.fromisoformat(rows[0]['timestamp_device_local']) < datetime.fromisoformat(journal['base_last_time']):
            raise RuntimeError('Pending data precedes last committed reading')
    return journal, rows


def _new_pending(paths: dict, state: dict) -> dict:
    if paths['pending'].exists() or paths['journal'].exists():
        raise RuntimeError('Pending files already exist')
    journal = {'version': ARCHIVE_VERSION, 'serial':state['serial'],
               'base_cursor': state['next_cursor'],
               'base_csv_sha256': state['csv_sha256'],
               'base_last_time': state['last_record_time'],
               'created_utc': datetime.now(timezone.utc).isoformat()}
    with paths['pending'].open('x',encoding='utf-8',newline='') as f:
        writer = csv.DictWriter(f,fieldnames=state['fieldnames'])
        writer.writeheader()
        f.flush();os.fsync(f.fileno())
    _json_atomic(paths['journal'],journal)
    return journal


class BlockSpool:
    def __init__(self,path: Path,names: list[str],existing: int = 0):
        self.path = path
        self.names = names
        self.count = existing

    def append(self,rows: list[dict],_block_meta: list[dict]) -> None:
        # Every CRC-checked, decoded block is appended and fsynced BEFORE ACK.
        # A process interruption can therefore retain already received blocks.
        with self.path.open('a',encoding='utf-8',newline='') as fd:
            writer = csv.DictWriter(fd,fieldnames=self.names, extrasaction='ignore')
            writer.writerows(rows)
            fd.flush()
            os.fsync(fd.fileno())
        self.count += len(rows)


def _commit_state_from_completed(paths: dict, state: dict, journal: dict, pending: list[dict]):
    complete = journal.get('completed')
    if complete is None:
        raise RuntimeError('No verified device completion cursor for pending rows; refusing to commit')
    next_cursor = int(complete['next_cursor'])
    if next_cursor != state['next_cursor'] + len(pending):
        raise RuntimeError('Completion cursor does not match pending row count and original cursor')
    if not pending:
        raise RuntimeError('Empty pending transfer')
    new_state = {**state,
        'next_cursor': next_cursor,
        'row_count': state['row_count'] + len(pending),
        'last_record_time': pending[-1]['timestamp_device_local'],
        'last_record_hex': pending[-1]['record_hex'],
        'csv_sha256': complete['staged_sha256'],
        'successful_incremental_syncs': state.get('successful_incremental_syncs',0) + 1,
        'last_sync_utc': datetime.now(timezone.utc).isoformat(),
        'last_sync_validation': complete.get('validation'),
    }
    _json_atomic(paths['state'],new_state)
    paths['pending'].unlink()
    paths['journal'].unlink()
    return new_state


def commit_pending(paths: dict, state: dict):
    journal,pending = _pending_loaded(paths,state)
    if journal is None or not journal.get('completed'):
        raise RuntimeError('Pending transfer has no validated end-of-transfer message')
    csvpath=paths['csv']
    current_digest = _sha256(csvpath)
    completed=journal['completed']
    if current_digest == completed.get('staged_sha256'):
        # Previous run was interrupted between replacing CSV and updating state.
        print('Recovering previously committed CSV; updating state without duplicating rows.', file=sys.stderr)
        return _commit_state_from_completed(paths,state,journal,pending)
    if current_digest != state['csv_sha256']:
        raise RuntimeError('Main CSV differs from saved state; refusing to append/reconcile blindly')
    if not pending:
        raise RuntimeError('No pending data to publish')
    if not csvpath.read_bytes().endswith(b'\n'):
        raise RuntimeError('Canonical CSV does not end in a line break')
    content = paths['pending'].read_bytes()
    if b'\n' not in content:
        raise RuntimeError('Pending CSV has no header line')
    pending_body = content.split(b'\n',1)[1]
    if paths['staged'].exists():
        paths['staged'].unlink()  # Rebuild only from verified archive & verified pending.
    with paths['staged'].open('xb') as fd:
        with csvpath.open('rb') as base:
            shutil.copyfileobj(base,fd)
        fd.write(pending_body)
        fd.flush();os.fsync(fd.fileno())
    digest = _sha256(paths['staged'])
    completed['staged_sha256'] = digest
    _json_atomic(paths['journal'],journal)  # journal before first atomic publish
    os.replace(paths['staged'],csvpath)
    return _commit_state_from_completed(paths,state,journal,pending)


def _check_archive_or_recover(paths: dict, state: dict) -> dict:
    if not paths['csv'].exists():
        raise FileNotFoundError('Canonical CSV missing; no data changed')
    actual = _sha256(paths['csv'])
    if actual == state['csv_sha256']:
        journal,pending = _pending_loaded(paths,state)
        if journal and journal.get('completed'):
            return commit_pending(paths,state)
        return state
    journal,pending = _pending_loaded(paths,state)
    if journal and journal.get('completed') and actual == journal['completed'].get('staged_sha256'):
        return commit_pending(paths,state)
    raise RuntimeError('Canonical CSV checksum changed independently; manual inspection needed, nothing overwritten')


async def sync_live(args):
    with _archive_lock(Path(args.output)):
        return await _sync_live_locked(args)


async def _sync_live_locked(args):
    if not args.execute:
        print('DRY RUN: no BLE commands sent. Use --execute to opt into experimental synchronization.')
        return 0
    paths = _archive_paths(Path(args.output))
    state = _check_archive_or_recover(paths, _state_loaded(paths))
    if state['serial'] != args.serial:
        raise RuntimeError('Requested serial differs from established archive serial; no BLE commands sent')
    journal, already = _pending_loaded(paths,state)
    base = state['next_cursor']
    resume = base + len(already)
    print(f'Archive {state["serial"]}: {state["row_count"]:,} committed rows, '
          f'{len(already):,} pending rows, requesting from absolute cursor {resume:,}.')
    from bleak import BleakClient
    device = await resolve_ble_device(args)
    async with BleakClient(device,timeout=30) as client:
        serial=(await client.read_gatt_char(SERIAL_UUID)).decode('utf-8','replace').strip('\x00 ')
        model=(await client.read_gatt_char(MODEL_UUID)).decode('utf-8','replace').strip('\x00 ')
        if serial != state['serial'] or 'DROP 3' not in model.upper():
            raise RuntimeError(f'Connected to an unexpected device: {serial} {model}. No command sent.')
        session=BleSession(client)
        await client.start_notify(UP_UUID,session.callback)
        try:
            count=await session.query_count(resume)
            print(f'D3 reports {count:,} new records available since cursor {resume:,}.')
            if not count:
                if already:
                    print('Previous pending rows are preserved but lack a confirmed final transfer. '
                          'Nothing appended. To recover the final partial batch, retry after more samples '
                          'arrive or independently verify that pending data match the device.')
                else:
                    print('Already up to date; canonical CSV left unchanged.')
                return 3 if already else 0
            if count > args.maxrows:
                raise RuntimeError(f'Device reports {count} > --maxrows {args.maxrows}. '
                                   'Older cursor may have been overwritten; investigate before continuing.')
            if journal is None:
                journal = _new_pending(paths,state)
            spool = BlockSpool(paths['pending'],state['fieldnames'],existing=len(already))
            progress=getattr(args, 'progress_factory', TransferProgress)(count)
            started=time.monotonic()
            try:
                rows, blocks, next_cursor = await session.download_from_cursor(
                    resume,count,args.maxrows,progress=progress,on_block=spool.append)
                elapsed=time.monotonic()-started
                verification=verify_completed_transfer(
                    rows=rows, blocks=blocks, initial_count=count,
                    requested_cursor=resume, next_cursor=next_cursor,
                    elapsed_seconds=elapsed,maxrows=args.maxrows)
                _,saved=_rows_checked(paths['pending'])
                if len(saved)!=len(already)+len(rows):
                    raise RuntimeError('Persisted pending rows differ from downloaded rows')
                if len(saved) != next_cursor - base:
                    raise RuntimeError('Pending row count inconsistent with absolute completion cursor')
                _check_order(saved)
                journal['completed'] = {'next_cursor':next_cursor,'validation':verification}
                _json_atomic(paths['journal'],journal)
                new_state = commit_pending(paths,state)
                progress.finish(success=True)
                print(f'Incrementally synchronized {len(rows):,} new records; '
                      f'{new_state["row_count"]:,} rows total. '
                      f'Committed through cursor {new_state["next_cursor"]:,} to {paths["csv"]}.')
            except BaseException:
                progress.finish(success=False, partial_saved=True)
                print(f'Interrupted: already decoded records remain in {paths["pending"]}. '
                      'The canonical CSV was not partially modified.',file=sys.stderr)
                raise
        finally:
            await client.stop_notify(UP_UUID)
    return 0


def archive_info(output: Path) -> dict:
    """Inspect/recover an archive under the same exclusive lock as writers."""
    with _archive_lock(output):
        return _archive_info_locked(output)


def _archive_info_locked(output: Path) -> dict:
    paths=_archive_paths(output)
    state=_check_archive_or_recover(paths,_state_loaded(paths))
    journal,pending=_pending_loaded(paths,state)
    info={'serial':state['serial'],'committed_records':state['row_count'],
          'next_cursor':state['next_cursor'],'last_timestamp':state['last_record_time'],
          'pending_records':len(pending),'resume_cursor':state['next_cursor']+len(pending),
          'pending_has_completion':bool(journal and journal.get('completed')),
          'csv':str(output),'csv_sha256_valid':_sha256(output)==state['csv_sha256']}
    return info


def archive_status(output: Path) -> int:
    print(json.dumps(archive_info(output),indent=2,ensure_ascii=False))
    return 0


async def live_read(args):
    if not args.execute:
        print('DRY RUN: No Bluetooth commands sent. Provide --execute to opt in to the experimental write-based read protocol.')
        return 0
    if not args.serial or args.cursor is None:
        raise ValueError('--serial and --cursor are both required')
    from bleak import BleakClient
    from bleak.exc import BleakError
    output=Path(args.output) if args.cmd=='live' else None
    if output is not None:
        if output.exists():raise FileExistsError(f'Refusing to overwrite existing output: {output}')
        output.parent.mkdir(parents=True,exist_ok=True)
    # Scan and connect within the same event loop using the BLEDevice object.
    # Connecting from a macOS UUID string forces a second implicit discovery.
    device = await resolve_ble_device(args)
    try:
        async with BleakClient(device, timeout=30) as client:
            return await _read_connected_d3(client, args, output)
    except BleakError as exc:
        raise RuntimeError(
            f"BLE connection lost or rejected after discovery: {exc}. "
            "Make sure LiNK on iPhone is disconnected and retry. "
            "No guarantee that any in-progress BLE operation completed."
        ) from exc


async def _read_connected_d3(client, args, output):
    serial=(await client.read_gatt_char(SERIAL_UUID)).decode('utf-8','replace').strip('\x00 ')
    model=(await client.read_gatt_char(MODEL_UUID)).decode('utf-8','replace').strip('\x00 ')
    if serial!=args.serial:
        raise RuntimeError(f'Serial mismatch: expected {args.serial}, connected {serial}. No command sent.')
    if 'DROP 3' not in model.upper():
        raise RuntimeError(f'Unsupported or unexpected model {model!r}; this D3 parser is not valid for D2')
    session=BleSession(client)
    await client.start_notify(UP_UUID,session.callback)
    try:
        count=await session.query_count(args.cursor)
        print(f'Identified {model} serial={serial}, cursor={args.cursor}, device reports {count} remaining records')
        if args.cmd=='probe':
            print('Probe completed. No history data downloaded and no file saved.')
            return 0
        if not count:
            print('No records at this cursor. No output written.')
            return 0
        if count>args.maxrows:
            raise RuntimeError(f'Would download {count} records, above --maxrows {args.maxrows}. Nothing requested.')
        transfer_start = time.monotonic()
        progress = getattr(args, 'progress_factory', TransferProgress)(count)
        partial = Path(str(output) + '.partial.csv') if output is not None else None
        spool = None
        if partial is not None:
            if partial.exists():
                raise FileExistsError(f'Previous partial download retained at {partial}; use a new output path')
            # The schema comes from decoded records; create the spool on the first block.
            def persist_block(new, meta):
                nonlocal spool
                if spool is None:
                    with partial.open('x', encoding='utf-8', newline='') as fd:
                        csv.DictWriter(fd, fieldnames=list(new[0])).writeheader()
                        fd.flush(); os.fsync(fd.fileno())
                    spool = BlockSpool(partial, list(new[0]))
                spool.append(new, meta)
        else:
            persist_block = None
        try:
            rows,blocks,new_cursor=await session.download_from_cursor(
                args.cursor,count,args.maxrows,progress=progress,on_block=persist_block)
            elapsed_seconds = time.monotonic() - transfer_start
            progress.update(len(rows), force=True, status='verifying')
            validation = verify_completed_transfer(
                rows=rows, blocks=blocks, initial_count=count,
                requested_cursor=args.cursor, next_cursor=new_cursor,
                elapsed_seconds=elapsed_seconds, maxrows=args.maxrows)
            metadata = {
                'serial':serial, 'model':model,
                'first_record_time':rows[0]['timestamp_device_local'],
                'last_record_time':rows[-1]['timestamp_device_local'],
                'rows':len(rows), 'blocks':blocks,
                'validation':validation,
                'status':'EXPERIMENTAL LIVE PROTOCOL — VALIDATE AGAINST LINK CSV',
            }
            if partial is not None:
                _, saved = _rows_checked(partial)
                if [r['record_hex'] for r in saved] != [r['record_hex'] for r in rows]:
                    raise RuntimeError('Durable first-download spool differs from received records')
            publish_live_files(output,rows,metadata)
            if partial is not None:
                partial.unlink()
        except BaseException:
            progress.finish(success=False, partial_saved=bool(partial and partial.exists()))
            raise
        else:
            progress.finish(success=True)
        if validation['new_records_during_transfer']:
            print(f"NOTICE: count grew during download: {count} at query time -> "
                  f"{len(rows)} received (+{validation['new_records_during_transfer']}). "
                  "CRC, block continuity, timestamps and completion cursor passed.")
        if validation['cursor_zero_clamped_to_oldest']:
            print(f"NOTICE: requested cursor 0; inferred oldest retained cursor "
                  f"{validation['inferred_first_cursor']}. Verify against LiNK CSV.")
        print(f"Downloaded {len(rows):,} rows to {output} "
              "(EXPERIMENTAL; compare with official LiNK CSV)")
    finally:
        await client.stop_notify(UP_UUID)
    return 0

async def scan_ble(args):
    """List BLE advertisements. Never connects to devices or writes to GATT.

    When macOS/CoreBluetooth does not expose a local name, a DROP service
    advertisement is shown as a *candidate* despite a name filter. A
    candidate MUST be identified by the Device Information serial characteristic
    before issuing any commands in `probe`/`live`.
    """
    from bleak import BleakScanner
    drop_service = '12630000-cc25-497d-9854-9b6c02c77054'
    found = await BleakScanner.discover(timeout=args.timeout, return_adv=True)
    all_devices = []
    matched = []
    for identifier, (device, adv) in found.items():
        name = adv.local_name or device.name or ''
        services = list(adv.service_uuids or [])
        known_drop_service = drop_service in {x.lower() for x in services}
        item = {
            'name': name,
            'ble_identifier': device.address,
            'rssi': adv.rssi,
            'service_uuids': services,
            'drop_service_advertised': known_drop_service,
        }
        all_devices.append(item)
        name_match = not args.name or args.name.casefold() in name.casefold()
        # An unnamed DROP with the known service is a candidate, not a
        # verified D3. Avoid silently dropping it when searching by name.
        service_fallback = bool(args.name and not name and known_drop_service)
        if name_match or service_fallback:
            item['match_reason'] = ('unnamed_DROP_candidate' if service_fallback
                                    else ('name' if args.name else 'unfiltered'))
            matched.append(item)
    matched.sort(key=lambda d: d['rssi'] if d['rssi'] is not None else -999, reverse=True)
    rendered = json.dumps(matched, indent=2, ensure_ascii=False)
    print(rendered)

    if not matched:
        print(f'Scan detected {len(all_devices)} BLE advertisements, but 0 matched '
              f'name filter {args.name!r}.', file=sys.stderr)
        if all_devices:
            nearby = sorted(all_devices, key=lambda d: d['rssi'] if d['rssi'] is not None else -999,
                            reverse=True)[:8]
            labels = ', '.join(f"{item['name'] or '(unnamed)'} ({item['rssi']} dBm)"
                               for item in nearby)
            print(f'Nearby: {labels}', file=sys.stderr)
        print('First disconnect Kestrel LiNK on the iPhone and close the app; '
              'then try: uv run scripts/kestrel_protocol.py scan --timeout 30', file=sys.stderr)
        print('If the unfiltered scan is empty too, check macOS Bluetooth permissions '
              'for the Terminal app and that Bluetooth is enabled.', file=sys.stderr)
    elif args.name and any(x['match_reason'] == 'unnamed_DROP_candidate' for x in matched):
        print('Warning: unnamed devices with the Kestrel DROP service are only '
              'candidates, NOT verified D3 devices. Verify their serial number '
              'before initiating any BLE writes.', file=sys.stderr)

    if args.output:
        out = Path(args.output)
        if out.exists():
            raise FileExistsError(f'Refusing to overwrite scan output: {out}')
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(rendered+'\n', encoding='utf-8')
    return 0


def main():
    ap=argparse.ArgumentParser(description='Kestrel DROP D3 offline packet decoder / experimental live BLE reader')
    sub=ap.add_subparsers(dest='cmd',required=True)
    sc=sub.add_parser('scan',help='BLE discovery only; no GATT connection or writes')
    sc.add_argument('--timeout',type=float,default=20)
    sc.add_argument('--name',default='',help='Optional name substring filter. Empty-name DROP service candidates are also shown.')
    sc.add_argument('--output',help='Optional JSON output file, never overwritten')
    off=sub.add_parser('decode',help='OFFLINE: extract/verify captured history from PacketLogger .pklg')
    off.add_argument('--pklg',required=True)
    off.add_argument('--reference',help='Official Kestrel LiNK export CSV for validation')
    off.add_argument('--output',default='decoded_output')
    off.add_argument('--connection',type=int,help='Optional HCI connection handle (the analyzed capture uses 79)')
    live=sub.add_parser('live',help='EXPERIMENTAL: replay observed read workflow on physical D3')
    probe=sub.add_parser('probe',help='EXPERIMENTAL: query available log-record count from a cursor (no log download)')
    for p in (live,probe):
        p.add_argument('--ble-id',required=True,help='Mac CoreBluetooth device UUID (may change across Macs)')
        p.add_argument('--serial',required=True,help='Expected D3 serial, verified BEFORE writing anything')
        p.add_argument('--scan-timeout',type=float,default=20,
                       help='Seconds to scan for device immediately before connecting (default: 20)')
        p.add_argument('--cursor',type=int,required=True,help='Explicit absolute log cursor (not a record count)')
        p.add_argument('--maxrows',type=int,default=6220)
        p.add_argument('--execute',action='store_true',help='Opt in to sending app-level GATT commands; NOT fully tested')
    live.add_argument('--output',default='live_history.csv')
    adopt=sub.add_parser('adopt',help='Initialize incremental sync from a verified full D3 CSV + its .csv.json metadata')
    adopt.add_argument('--output',required=True,help='Existing full CSV to keep and append to')
    adopt.add_argument('--metadata',required=True,help='Existing verified live export .csv.json')
    status=sub.add_parser('status',help='Show committed records, cursor and any durable pending records')
    status.add_argument('--output',required=True)
    inc=sub.add_parser('sync',help='Download only new records; stream every valid block into .pending.csv; commit atomically')
    inc.add_argument('--output',required=True,help='Existing adopted full CSV')
    inc.add_argument('--ble-id',help='Optional Mac BLE identifier; if omitted, scan by advertised D3 serial')
    inc.add_argument('--serial',required=True,help='Expected D3 serial')
    inc.add_argument('--scan-timeout',type=float,default=20)
    inc.add_argument('--maxrows',type=int,default=6220)
    inc.add_argument('--execute',action='store_true')
    args=ap.parse_args()
    try:
        if args.cmd=='adopt':
            state=adopt_archive(Path(args.output),Path(args.metadata))
            print(f'Adopted {state["row_count"]:,} D3 records; next absolute cursor {state["next_cursor"]:,}.')
            return 0
        if args.cmd=='status':return archive_status(Path(args.output))
        if args.cmd=='sync':
            if args.scan_timeout<=0 or args.maxrows<1:raise ValueError('Invalid --scan-timeout/--maxrows')
            return asyncio.run(sync_live(args))
        if args.cmd=='decode':return decode_capture(args)
        if args.cmd=='scan':
            if args.timeout<=0:raise ValueError('Timeout must be positive')
            return asyncio.run(scan_ble(args))
        if args.cursor<0 or args.maxrows<1 or args.scan_timeout<=0:raise ValueError('Invalid cursor/maxrows/scan-timeout')
        return asyncio.run(live_read(args))
    except (ValueError,RuntimeError,FileNotFoundError,FileExistsError,OSError) as exc:
        print('ERROR:',exc,file=sys.stderr)
        return 2


if __name__=='__main__':
    raise SystemExit(main())
