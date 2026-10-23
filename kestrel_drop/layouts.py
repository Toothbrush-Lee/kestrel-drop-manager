"""Exact, captured DROP history layouts; reject unknown firmware schemas.

Field triples (id, representation, byte count) were observed in device schema
frames. D1/D2 samples captured 2026-10-23; D3 cross-checked against LiNK CSV.
"""
from dataclasses import dataclass
from datetime import datetime

from .devices import model_type

MEASUREMENTS = ('temperature_c', 'relative_humidity_pct', 'station_pressure_hpa',
                'heat_index_c', 'dew_point_c', 'density_altitude_m_raw',
                'density_altitude_m_official_integer_estimate')
FIELDS = ('timestamp_device_local', *MEASUREMENTS, 'unknown_byte', 'block', 'position_in_block', 'record_hex')


@dataclass(frozen=True)
class Layout:
    kind: str
    record_bytes: int
    schema_hex: str

    @property
    def schema(self): return bytes.fromhex(self.schema_hex)

    @property
    def decoder(self): return f'{self.kind.lower()}-{self.record_bytes}-v1'

    def decode(self, raw, *, block, position):
        if len(raw) != self.record_bytes:
            raise ValueError(f'{self.kind}: Expected {self.record_bytes} bytes, got {len(raw)}')
        second, minute, hour, day, month = raw[:5]
        stamp = datetime(int.from_bytes(raw[5:7], 'little'), month, day, hour, minute, second)
        row = dict.fromkeys(FIELDS)
        row.update(timestamp_device_local=stamp.isoformat(sep=' '),
                   temperature_c=int.from_bytes(raw[7:9], 'little', signed=True)/100,
                   block=block, position_in_block=position, record_hex=raw.hex())
        if self.kind in ('D2','D3'):
            row.update(relative_humidity_pct=int.from_bytes(raw[9:11], 'little')/100,
                       heat_index_c=int.from_bytes(raw[11:13], 'little', signed=True)/100,
                       unknown_byte=raw[13], dew_point_c=int.from_bytes(raw[14:16], 'little', signed=True)/100)
        if self.kind == 'D3':
            value=int.from_bytes(raw[18:21], 'little', signed=True)
            row.update(station_pressure_hpa=int.from_bytes(raw[16:18], 'little')/10,
                       density_altitude_m_raw=value/10,
                       density_altitude_m_official_integer_estimate=value//10)
        return row


PROFILES = {
    'D1': Layout('D1',9,'00002400070002001c000200'),
    'D2': Layout('D2',16,'00002400070002001c00020005001b0002000a003e0003000b001c000200'),
    'D3': Layout('D3',21,'00002400070002001c00020005001b0002000a003e0003000b001c00020008001f0002000e003c000300'),
}


def profile(model='D3'):
    kind=model_type(model)
    if kind not in PROFILES: raise ValueError(f'Unverified history model: {model!r}')
    return PROFILES[kind]
