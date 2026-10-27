"""Frozen entry point: validate platform backend imports without starting BLE."""
import sys

if sys.platform == 'darwin':
    import bleak.backends.corebluetooth.client
    import bleak.backends.corebluetooth.scanner
else:
    import bleak.backends.bluezdbus.client
    import bleak.backends.bluezdbus.scanner

from kestrel_drop.manager import main

if __name__ == '__main__':
    raise SystemExit(main())
