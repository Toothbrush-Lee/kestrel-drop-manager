"""Run the suite with isolated default data/settings locations.

Use `uv run --locked python -B tests/run.py` from any working directory.
Explicit per-test temporary directories continue to override these defaults.
"""
import argparse
import os
from pathlib import Path
import sys
import tempfile
import unittest


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-q','--quiet',action='store_true')
    args=parser.parse_args()
    root=Path(__file__).resolve().parents[1]
    sys.path.insert(0,str(root))
    with tempfile.TemporaryDirectory(prefix='kestrel-test-defaults-') as temporary:
        os.environ['KESTREL_DATA_DIR']=str(Path(temporary)/'data')
        os.environ['KESTREL_CONFIG_FILE']=str(Path(temporary)/'settings.json')
        suite=unittest.defaultTestLoader.discover(str(root/'tests'),pattern='test_*.py')
        result=unittest.TextTestRunner(verbosity=0 if args.quiet else 2).run(suite)
        return 0 if result.wasSuccessful() else 1


if __name__=='__main__':
    raise SystemExit(main())
