# /// script
# requires-python = ">=3.11"
# dependencies = ["bleak>=0.22,<4"]
# ///
"""Advanced protocol CLI; normal downloads use the root kestrel.py launcher."""
from pathlib import Path
import sys

# Resolve the package from the checkout even when invoked from another folder.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from kestrel_drop.protocol import main

if __name__ == '__main__':
    raise SystemExit(main())
