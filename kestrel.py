# /// script
# requires-python = ">=3.11"
# dependencies = ["bleak>=0.22,<4"]
# ///
"""Run `uv run kestrel.py` for the TUI; use --help for scripting."""
from kestrel_drop.manager import main

if __name__ == '__main__':
    raise SystemExit(main())
