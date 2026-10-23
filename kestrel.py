"""Run `uv run kestrel.py` for the TUI; use --help for scripting."""
from kestrel_drop.manager import main

if __name__ == '__main__':
    raise SystemExit(main())
