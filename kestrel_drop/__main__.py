"""Allow `python -m kestrel_drop` from the project directory."""
from .manager import main

if __name__ == '__main__':
    raise SystemExit(main())
