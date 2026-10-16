"""Compatibility entry point: `python haveyoursay.py ...` from a source checkout.

The installed `haveyoursay` command (pip install .) is the preferred way to
run the tool. This file adds src/ to the path and calls the same CLI. When
Python imports this file instead of the package (because the repository root
is on sys.path), it hands over to the real package so that
`import haveyoursay` still yields the package.
"""

import importlib
import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent / "src")
sys.path[:] = [_SRC] + [p for p in sys.path if p != _SRC]

if __name__ == "__main__":
    from haveyoursay.cli import main

    sys.exit(main())
else:
    del sys.modules[__name__]
    sys.modules[__name__] = importlib.import_module("haveyoursay")
