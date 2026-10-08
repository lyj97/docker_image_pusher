"""h3 local inference worker (v1).

Implements service/client/README.md against service/protocol.md.
"""

from __future__ import annotations

import os
import sys

__version__ = "0.2.0"

_SERVICE_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if _SERVICE_ROOT not in sys.path:
    sys.path.insert(0, _SERVICE_ROOT)
