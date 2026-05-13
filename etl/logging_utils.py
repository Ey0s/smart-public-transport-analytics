from __future__ import annotations

import logging
import os
from typing import Optional


def setup_logging(level: Optional[str] = None) -> None:
    """
    Configure application logging once.

    Priority:
      1) explicit `level`
      2) env var `LOG_LEVEL`
      3) INFO
    """

    log_level = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    numeric = getattr(logging, log_level, logging.INFO)

    root = logging.getLogger()
    if root.handlers:
        root.setLevel(numeric)
        return

    logging.basicConfig(
        level=numeric,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

