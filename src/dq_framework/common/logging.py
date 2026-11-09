from __future__ import annotations

import logging
import os
import sys


def setup_logging(level: int | None = None) -> None:
    """Configura il logging in modo uniforme per esecuzione locale e CDE.

    Se `level` non e' passato si legge l'env `LOG_LEVEL` (default INFO), cosi'
    da poter abilitare i `logger.debug` senza toccare il codice. `force=True`
    sovrascrive eventuali handler gia' registrati (es. da import precedenti).
    """
    if level is None:
        resolved = logging.getLevelName(os.getenv("LOG_LEVEL", "INFO").upper())
        level = resolved if isinstance(resolved, int) else logging.INFO

    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)-8s | %(module)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )
