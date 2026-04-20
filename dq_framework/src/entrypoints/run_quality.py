"""
Entrypoint CDE per il job di Data Quality.

Invocazione da Airflow DAG:
    spark-submit dq_framework.whl --entrypoint run_quality \
      --contract-path /path/to/contracts/gpd/silver_table.yaml

Se --contract-path è omesso, viene usato il valore di default definito
in AppConfig per l'ambiente corrente (ENV).
"""

from __future__ import annotations

import argparse
import sys

from dq_framework.src.common.config import load_config
from dq_framework.src.common.logging import setup_logging
from dq_framework.src.quality.engine import run_pipeline


def _parse_args(default_contract_path: str, argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Data Quality pipeline — GPD Silver Layer")
    parser.add_argument(
        "--contract-path",
        default=default_contract_path,
        help=(
            "Percorso assoluto/relativo o URL github.com/blob/ del Data Contract. "
            "Se omesso, usa il valore configurato per l'ambiente corrente."
        ),
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    config = load_config()
    args = _parse_args(default_contract_path=config.contract_path, argv=argv)
    run_pipeline(args.contract_path, config)


if __name__ == "__main__":
    main(sys.argv[1:])
