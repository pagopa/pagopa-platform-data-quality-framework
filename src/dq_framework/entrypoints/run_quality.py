"""
Entrypoint CDE per il job di Data Quality.

Invocazione tipica su CDE:
    spark-submit launcher.py \
      --contract-path src/data/pagopa/gpd/silver/dc-gpd-payment_option.yaml \
      --repository carlomanco-qty/qty-data-contracts \
      --ref main

Ognuno dei tre argomenti è opzionale: se omesso viene usato il valore di default
definito in AppConfig per l'ambiente corrente (ENV).

Invocazione da Airflow DAG:
    spark-submit dq_framework.whl --entrypoint run_quality \
      --contract-path /path/to/contracts/gpd/silver_table.yaml
      --repository carlomanco-qty/qty-data-contracts \
      --ref main
"""

from __future__ import annotations

import argparse
import sys

from dq_framework.common.config import load_config
from dq_framework.common.logging import setup_logging
from dq_framework.quality.engine import run_pipeline


def _parse_args(
    default_contract_path: str,
    default_repository: str,
    default_ref: str,
    argv: list[str] | None = None,
) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Data Quality pipeline — GPD Silver Layer")
    parser.add_argument(
        "--contract-path",
        default=default_contract_path,
        help=(
            "Path del Data Contract: repo-relativo (es. 'src/data/.../x.yaml') "
            "oppure path locale. Default: valore configurato per l'ambiente."
        ),
    )
    parser.add_argument(
        "--repository",
        default=default_repository,
        help=(
            "Repository GitHub nella forma 'owner/repo'. "
            "Se vuoto, --contract-path è trattato come path locale. "
            "Default: valore configurato per l'ambiente."
        ),
    )
    parser.add_argument(
        "--ref",
        default=default_ref,
        help="Branch, tag o commit del repository. Default: valore configurato per l'ambiente.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    setup_logging()
    config = load_config()
    args = _parse_args(
        default_contract_path=config.contract_path,
        default_repository=config.default_repository,
        default_ref=config.default_ref,
        argv=argv,
    )
    run_pipeline(
        contract_path=args.contract_path,
        repository=args.repository,
        ref=args.ref,
        config=config,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
