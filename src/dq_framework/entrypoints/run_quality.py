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
import os
import sys
from datetime import datetime

from dq_framework.common.config import load_config
from dq_framework.common.logging import setup_logging
from dq_framework.quality.engine import run_pipeline


def _parse_iso_datetime(value: str) -> datetime:
    """Parser argparse per --watermark-from. Accetta ISO 8601 con o senza microsecondi.

    Esempi validi:
        2026-05-24
        2026-05-24T03:00:00
        2026-05-24 03:00:00.123456
    """
    try:
        # datetime.fromisoformat copre la maggior parte dei formati ISO 8601
        # (Python >= 3.7). Se la stringa contiene 'Z' come suffisso UTC,
        # lo normalizziamo a '+00:00' per compatibilita'.
        normalized = value.replace("Z", "+00:00")
        return datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"--watermark-from non e' un timestamp ISO 8601 valido: {value!r}. "
            f"Esempi: '2026-05-24', '2026-05-24T03:00:00'."
        ) from exc


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
    parser.add_argument(
        "--dag-id",
        default=os.environ.get("AIRFLOW_CTX_DAG_ID"),
        help=(
            "Identificativo del DAG Airflow. "
            "Se omesso usa la variabile d'ambiente AIRFLOW_CTX_DAG_ID."
        ),
    )
    parser.add_argument(
        "--airflow-run-id",
        default=os.environ.get("AIRFLOW_CTX_DAG_RUN_ID"),
        help=(
            "Identificativo del DAG run Airflow. "
            "Se omesso usa la variabile d'ambiente AIRFLOW_CTX_DAG_RUN_ID."
        ),
    )
    parser.add_argument(
        "--watermark-column",
        default=None,
        help=(
            "Nome della colonna usata per espandere il placeholder "
            "${INCREMENTAL_CONDITIONS} nelle query SodaCL custom. "
            "Override di AppConfig.default_watermark_column. "
            "Se assente e default non configurato, il framework solleva "
            "errore se nel SodaCL e' presente il placeholder. "
            "Per query con JOIN usare ${INCREMENTAL_CONDITIONS:<alias>} per "
            "qualificare la colonna con la tabella driving (es. :spo)."
        ),
    )
    parser.add_argument(
        "--watermark-from",
        type=_parse_iso_datetime,
        default=None,
        help=(
            "Override esplicito di watermark_from in formato ISO 8601 "
            "(es. '2026-05-24T03:00:00'). Bypassa il lookup automatico "
            "sulla tabella results: viene applicato a TUTTI i check "
            "incrementali del run."
        ),
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
        contract_path             = args.contract_path,
        repository                = args.repository,
        ref                       = args.ref,
        config                    = config,
        dag_id                    = args.dag_id,
        airflow_run_id            = args.airflow_run_id,
        watermark_column_override = args.watermark_column,
        watermark_from_override   = args.watermark_from,
    )


if __name__ == "__main__":
    main(sys.argv[1:])
