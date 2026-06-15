"""Test di _resolve_ts_literal_kind (introspezione del tipo colonna watermark).

Verifica che la keyword del literal timestamp venga allineata al tipo Spark
della colonna watermark, con fallback difensivo a "TIMESTAMP":
    - colonna TIMESTAMP_NTZ  -> "TIMESTAMP_NTZ"
    - colonna TIMESTAMP (LTZ) -> "TIMESTAMP"
    - tipo inatteso           -> "TIMESTAMP" (fallback)
    - errore di risoluzione   -> "TIMESTAMP" (fallback)
    - spark assente (test)    -> "TIMESTAMP" senza toccare il catalogo
"""
from __future__ import annotations

from unittest.mock import MagicMock

from dq_framework.quality.utils.incremental import _resolve_ts_literal_kind


def _spark_returning(simple_string: str) -> MagicMock:
    """SparkSession fittizia il cui schema[col].dataType.simpleString() e' fisso."""
    spark = MagicMock()
    (spark.table.return_value
        .schema.__getitem__.return_value
        .dataType.simpleString.return_value) = simple_string
    return spark


def test_colonna_ntz_produce_timestamp_ntz():
    spark = _spark_returning("timestamp_ntz")
    assert _resolve_ts_literal_kind(spark, "pagopa.silver_t", "dl_event_tms") == "TIMESTAMP_NTZ"


def test_colonna_ltz_produce_timestamp():
    spark = _spark_returning("timestamp")
    assert _resolve_ts_literal_kind(spark, "pagopa.silver_t", "dl_event_tms") == "TIMESTAMP"


def test_tipo_inatteso_fallback_timestamp():
    """Un tipo non-timestamp (config errata) non deve far esplodere: fallback."""
    spark = _spark_returning("bigint")
    assert _resolve_ts_literal_kind(spark, "pagopa.silver_t", "dl_event_tms") == "TIMESTAMP"


def test_errore_risoluzione_fallback_timestamp():
    """Se spark.table esplode (tabella/colonna assente) -> fallback difensivo."""
    spark = MagicMock()
    spark.table.side_effect = RuntimeError("table not found")
    assert _resolve_ts_literal_kind(spark, "pagopa.silver_t", "dl_event_tms") == "TIMESTAMP"


def test_spark_none_non_tocca_il_catalogo():
    """spark=None (contesto unit test) -> "TIMESTAMP" senza alcuna chiamata."""
    assert _resolve_ts_literal_kind(None, "pagopa.silver_t", "dl_event_tms") == "TIMESTAMP"


def test_il_dataset_qualificato_viene_quotato_con_backtick():
    """Il dataset multi-parte deve essere passato a spark.table con i backtick
    (coerente con come soda_executor carica la tabella principale)."""
    spark = _spark_returning("timestamp_ntz")
    _resolve_ts_literal_kind(spark, "pagopa.silver_gpd_transfer", "dl_event_tms")
    spark.table.assert_called_once_with("`pagopa`.`silver_gpd_transfer`")
