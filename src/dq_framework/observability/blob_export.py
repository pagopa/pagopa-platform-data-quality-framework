"""Export degli esiti DQF verso Azure Blob Storage, da cui li legge Superset.

Superset gira su Azure e non puo' interrogare Cloudera: a fine run il framework
copia sul container Blob lo storico del dataset appena controllato, in Parquet,
un file per dataset:

    <container>/results/dl_layer=<dl_layer>/domain=<domain>/<dataset>.parquet
    <container>/failed_records/dl_layer=<dl_layer>/domain=<domain>/<dataset>.parquet

Ogni export riscrive il file con TUTTO lo storico del dataset, non solo l'ultima
run: rerun e retry non creano duplicati e le cancellazioni della retention si
riflettono anche su Azure. L'upload di un singolo blob e' atomico, quindi
Superset legge sempre una versione integra, al piu' non aggiornata.

E' un canale secondario, come Soda Cloud: un errore viene loggato e il run di
quality prosegue. Farlo fallire sarebbe peggio del problema: il retry del task
Airflow rieseguirebbe l'intera pipeline, accodando su Iceberg una seconda copia
degli esiti. La run successiva riesporta comunque tutto lo storico.

failed_records contiene valori reali dei record (PII): il suo export ha un flag
dedicato, spento di default, da accendere solo con l'ok del cliente.
"""

from __future__ import annotations

import io
import logging
import re
import time
import urllib.error
import urllib.request
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import StructType

from dq_framework.common import secrets
from dq_framework.common.config import AppConfig

logger = logging.getLogger(__name__)

# Tipi Spark -> Arrow. Lo schema esplicito evita colonne di tipo "null" quando
# sono tutte vuote e produce un file con le colonne giuste anche a zero righe.
_SPARK_TO_ARROW = {
    "string":        pa.string(),
    "int":           pa.int32(),
    "bigint":        pa.int64(),
    "double":        pa.float64(),
    "boolean":       pa.bool_(),
    "date":          pa.date32(),
    "timestamp":     pa.timestamp("us"),
    "timestamp_ntz": pa.timestamp("us"),
}

# Versione delle API Blob: coincide con il campo `sv` del SAS token in uso.
_BLOB_API_VERSION = "2018-11-09"
_UPLOAD_TIMEOUT_SECONDS = 120
# Attese tra un tentativo e l'altro: 3 tentativi in tutto, solo su errori temporanei.
_RETRY_WAITS_SECONDS = (5, 15)

# Causa piu' probabile dei codici d'errore Azure visti in pratica.
_AZURE_ERROR_HINTS = {
    "AuthorizationFailure": "firewall dello storage account: l'IP di uscita di CDP non e' ammesso",
    "AuthenticationFailed": "SAS token non valido o scaduto",
    "AuthorizationPermissionMismatch": "il SAS token non ha i permessi di scrittura",
}


def export_to_blob(
    spark: SparkSession,
    config: AppConfig,
    domain: str,
    dl_layer: str,
    datasets: list[str],
    include_failed_records: bool = False,
) -> None:
    """Esporta su Azure Blob lo storico completo dei `datasets` del run.

    Non solleva mai: ogni errore viene loggato e il run di quality prosegue.
    `include_failed_records` aggiunge la tabella failed_records, che contiene
    valori reali dei record: il default e' False.
    """
    if not config.results_write_enabled:
        logger.info(
            f"results_write_enabled=False (env={config.env}): export verso Azure Blob saltato, "
            f"le tabelle Iceberg non sono state scritte."
        )
        return
    if not config.blob_export_account:
        logger.error(
            f"Export verso Azure Blob attivo ma nessuno storage account configurato per env={config.env} "
            f"(AppConfig.blob_export_account): export saltato."
        )
        return

    # Il SAS token viene letto solo qui, cioe' solo con l'export attivo.
    sas_token = secrets.blob_export_sas_token()
    if not sas_token:
        logger.error(
            "Export verso Azure Blob attivo ma SAS token mancante (workload credential "
            "'dqf-blob-sas', chiave 'sas_token', o env DQF_BLOB_SAS_TOKEN): export saltato."
        )
        return

    tables = ["results", "failed_records"] if include_failed_records else ["results"]
    for table in tables:
        fqn = f"{config.results_database}.{dl_layer}_dqf_{domain}_{table}"
        for dataset in datasets:
            blob_name = f"{table}/dl_layer={dl_layer}/domain={domain}/{dataset}.parquet"
            try:
                table_df = spark.table(fqn)
                n_rows, data = dataframe_to_parquet(table_df.where(table_df["dataset"] == dataset))
                status = upload_blob(
                    config.blob_export_account, config.blob_export_container, blob_name, data, sas_token
                )
                logger.info(
                    f"Export Azure Blob {fqn} [{dataset}]: {n_rows} righe, {len(data)} B -> "
                    f"{config.blob_export_container}/{blob_name} (HTTP {status})"
                )
            except urllib.error.HTTPError as e:
                logger.error(f"Export Azure Blob rifiutato per {fqn} [{dataset}]: {_describe_http_error(e)}")
            except Exception as e:
                logger.error(f"Export Azure Blob fallito per {fqn} [{dataset}]: {e}", exc_info=True)


def dataframe_to_parquet(df) -> tuple[int, bytes]:
    """Porta il DataFrame sul driver e lo serializza in Parquet in memoria.

    Il collect e' sostenibile perche' gli esiti di un dataset sono poche migliaia
    di righe al massimo, anche con tutto lo storico.
    """
    return rows_to_parquet(df.collect(), df.schema)


def rows_to_parquet(rows: list[Row], spark_schema: StructType) -> tuple[int, bytes]:
    """Serializza le righe in Parquet (snappy) con lo schema Arrow derivato da quello Spark."""
    fields = []
    for f in spark_schema.fields:
        spark_type = f.dataType.simpleString()
        if spark_type not in _SPARK_TO_ARROW:
            raise TypeError(
                f"Tipo Spark non supportato per l'export Parquet: colonna {f.name!r} ({spark_type})"
            )
        fields.append((f.name, _SPARK_TO_ARROW[spark_type]))

    table = pa.Table.from_pylist([r.asDict() for r in rows], schema=pa.schema(fields))
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return table.num_rows, buf.getvalue()


def upload_blob(account: str, container: str, blob_name: str, data: bytes, sas_token: str) -> int:
    """Carica `data` come block blob, sovrascrivendo. Restituisce lo status HTTP.

    Riprova solo sugli errori temporanei (rete, timeout, 5xx, 429): un 403 o un
    404 non cambiano riprovando, quindi falliscono subito.
    """
    url = f"https://{account}.blob.core.windows.net/{container}/{blob_name}?{sas_token.lstrip('?')}"
    request = urllib.request.Request(
        url,
        data=data,
        method="PUT",
        headers={
            "x-ms-version": _BLOB_API_VERSION,
            "x-ms-blob-type": "BlockBlob",
            "Content-Type": "application/octet-stream",
        },
    )

    waits: list[Optional[int]] = [*_RETRY_WAITS_SECONDS, None]
    for attempt, wait in enumerate(waits, start=1):
        try:
            with urllib.request.urlopen(request, timeout=_UPLOAD_TIMEOUT_SECONDS) as response:
                return response.status
        except urllib.error.HTTPError as e:
            if wait is None or not (e.code >= 500 or e.code == 429):
                raise
            reason = f"HTTP {e.code}"
        except (urllib.error.URLError, TimeoutError) as e:
            if wait is None:
                raise
            reason = f"{type(e).__name__}: {e}"
        logger.warning(f"Upload {blob_name}: tentativo {attempt} fallito ({reason}), nuovo tentativo tra {wait}s")
        time.sleep(wait)

    raise AssertionError("irraggiungibile: l'ultimo tentativo restituisce o solleva")


def _describe_http_error(error: urllib.error.HTTPError) -> str:
    """Status HTTP + codice d'errore Azure + causa probabile, se nota."""
    body = error.read().decode(errors="replace")
    match = re.search(r"<Code>(.*?)</Code>", body)
    code = match.group(1) if match else ""
    hint = _AZURE_ERROR_HINTS.get(code)
    return " - ".join(part for part in (f"HTTP {error.code}", code, hint) if part)
