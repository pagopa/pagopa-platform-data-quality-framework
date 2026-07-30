#!/usr/bin/env bash
# =============================================================================
# Deploy del Data Quality framework su Cloudera Data Engineering.
# -----------------------------------------------------------------------------
# Parametrizzato per ambiente: i nomi della resource, del job e del Python env
# su CDE vengono derivati dall'ENV passata da CLI secondo il pattern:
#     RESOURCE = dq-framework-<env>
#     JOB      = dq-quality-<env>
#     PYENV    = dq-framework-pyenv-<env>
#
# La pipeline a runtime legge `ENV` da os.environ (vedi common/config/__init__.py)
# e seleziona dall'AppConfig registry il blocco corrispondente. La variabile
# d'ambiente viene iniettata nel driver/executor Spark via spark.kubernetes.
#
# USO:
#     ./scripts/deploy.sh <ENV> <CDE_VCLUSTER_ENDPOINT> <CDE_CONFIG_PROFILE> [--run]
#
# ESEMPI:
#     # Deploy ambiente test (no run)
#     ./scripts/deploy.sh test \
#         https://<vcluster>.cloudera.site/dex/api/v1 \
#         default
#
#     # Deploy + lancio immediato su prod
#     ./scripts/deploy.sh prod \
#         https://<vcluster>.cloudera.site/dex/api/v1 \
#         default \
#         --run
#
# ENV SUPPORTATI:
#     test       - deploy/run su vcluster CDE (TEST_CONFIG)
#     dev-github - deploy/run su vcluster CDE leggendo contract da GitHub
#     prod       - deploy/run produzione (PROD_CONFIG)
#     (env=dev non e' deployabile: gira solo in locale dentro al Dev Container)
#
# PREREQUISITI (una tantum):
#     1. `cde` CLI installato e autenticato
#     2. Python env resource dq-framework-pyenv-<env> gia' creato su CDE
#     3. Workload credential `github-token` configurata sul vcluster
#     4. Pacchetto Python `build` installato in locale (`pip install build`)
#     5. Migrazione DDL applicata sulla tabella results del'ambiente
#        (script: migrations/001_add_watermark_columns.sql)
# =============================================================================

set -euo pipefail

# ----- Argomenti di linea ----------------------------------------------------
if [[ $# -lt 3 ]]; then
    echo "ERRORE: argomenti mancanti."
    echo ""
    echo "Uso: $0 <ENV> <CDE_VCLUSTER_ENDPOINT> <CDE_CONFIG_PROFILE> [--run]"
    echo ""
    echo "Esempi:"
    echo "    $0 test https://<vcluster>.cloudera.site/dex/api/v1 default"
    echo "    $0 prod https://<vcluster>.cloudera.site/dex/api/v1 default --run"
    echo ""
    echo "ENV supportati: test | dev-github | prod"
    exit 1
fi

ENV_NAME="$1"
export CDE_VCLUSTER_ENDPOINT="$2"
export CDE_CONFIG_PROFILE="$3"
RUN_AFTER_DEPLOY="false"
if [[ "${4:-}" == "--run" ]]; then
    RUN_AFTER_DEPLOY="true"
fi

# ----- Validazione ENV -------------------------------------------------------
case "$ENV_NAME" in
    test|dev-github|prod)
        ;;
    dev)
        echo "ERRORE: ENV='dev' non e' deployabile su CDE (gira solo in locale)."
        echo "Per il deploy usa: test | dev-github | prod"
        exit 1
        ;;
    *)
        echo "ERRORE: ENV='$ENV_NAME' non riconosciuta."
        echo "Valori validi: test | dev-github | prod"
        exit 1
        ;;
esac

# ----- Naming derivato dall'ENV ----------------------------------------------
RESOURCE="dq-framework-${ENV_NAME}"
JOB="dq-quality-${ENV_NAME}"
PYENV="dq-framework-pyenv-${ENV_NAME}"

# ----- Argomenti del job (entrypoint run_quality) ----------------------------
# Vengono scritti nella definizione del job su CDE, quindi valgono per OGNI run
# e sovrascrivono i default di AppConfig (common/config/<env>.py). Tenerli qui e
# non solo nella UI di CDE e' necessario perche' `cde job update` riscrive la
# spec del job: argomenti impostati a mano dalla UI verrebbero persi al deploy
# successivo. Il DAG puo' comunque sovrascriverli per singolo run.
JOB_ARGS=(
    --arg "--domain=gpd"
    --arg "--table-scope=silver"
    --arg "--contract-path=src/data/pagopa/gpd/silver/transfer.yaml"
    --arg "--repository=carlomanco-qty/qty-data-contracts"
    --arg "--ref=main"
    # BOOTSTRAP: forza wm_from su TUTTI i check incrementali bypassando il lookup
    # su Iceberg. Serve solo al primo run di un ambiente, dove la tabella results
    # non esiste ancora e il lookup farebbe fail-fast. RIMUOVERE dopo il primo run
    # verde: se resta, ogni run riparte dal 2026-07-01 e la finestra incrementale
    # cresce senza limite invece di avanzare.
    --arg "--watermark-from=2026-07-01T13:00:00"
)

# ----- Sanity check tooling ---------------------------------------------------
command -v cde     >/dev/null 2>&1 || { echo "ERRORE: 'cde' CLI non trovata in PATH"; exit 1; }
command -v python3 >/dev/null 2>&1 || { echo "ERRORE: 'python3' non trovato"; exit 1; }
python3 -c "import build" >/dev/null 2>&1 || {
    echo "ERRORE: modulo Python 'build' non installato. Esegui: pip install build"
    exit 1
}

echo "================================================================="
echo "  Deploy dq-framework -> CDE"
echo "================================================================="
echo "  ENV       : $ENV_NAME"
echo "  Endpoint  : $CDE_VCLUSTER_ENDPOINT"
echo "  Profile   : $CDE_CONFIG_PROFILE"
echo "  Resource  : $RESOURCE"
echo "  Job       : $JOB"
echo "  Pyenv     : $PYENV"
echo "  Run dopo  : $RUN_AFTER_DEPLOY"
echo "================================================================="

# ----- Build wheel ------------------------------------------------------------
echo ""
echo "[1/4] Build wheel..."
rm -rf dist/ build/ *.egg-info
python3 -m build --wheel

WHL=$(ls dist/*.whl | head -n1)
if [[ -z "$WHL" ]]; then
    echo "ERRORE: nessuna wheel prodotta sotto dist/"
    exit 1
fi
WHL_NAME=$(basename "$WHL")
echo "       -> $WHL_NAME"

# ----- Resource su CDE (idempotente) -----------------------------------------
echo ""
echo "[2/4] Upload risorse su CDE..."
cde resource create --name "$RESOURCE" --type files 2>/dev/null || true
cde resource upload --name "$RESOURCE" --local-path "$WHL"
cde resource upload --name "$RESOURCE" --local-path launcher.py
echo "       -> wheel e launcher.py caricati su resource '$RESOURCE'"

# ----- Job create/update ------------------------------------------------------
echo ""
echo "[3/4] Create/update job '$JOB'..."
if cde job describe --name "$JOB" >/dev/null 2>&1; then
    echo "       -> il job esiste gia', UPDATE in corso"
    ACTION_ARGS=(update)
else
    echo "       -> il job non esiste, CREATE in corso"
    ACTION_ARGS=(create --type spark)
fi

cde job "${ACTION_ARGS[@]}" \
    --name "$JOB" \
    --application-file launcher.py \
    --py-file "$WHL_NAME" \
    --mount-1-resource "$RESOURCE" \
    --python-env-resource-name "$PYENV" \
    --workload-credential github-token \
    --conf "spark.kubernetes.driverEnv.ENV=${ENV_NAME}" \
    --conf "spark.executorEnv.ENV=${ENV_NAME}" \
    "${JOB_ARGS[@]}"

echo "       -> job '$JOB' pronto (ENV=$ENV_NAME)"

# ----- Run opzionale ----------------------------------------------------------
echo ""
if [[ "$RUN_AFTER_DEPLOY" == "true" ]]; then
    echo "[4/4] Lancio del job..."
    cde job run --name "$JOB"
    echo ""
    echo "Per recuperare l'id dell'ultima run:"
    echo "    cde run list --filter 'job[eq]${JOB}'"
    echo ""
    echo "Per seguire i log driver (sostituisci <RUN_ID>):"
    echo "    cde run logs --id <RUN_ID> --type driver --follow"
else
    echo "[4/4] Skip run (passare --run per lanciare il job subito dopo il deploy)."
    echo ""
    echo "Per lanciarlo manualmente:"
    echo "    cde job run --name $JOB"
fi

echo ""
echo "================================================================="
echo "  Deploy completato (ENV=$ENV_NAME)"
echo "================================================================="
