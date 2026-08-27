# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Data Quality framework for the PagoPa CDP platform. Reads **Data Contract** YAML files (locally or from GitHub) whose quality rules are written directly as **SodaCL** in the `quality.specification` block and read as-is — `datacontract-cli` was removed (commit `863ab54`) and there is no conversion step. It then runs the Soda scan against a Spark DataFrame and writes structured results to two Iceberg tables (a granular results log and a failed-records detail table), optionally sending aggregate metrics only to Soda Cloud.

The deliverable is a wheel deployed to **CDE** (Cloudera Data Engineering) and invoked via `spark-submit`, typically orchestrated by Airflow.

## Common commands

Development happens inside the Dev Container (`.devcontainer/`), which installs the package editable. Inside the container:

```bash
make lint              # flake8 .
make run-dev           # generate mock data + run pipeline with ENV=dev (local contract)
make run-dev-github    # same, with ENV=dev-github (downloads contract from GitHub)
pytest                 # tests (testpaths = tests/)
pytest tests/unit/test_x.py::test_name   # single test
```

The `.env` file at repo root is loaded by the Makefile (`include .env; export`) and must define `GITHUB_TOKEN` (for `dev-github`/`prod`) and optionally `SODA_HOST`/`SODA_API_KEY`/`SODA_API_SECRET`. Without Soda Cloud credentials the scan still runs and logs locally.

Manual entrypoint invocation (used on CDE via `spark-submit launcher.py ...`):

```bash
ENV=dev python -m dq_framework.entrypoints.run_quality \
  --domain gpd --dl-layer silver \
  --contract-path src/data/.../dc-foo.yaml --repository owner/repo --ref main
```

`--domain` and `--dl-layer` are both **required**: together they name the output
tables (`{dl_layer}_dqf_{domain}_results` / `_failed_records`). Both are validated
as plain SQL identifiers (`^[a-z][a-z0-9_]*$`, lowercased) because they are
interpolated straight into the FQN and the `LOCATION` clause.

Note: the CI lint gate is `ruff check .` (`.github/workflows/dq_framework_ci.yml`, `line-length = 120` from `pyproject.toml`); CI does **not** run pytest. The Makefile `make lint` target still calls `flake8`, which is *not* what CI enforces — lint with `ruff check .` to match CI.

## Architecture

**Source layout is `src/`-based** (`src/dq_framework/...`), despite the README's example tree showing `dq_framework/src/...` — trust the filesystem. Two `spark-submit` entrypoints are exposed via `[project.scripts]`:

- `run_quality` → `dq_framework.entrypoints.run_quality:main`
- `run_observability` → `dq_framework.entrypoints.run_observability:main` (in development)

### Environment-driven config

`dq_framework.common.config.load_config()` reads `ENV` (default `dev`) and returns a frozen `AppConfig` dataclass picked from a registry: `dev` (local contract + local data), `dev-github` (contract fetched from GitHub API), `prod`. Each is defined in its own file under `common/config/`. `AppConfig` controls: default `contract_path`/`repository`/`ref`, Soda host & `data_source`, `table_limit`, and the Iceberg results sink (`results_database`, `results_table_location`, `results_write_enabled`). The *names* of the two sink tables are not in `AppConfig`: they are composed at runtime as `{results_database}.{dl_layer}_dqf_{domain}_{results|failed_records}` from the required `--dl-layer`/`--domain` CLI flags. CLI flags override the env defaults; if `results_write_enabled=False` results are only logged.

### Quality pipeline flow (`quality/engine.py::run_pipeline`)

`run_pipeline` is a thin orchestrator: it only calls functions, the logic lives in the leaf modules below.

1. `contract_parser.parse_contract_file` — delegates I/O to `contract_reader.read_contract_doc` (local path or GitHub Contents API, base64-decoded, `GITHUB_TOKEN` from `common.secrets`), then extracts metadata, `dataset`, `table_name` and the normalized SodaCL. Returns a dict (no checks pre-extracted). **Never returns `None`**: an unreadable contract raises `ContractNotReadableError` and a contract with no usable `quality` block (missing/empty, `type != SodaCL`, missing `dataset`/`specification`) raises `ContractValidationError` (`quality/errors.py`). Both derive from `DQFrameworkError`, which `run_quality.main` catches to log a one-line error and `sys.exit(1)` — so `spark-submit` returns non-zero and CDE marks the run FAILED instead of a silent green run with zero checks.
2. `utils.incremental.apply_incremental_conditions` — if the SodaCL contains `${INCREMENTAL_CONDITIONS}`, resolves the per-check watermark (CLI override → Iceberg `pass` lookup → epoch bootstrap) and substitutes it; otherwise a no-op. The lookup reads the same `{dl_layer}_dqf_{domain}_results` table the writer targets, so two layers have independent watermarks. If the lookup *query* fails (missing table, e.g. right after a rename) there is no fallback: it raises `RuntimeError` and the operator must pass `--watermark-from`.
3. `contract_parser.extract_and_clean_failed_queries` — pulls the deferred `failed query fields` out of the SodaCL **after** watermark substitution (so the saved query has resolved timestamps).
4. `init_spark(app_name=...)` — `SparkSession` with `enableHiveSupport()`, app name derived from the contract (`gpd_quality_<table_name>`).
5. `soda_executor.run_dataframe_soda_scan` — loads the table (and xref) via `_load_view`, injects a `MemorySampler` (failed rows captured on-prem; Soda Cloud `samples_limit` forced to 0 so no record samples are uploaded), runs the scan.
6. `result_writer.process_scan_results` — flattens Soda outcomes into rows matching `RESULTS_SCHEMA` (`run_id`, Airflow context, dataset, check metadata, measured values, row counts, watermark columns). `dag_id` is `NOT NULL`; falls back to `manual:{env}`.
7. `result_writer.write_results_to_iceberg` — `CREATE TABLE IF NOT EXISTS` on `{dl_layer}_dqf_{domain}_results` (Iceberg, partitioned by `execution_date`, merge-on-read) then `df.writeTo(fqn).append()`. Skipped when `results_write_enabled=False`. When `results_table_location` is set, the `dl_layer` prefix must appear in the directory name too, otherwise two layers would share one warehouse directory.
8. `result_writer.run_manual_failed_queries` + `process_and_write_failed_records` — re-run the deferred failed-row queries and persist the offending records to the `{dl_layer}_dqf_{domain}_failed_records` Iceberg table. The primary keys come from the `--primary-keys` CLI flag (surrogate `dl_id` if absent).

Auxiliary modules live under `quality/utils/`: logging summaries in `utils/result_logging`, incremental watermark logic in `utils/incremental`. There is no Impala execution path.

### Tests

`tests/mock_data_setup.py` is the bootstrap script invoked by `make run-dev*`: it spins up a local Spark session, reads the configured Data Contract to derive the expected schema, and writes synthetic data conforming to it. `tests/fixtures/contracts/` and `tests/fixtures/data/` hold the sample contracts and CSVs. `tests/unit/` and `tests/integration/` are the test homes.

## Conventions

- Code, log messages, and docstrings are in **Italian** — match the existing style when extending.
- `pre-commit` runs `ggshield` (secret scanning, requires `GITGUARDIAN_API_KEY`) and the PagoPa Google-style Java hook.
- Dependencies in `pyproject.toml` are intentionally unpinned (`# TODO: pin all versions to match the exact CDE cluster environment`) — be careful when bumping `pyspark`/`soda-core-spark`.
