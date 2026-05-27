# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Data Quality framework for the PagoPa CDP platform. Reads **Data Contract** YAML files (locally or from GitHub), converts the embedded quality rules to **SodaCL** via `datacontract-cli`, runs the Soda scan against a Spark DataFrame, and writes structured results to an Iceberg table (and optionally to Soda Cloud).

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
  --contract-path src/data/.../dc-foo.yaml --repository owner/repo --ref main
```

Note: `pyproject.toml` declares `ruff` as the dev linter, but the Makefile target uses `flake8`. Use `make lint` to match CI.

## Architecture

**Source layout is `src/`-based** (`src/dq_framework/...`), despite the README's example tree showing `dq_framework/src/...` — trust the filesystem. Two `spark-submit` entrypoints are exposed via `[project.scripts]`:

- `run_quality` → `dq_framework.entrypoints.run_quality:main`
- `run_observability` → `dq_framework.entrypoints.run_observability:main` (in development)

### Environment-driven config

`dq_framework.common.config.load_config()` reads `ENV` (default `dev`) and returns a frozen `AppConfig` dataclass picked from a registry: `dev` (local contract + local data), `dev-github` (contract fetched from GitHub API), `prod`. Each is defined in its own file under `common/config/`. `AppConfig` controls: default `contract_path`/`repository`/`ref`, Soda host & `data_source`, `table_limit`, and the Iceberg results sink (`results_database`, `results_table`, `results_table_location`, `results_write_enabled`). CLI flags override the env defaults; if `results_write_enabled=False` results are only logged.

### Quality pipeline flow (`quality/engine.py::run_pipeline`)

1. `contract_parser.parse_contract_file` — loads YAML from local path or GitHub Contents API (base64-decoded; uses `GITHUB_TOKEN` from `common.secrets`). Returns dict with `contract_title`, `contract_version`, `table_name`, parsed checks.
2. `init_spark()` — `SparkSession` with `enableHiveSupport()`, app name `gpd_quality_pipeline`.
3. `soda_executor.run_dataframe_soda_scan` — converts contract → SodaCL and runs the scan against the registered Spark DataFrame.
4. `result_writer.process_scan_results` — flattens Soda check outcomes into rows matching `RESULTS_SCHEMA` (includes `run_id`, Airflow context, dataset, check metadata, measured values, row counts). `dag_id` is `NOT NULL`; if no Airflow context is provided it falls back to `manual:{env}`.
5. `_write_results_to_iceberg` — `CREATE TABLE IF NOT EXISTS` (Iceberg, partitioned by `execution_date`, merge-on-read) then `df.writeTo(fqn).append()`. Skipped entirely when `results_write_enabled=False`.

There is a commented-out path for executing checks directly against Impala via SQL embedded in the contract's `quality:` block — keep this in mind when extending the scan stage.

### Tests

`tests/mock_data_setup.py` is the bootstrap script invoked by `make run-dev*`: it spins up a local Spark session, reads the configured Data Contract to derive the expected schema, and writes synthetic data conforming to it. `tests/fixtures/contracts/` and `tests/fixtures/data/` hold the sample contracts and CSVs. `tests/unit/` and `tests/integration/` are the test homes.

## Conventions

- Code, log messages, and docstrings are in **Italian** — match the existing style when extending.
- `pre-commit` runs `ggshield` (secret scanning, requires `GITGUARDIAN_API_KEY`) and the PagoPa Google-style Java hook.
- Dependencies in `pyproject.toml` are intentionally unpinned (`# TODO: pin all versions to match the exact CDE cluster environment`) — be careful when bumping `pyspark`/`soda-core-spark`.
