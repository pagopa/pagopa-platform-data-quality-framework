.PHONY: test-unit test-integration test lint build local-run-quality local-run-observability

test-unit:        ## run fast unit tests (no Spark)
test-integration: ## run integration tests (requires Docker/devcontainer)
test:             ## run all tests
lint:             ## lint and format check (ruff)
build:            ## build the deployable .whl artifact
local-run-quality:        ## SOURCE=<system> CONTRACT=<path> — local end-to-end quality run
local-run-observability:  ## SOURCE=<system> — local end-to-end observability run

# TODO: implement each target body once entrypoints are in place
