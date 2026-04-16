include .env
export

# Makefile per l'ambiente Dev Container
.PHONY: run-soda all


lint:
	flake8 .

run-soda-direct:
	python ./dq_framework/tests/mock_data_setup.py && IGNORE_DATACONTRACT_CLI=true python ./dq_framework/src/data_quality.py "./dq_framework/tests/fixtures/contracts/dc payment-option v4.yaml"

run-soda:
	python ./dq_framework/tests/mock_data_setup.py && python ./dq_framework/src/data_quality.py "./dq_framework/tests/fixtures/contracts/dc payment-option v4.yaml"