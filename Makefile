include .env
export

.PHONY: lint run-dev run-local-github run-prod

lint:
	flake8 .

run-dev:
	python ./tests/mock_data_setup.py && ENV=dev python -m dq_framework.entrypoints.run_quality --watermark-from 1970-01-01 --domain gpd --dl-layer silver

run-dev-github:
	python ./tests/mock_data_setup.py && ENV=dev-github python -m dq_framework.entrypoints.run_quality --domain gpd --dl-layer silver

run-test:
	python ./tests/mock_data_setup.py && ENV=test python -m dq_framework.entrypoints.run_quality --domain gpd --dl-layer silver
