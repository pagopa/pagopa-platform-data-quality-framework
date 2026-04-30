include .env
export

.PHONY: lint run-dev run-local-github run-prod

lint:
	flake8 .

run-dev:
	python ./tests/mock_data_setup.py && ENV=dev python -m dq_framework.entrypoints.run_quality

run-dev-github:
	python ./tests/mock_data_setup.py && ENV=dev-github python -m dq_framework.entrypoints.run_quality
