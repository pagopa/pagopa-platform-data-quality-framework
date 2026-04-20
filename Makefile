include .env
export

.PHONY: lint run-dev run-local-github run-prod

lint:
	flake8 .

run-dev:
	python ./dq_framework/tests/mock_data_setup.py && ENV=dev python -m dq_framework.src.entrypoints.run_quality

run-dev-github:
	python ./dq_framework/tests/mock_data_setup.py && ENV=local-github python -m dq_framework.src.entrypoints.run_quality
