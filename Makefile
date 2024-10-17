install:
	pip install --upgrade pip &&\
		pip install -r requirements.txt

test:
	python -m pytest -vv --cov=main --cov=mylib test_*.py

format:
	black *.py

lint:
	ruff check *.py mylib/*.py

container-lint:
	docker run --rm hadolint/hadolint < Dockerfile

refactor: format lint

all: install lint test format