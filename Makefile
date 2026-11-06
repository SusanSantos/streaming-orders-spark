.PHONY: install produce run test lint kafka-up
install:
	pip install -r requirements.txt
produce:
	python -m src.producer -n 5000
run:
	python -m src.stream_job --source files
test:
	pytest -q
lint:
	ruff check .
kafka-up:
	docker compose up -d
