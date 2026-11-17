.PHONY: validate plan compile run test report publish up down verify-airflow analytics demo

PYTHON := .venv/bin/python
export YAMLFLOW_DB := $(CURDIR)/data/analytics.duckdb

validate plan compile run report publish:
	$(PYTHON) -m yamlflow.cli $@

test:
	$(PYTHON) -m pytest -q

up: compile
	chmod g+w data
	docker compose up -d --build

down:
	docker compose down

verify-airflow:
	docker compose exec -T airflow python - < scripts/verify_airflow.py

analytics:
	$(PYTHON) -m streamlit run streamlit_app.py

demo: run
	$(PYTHON) -m yamlflow.cli publish
	$(PYTHON) -m streamlit run streamlit_app.py
