.PHONY: all
all: ## Show the available make targets.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@fgrep "##" Makefile | fgrep -v fgrep

.PHONY: clean
clean: ## Clean the temporary files.
	rm -rf .mypy_cache
	rm -rf .ruff_cache
	rm -rf .pytest_cache
	rm -rf htmlcov
	rm -rf .coverage

# Make does not like interpreting : in the target name, so we use a variable
RUN_CMD=poetry run python -m survey_assist_themes.demo_themefinder_vertexai

run-themes: ## Run the ThemeFinder application
	$(RUN_CMD)

run-docs: ## Run the mkdocs server
	poetry run mkdocs serve

check-python: ## Format the python code (auto fix)
	poetry run ruff check . --fix
	poetry run mypy src tests
	poetry run ruff format .

check-python-nofix: ## Check the python code (no fix)
	poetry run ruff check .
	poetry run mypy src tests
	poetry run ruff format --check .

tests: ## Run all tests with coverage
	poetry run pytest --cov=survey_assist_themes --cov-report=term-missing --cov-report=html

tests-verbose: ## Run all tests with verbose output
	poetry run pytest -v --cov=survey_assist_themes --cov-report=term-missing

all-tests: ## Run all tests with coverage
	@if [ -d "tests" ] && [ -n "$$(find tests -name '*.py' -type f ! -name '__init__.py' 2>/dev/null)" ]; then \
		poetry run pytest --cov=survey_assist_themes --cov-report=term-missing --cov-report=html; \
	else \
		echo "No tests found, skipping test execution"; \
	fi

install: ## Install the dependencies
	poetry install --only main --no-root

install-dev: ## Install the dev dependencies
	poetry install --no-root

