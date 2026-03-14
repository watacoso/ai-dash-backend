.PHONY: dev test seed db-create e2e-reset

## ── Dev server ────────────────────────────────────────────────────────────────

dev: ## Start uvicorn with hot reload.
	uvicorn app.main:app --reload

## ── Database ──────────────────────────────────────────────────────────────────

db-create: ## Create all tables (idempotent).
	python -m app.auth.db_create

seed: db-create ## Create tables and seed default users.
	python -m app.auth.seed

e2e-reset: ## Reset DB to clean E2E baseline (restore seed users, remove invite users).
	python -m app.auth.e2e_reset

## ── Testing ───────────────────────────────────────────────────────────────────

test: ## Run the full pytest suite.
	python -m pytest
