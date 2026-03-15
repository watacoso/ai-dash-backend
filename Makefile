.PHONY: dev dev-up dev-down test seed db-create e2e-reset

## ── Dev infrastructure ────────────────────────────────────────────────────────

dev-up: ## Start dev Postgres + Redis containers.
	docker compose -f docker-compose.dev.yml up -d

dev-down: ## Stop and remove dev containers.
	docker compose -f docker-compose.dev.yml down

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

test: ## Run the full pytest suite (uses docker-compose.test.yml on ports 5433/6380).
	python -m pytest
