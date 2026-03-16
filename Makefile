.PHONY: dev dev-up dev-down test seed db-create e2e-reset seed-live-connections

## ── Dev infrastructure ────────────────────────────────────────────────────────

dev-up: ## Start dev Postgres + Redis containers.
	docker compose -f docker-compose.dev.yml up -d

dev-down: ## Stop and remove dev containers.
	docker compose -f docker-compose.dev.yml down

## ── Dev server ────────────────────────────────────────────────────────────────

dev: seed seed-live-connections ## Seed users + live connections, then start uvicorn with hot reload.
	.venv/bin/uvicorn app.main:app --reload

## ── Database ──────────────────────────────────────────────────────────────────

db-create: ## Create all tables (idempotent).
	.venv/bin/python -m app.auth.db_create

seed: db-create ## Create tables and seed default users.
	.venv/bin/python -m app.auth.seed

seed-live-connections: ## Seed LIVE SNOWFLAKE + LIVE CLAUDE connections from ai-dash-frontend/.env.e2e.
	.venv/bin/python -m app.connections.seed_live

e2e-reset: ## Reset DB to clean E2E baseline (restore seed users, remove invite users).
	.venv/bin/python -m app.auth.e2e_reset

## ── Testing ───────────────────────────────────────────────────────────────────

test: ## Run the full pytest suite (uses docker-compose.test.yml on ports 5433/6380).
	.venv/bin/python -m pytest
