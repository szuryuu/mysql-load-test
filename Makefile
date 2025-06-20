.PHONY: migrate-up migrate-down migrate-create install-tools

DB_URL ?= mysql://root:root@tcp(localhost:3306)/MySQLLoadTester

migrate-up:
	@if [ -z "$$DB_URL" ]; then \
		echo "Error: DB_URL environment variable is required"; \
		exit 1; \
	fi
	migrate -database "${DB_URL}" -path internal/database/migrations/mysql up

migrate-down:
	@if [ -z "$$DB_URL" ]; then \
		echo "Error: DB_URL environment variable is required"; \
		exit 1; \
	fi
	migrate -database "${DB_URL}" -path internal/database/migrations/mysql down

migrate-create:
	@if [ -z "$$NAME" ]; then \
		read -p "Enter migration name: " name; \
	else \
		name="$$NAME"; \
	fi; \
	migrate create -ext sql -dir internal/database/migrations/mysql -seq $$name

install-tools:
	go install -tags 'mysql' github.com/golang-migrate/migrate/v4/cmd/migrate@latest

print-db-url:
	@echo "Current DB_URL: ${DB_URL}"
