.PHONY: build test integration integration-verbose down reset

build:
	go build ./...

# Unit tests only: no Docker required.
test:
	go test ./...

# Integration tests against the running docker compose stack. The tests need the
# source/destination PostgreSQL and Kafka containers (source port 5433, destination
# 5434, Kafka 9092), and use an isolated destination_test database.
integration:
	PGCDC_INTEGRATION=1 go test -tags=integration -count=1 -timeout 180s ./internal/source/... ./internal/destination/... ./cmd/destination-writer/...

integration-verbose:
	PGCDC_INTEGRATION=1 go test -tags=integration -count=1 -timeout 180s -v ./internal/source/... ./internal/destination/... ./cmd/destination-writer/...

down:
	docker compose down

reset:
	docker compose down -v