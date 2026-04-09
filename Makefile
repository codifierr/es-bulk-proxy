.PHONY: help build run test docker-build docker-run docker-compose-up docker-compose-down clean k8s-deploy k8s-delete deps lint precommit precommit-install

# Variables
IMAGE-PREFIX=ssingh3339
BINARY_NAME=es-bulk-proxy
IMAGE_NAME=es-bulk-proxy
IMAGE_TAG=latest
DOCKER_REGISTRY ?= localhost

help: ## Show this help message
	@echo 'Usage: make [target]'
	@echo ''
	@echo 'Available targets:'
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-20s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

deps: ## Download Go dependencies
	go mod download
	go mod tidy

precommit-install: ## Install pre-commit hooks
	@which pre-commit > /dev/null || (echo "Installing pre-commit..." && pip install pre-commit)
	pre-commit install

precommit: ## Run pre-commit on all files
	pre-commit run --all-files

build: ## Build the binary
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags='-w -s' -o $(BINARY_NAME) ./cmd

run: ## Run the application locally
	ES_URL=http://localhost:9200 go run ./cmd/es-bulk-proxy

test: ## Run tests
	go test -v ./...

lint: ## Run linters
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...
	pre-commit run --all-files

docker-build: ## Build Docker image
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--output "type=docker,push=false" \
		--tag $(IMAGE-PREFIX)/$(IMAGE_NAME):$(IMAGE_TAG) .

docker-publish: docker-build ## Build and publish Docker image
	docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--output "type=image,push=true" \
		--tag $(IMAGE-PREFIX)/$(IMAGE_NAME):$(IMAGE_TAG) .

docker-run: ## Run Docker container
	docker run -d \
		-p 8080:8080 \
		-e ES_URL=http://host.docker.internal:9200 \
		--name $(BINARY_NAME) \
		$(IMAGE_NAME):$(IMAGE_TAG)

docker-stop: ## Stop Docker container
	docker stop $(BINARY_NAME) || true
	docker rm $(BINARY_NAME) || true

docker-compose-up: ## Start services with Docker Compose
	cd deployments && docker compose up -d

docker-compose-down: ## Stop services with Docker Compose
	cd deployments && docker compose down -v

docker-compose-logs: ## View Docker Compose logs
	cd deployments && docker compose logs -f es-proxy

integration-test: docker-compose-up ## Run integration tests
	@chmod +x scripts/test.sh
	./scripts/test.sh

k8s-deploy: ## Deploy to Kubernetes
	kubectl apply -f deployments/kubernetes.yaml

k8s-delete: ## Delete from Kubernetes
	kubectl delete -f deployments/kubernetes.yaml

k8s-logs: ## View Kubernetes logs
	kubectl logs -l app=es-proxy -f

k8s-port-forward: ## Port forward Kubernetes service
	kubectl port-forward svc/es-proxy 8080:8080

clean: ## Clean build artifacts
	rm -f $(BINARY_NAME)
	rm -f coverage.out coverage.html
	docker-compose down -v || true
	docker rmi $(IMAGE_NAME):$(IMAGE_TAG) || true

format: ## Format Go code
	go fmt ./...
	gofmt -s -w .

vet: ## Run go vet
	go vet ./...

bench: ## Run benchmarks
	go test -bench=. -benchmem ./...

all: deps lint test build docker-build ## Run all checks and build

dev: docker-compose-up ## Start development environment
	@echo "Services started!"
	@echo "ES Proxy: http://localhost:8080"
	@echo "Elasticsearch: http://localhost:9200"
	@echo "Prometheus: http://localhost:9090"
	@echo "Grafana: http://localhost:3001 (admin/admin)"
	@echo "Dashboard: http://localhost:3001/d/es-proxy-dashboard"
	@echo ""
	@echo "Run 'make docker-compose-logs' to view logs"
	@echo "Run 'make test-traffic' to generate sample traffic"

test-traffic: ## Generate test traffic for dashboard
	@chmod +x scripts/generate-test-traffic.sh
	@./scripts/generate-test-traffic.sh

.DEFAULT_GOAL := help
