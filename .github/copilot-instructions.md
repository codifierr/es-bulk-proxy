# ES Bulk Proxy - Copilot Instructions

## Language and Framework
This repository uses **Go 1.26.2** for the ES Bulk Proxy service.

## Development Workflow

### Always Follow These Steps When Making Changes

1. **Implement changes** following Go best practices
2. **Format code** using `make format` or `gofmt -s -w .`
3. **Verify changes** by running all verification commands:
   ```bash
   make build    # Build the binary
   make vet      # Run go vet
   make lint     # Run golangci-lint with pre-commit hooks
   make test     # Run all unit tests
   ```
4. **Run integration tests** if changes affect core functionality:
   ```bash
   make integration-test
   ```
5. **Update related files** as needed (see below)
6. **Update README.md** once all changes are validated and complete

## Required Updates

### Metrics Changes
When adding, removing, or updating metrics:
- Update the Grafana dashboard: `deployments/grafana-dashboard.json`
- Update the dashboard documentation: `deployments/GRAFANA_DASHBOARD.md`
- Ensure both files stay in sync
- Update metric definitions in `internal/metrics/metrics.go`
- Add corresponding test cases in `internal/metrics/metrics_test.go`

### Configuration Changes
When modifying configuration:
- Update `internal/config/config.go` with new fields
- Update `configs/config.yaml` with examples
- Add validation logic if needed
- Update configuration test cases in `internal/config/config_test.go`
- Document changes in README.md

### New Functionality
- **Always add test cases** for new functionality
- Follow existing test patterns in `*_test.go` files
- Maintain or improve test coverage
- Add table-driven tests where appropriate
- Test both success and error scenarios
- Include edge cases and boundary conditions

### Code Quality Standards
- Follow Go best practices and idioms
- Ensure code passes `make lint` checks
- Write clear, maintainable code with appropriate comments
- Keep functions focused and testable
- Use meaningful variable and function names
- Handle errors explicitly; never ignore errors
- Use structured logging with zerolog
- Follow the [Standard Go Project Layout](https://github.com/golang-standards/project-layout)
- Run `gofmt -s -w .` to format code with simplifications
- Use pre-commit hooks: install with `make precommit-install`

### Linting and Formatting
- The project uses golangci-lint with extensive linters enabled (see `.golangci.yaml`)
- Format code with `make format` before committing
- Run `make precommit` to execute all pre-commit hooks manually
- Pre-commit hooks automatically check:
  - Trailing whitespace
  - End-of-file fixers
  - YAML syntax
  - Large file additions

### Project Structure
- `/cmd` - Main application entry points
- `/internal` - Private application code (not importable by other projects)
  - `/internal/buffer` - Bulk buffer aggregation logic
  - `/internal/config` - Configuration management
  - `/internal/handler` - HTTP handlers and routing
  - `/internal/logger` - Structured logging
  - `/internal/metrics` - Prometheus metrics
- `/configs` - Configuration file examples
- `/deployments` - Deployment configurations (Docker, K8s, Grafana)
- `/scripts` - Build and test scripts

## Testing Requirements
- All new functionality must include corresponding test cases
- Tests should cover both success and error scenarios
- Run `make test` to ensure all tests pass before finalizing changes
- Use table-driven tests where multiple similar test cases exist
- Mock external dependencies (HTTP servers, Elasticsearch)
- Test concurrent operations where applicable
- Integration tests are available via `make integration-test` (requires Docker)
- Tests should be idempotent and isolated

## Development Environment
- Use `make dev` to start the full development stack with Docker Compose:
  - Elasticsearch on port 9200
  - ES Proxy on port 8080
  - Prometheus on port 9090
  - Grafana on port 3001 (admin/admin)
- Development dependencies: `make deps` to download Go modules
- Pre-commit setup: `make precommit-install` to install hooks
- View logs: `make docker-compose-logs` for Docker Compose logs
- Clean environment: `make clean` to remove build artifacts and containers

## Docker and Kubernetes
- Docker build: `make docker-build` (multi-arch: amd64, arm64)
- Docker publish: `make docker-publish` (requires Docker Hub credentials)
- K8s deployment: `make k8s-deploy` applies `deployments/kubernetes.yaml`
- K8s logs: `make k8s-logs` to view pod logs
- K8s port-forward: `make k8s-port-forward` to access service locally
- When updating Dockerfile, ensure multi-stage build optimization is maintained

## Documentation
- Update README.md after completing changes
- Keep inline code comments clear and up-to-date
- Update configuration examples if config structure changes
