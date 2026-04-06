# Contributing to ES Bulk Proxy

Thank you for your interest in contributing to ES Bulk Proxy! This document provides guidelines and instructions for contributing to the project.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [Development Workflow](#development-workflow)
- [Code Standards](#code-standards)
- [Testing Guidelines](#testing-guidelines)
- [Submitting Changes](#submitting-changes)
- [Project Structure](#project-structure)
- [Useful Commands](#useful-commands)

## Code of Conduct

This project adheres to the Contributor Covenant [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code. Please report unacceptable behavior to the project maintainers.

## Getting Started

### Prerequisites

Before you begin, ensure you have the following installed:

- **Go 1.25+**: [Download and install Go](https://golang.org/dl/)
- **Docker**: [Install Docker](https://docs.docker.com/get-docker/)
- **Docker Compose**: Usually included with Docker Desktop
- **Git**: [Install Git](https://git-scm.com/downloads)
- **Make**: Should be available on most Unix-like systems
- **pre-commit** (optional but recommended): `pip install pre-commit`

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:

   ```bash
   git clone https://github.com/YOUR_USERNAME/es-bulk-proxy.git
   cd es-bulk-proxy
   ```

3. Add the upstream repository:

   ```bash
   git remote add upstream https://github.com/ORIGINAL_OWNER/es-bulk-proxy.git
   ```

## Development Setup

### 1. Install Dependencies

```bash
make deps
```

This will download all Go module dependencies.

### 2. Install Pre-commit Hooks (Recommended)

```bash
make precommit-install
```

This installs pre-commit hooks that will automatically run linters and formatters before each commit.

### 3. Start Development Environment

```bash
make dev
```

This starts Elasticsearch, Prometheus, and Grafana using Docker Compose:

- ES Proxy: <http://localhost:8080>
- Elasticsearch: <http://localhost:9200>
- Prometheus: <http://localhost:9090>
- Grafana: <http://localhost:3000>

## Development Workflow

### 1. Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

Use descriptive branch names:

- `feature/add-compression-support`
- `fix/buffer-overflow-issue`
- `docs/improve-readme`

### 2. Make Your Changes

- Write clear, concise code following Go best practices
- Add tests for new functionality
- Update documentation as needed
- Keep commits atomic and well-described

### 3. Run Tests and Linters

Before committing, ensure your code passes all checks:

```bash
# Format code
make format

# Run linters
make lint

# Run tests
make test

# Run pre-commit checks
make precommit
```

### 4. Commit Your Changes

Write clear, descriptive commit messages:

```bash
git add .
git commit -m "feat: add compression support for bulk requests"
```

**Commit Message Format:**

```
<type>: <subject>

<body (optional)>

<footer (optional)>
```

**Types:**

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

### 5. Keep Your Branch Updated

Regularly sync with the upstream repository:

```bash
git fetch upstream
git rebase upstream/main
```

### 6. Push and Create Pull Request

```bash
git push origin feature/your-feature-name
```

Then create a Pull Request on GitHub with:

- Clear description of changes
- Reference to related issues (if any)
- Screenshots/logs for UI or behavior changes
- Test results

## Code Standards

### Go Code Style

- Follow [Effective Go](https://golang.org/doc/effective_go.html) guidelines
- Use `gofmt` for formatting (automatically run by pre-commit hooks)
- Run `go vet` to catch common mistakes
- Follow the [Standard Go Project Layout](https://github.com/golang-standards/project-layout)

### General Principles

- **Keep it simple**: Prefer simple, readable code over clever solutions
- **DRY**: Don't Repeat Yourself
- **SOLID**: Follow SOLID principles where applicable
- **Error handling**: Always handle errors properly, never ignore them
- **Logging**: Use structured logging with appropriate log levels
- **Comments**: Write godoc-style comments for exported functions and types

### Code Organization

```go
// Package example demonstrates proper code organization
package example

import (
    "context"
    "fmt"
    
    "github.com/rs/zerolog/log"
)

// MyStruct represents...
// Explain the purpose of exported types
type MyStruct struct {
    field string
}

// NewMyStruct creates a new instance...
// Document exported functions
func NewMyStruct(field string) *MyStruct {
    return &MyStruct{field: field}
}

// DoSomething performs...
// Describe what exported methods do
func (m *MyStruct) DoSomething(ctx context.Context) error {
    log.Debug().Str("field", m.field).Msg("doing something")
    // Implementation
    return nil
}
```

### Naming Conventions

- **Packages**: Short, lowercase, single-word names (avoid underscores)
- **Variables**: camelCase for locals, PascalCase for exported
- **Constants**: PascalCase for exported, camelCase for internal
- **Functions**: PascalCase for exported, camelCase for internal
- **Interfaces**: Often end with "er" (e.g., `Reader`, `Writer`, `Flusher`)

## Testing Guidelines

### Writing Tests

- Place tests in `*_test.go` files alongside the code
- Use table-driven tests where appropriate
- Test both success and failure cases
- Mock external dependencies (Elasticsearch, etc.)

**Example:**

```go
func TestBufferAdd(t *testing.T) {
    tests := []struct {
        name    string
        data    []byte
        wantErr bool
    }{
        {"valid data", []byte(`{"index": {}}`), false},
        {"empty data", []byte{}, true},
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            buf := NewBuffer(1024)
            err := buf.Add(tt.data)
            if (err != nil) != tt.wantErr {
                t.Errorf("Add() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

### Running Tests

```bash
# Run all tests
make test

# Run tests with race detection
go test -race ./...

# Run specific package
go test ./internal/buffer

# Run with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Run benchmarks
make bench
```

### Integration Tests

```bash
# Start test environment and run integration tests
make integration-test
```

## Submitting Changes

### Pull Request Checklist

Before submitting your PR, ensure:

- [ ] Code follows the project's style guidelines
- [ ] All tests pass (`make test`)
- [ ] Linters pass (`make lint`)
- [ ] Pre-commit hooks pass (`make precommit`)
- [ ] New code has appropriate test coverage
- [ ] Documentation is updated (if applicable)
- [ ] Commit messages are clear and descriptive
- [ ] Branch is up-to-date with `main`

### Pull Request Description

Include in your PR description:

1. **What**: Brief description of changes
2. **Why**: Motivation for the changes
3. **How**: Technical approach (if complex)
4. **Testing**: How you tested the changes
5. **Related Issues**: Reference any related issues (e.g., "Fixes #123")

**Template:**

```markdown
## Description
Brief description of what this PR does

## Motivation
Why is this change needed?

## Changes
- Change 1
- Change 2

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests pass
- [ ] Manual testing performed

## Related Issues
Fixes #123
```

### Review Process

1. Automated checks will run (tests, linters)
2. Maintainers will review your code
3. Address any feedback or requested changes
4. Once approved, a maintainer will merge your PR

## Project Structure

Understanding the project layout:

```
es-bulk-proxy/
├── cmd/                    # Application entry points
│   └── main.go            # Main application
├── internal/              # Private application code
│   ├── buffer/           # Bulk request buffering logic
│   ├── config/           # Configuration management
│   ├── handler/          # HTTP handlers and routing
│   ├── logger/           # Structured logging setup
│   └── metrics/          # Prometheus metrics
├── configs/              # Configuration files
├── deployments/          # Deployment configs (Docker, K8s)
├── scripts/              # Build and utility scripts
├── Dockerfile            # Container build
├── Makefile             # Build automation
└── go.mod               # Go dependencies
```

### Adding New Features

When adding new features:

1. **Internal packages**: Add to appropriate `internal/` subdirectory
2. **Configuration**: Update `internal/config/config.go` and `configs/config.yaml`
3. **Metrics**: Add Prometheus metrics in `internal/metrics/metrics.go`
4. **Documentation**: Update README.md and add inline godoc comments
5. **Tests**: Add comprehensive unit and integration tests

## Useful Commands

### Building

```bash
make build              # Build the binary
make docker-build      # Build Docker image
```

### Running

```bash
make run               # Run locally
make docker-run        # Run in Docker
make dev               # Start full dev environment
```

### Testing

```bash
make test              # Run tests
make integration-test  # Run integration tests
make bench             # Run benchmarks
```

### Quality Checks

```bash
make format            # Format code
make lint              # Run linters
make vet               # Run go vet
make precommit         # Run all pre-commit checks
```

### Deployment

```bash
make k8s-deploy        # Deploy to Kubernetes
make k8s-delete        # Remove from Kubernetes
make k8s-logs          # View logs
```

### Cleanup

```bash
make clean             # Remove build artifacts
make docker-stop       # Stop Docker containers
make docker-compose-down  # Stop Docker Compose stack
```

## Getting Help

- **Issues**: Open an issue on GitHub for bugs or feature requests
- **Discussions**: Use GitHub Discussions for questions and ideas
- **Documentation**: Check the [README](README.md) and [Project](Project.md) docs

## Recognition

Contributors will be recognized in:

- GitHub contributors list
- Release notes for significant contributions
- Project documentation (when appropriate)

Thank you for contributing to ES Bulk Proxy! 🚀
