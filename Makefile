.PHONY: help build run test lint fmt clean tidy docker-up docker-down

# Variables
GO := go
GOFLAGS := -v
BINARY_NAME := cluster-db
BINARY_PATH := bin/$(BINARY_NAME)
MAIN_PACKAGE := ./cmd/$(BINARY_NAME)
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
BUILD_TIME := $(shell date -u '+%Y-%m-%d_%H:%M:%S')
GIT_COMMIT := $(shell git rev-parse --short HEAD 2>/dev/null || echo "unknown")
LDFLAGS := -ldflags "-X main.Version=$(VERSION) -X main.BuildTime=$(BUILD_TIME) -X main.GitCommit=$(GIT_COMMIT)"

# Default target
help:
	@echo "AtlasDB - Distributed Database"
	@echo ""
	@echo "Available targets:"
	@echo "  make build       - Build the application binary"
	@echo "  make run         - Build and run the application"
	@echo "  make test        - Run all tests"
	@echo "  make test-unit   - Run unit tests only"
	@echo "  make test-int    - Run integration tests"
	@echo "  make bench       - Run benchmarks"
	@echo "  make lint        - Run linter (golangci-lint)"
	@echo "  make fmt         - Format code with gofmt"
	@echo "  make vet         - Run go vet"
	@echo "  make tidy        - Tidy and verify go.mod"
	@echo "  make clean       - Clean build artifacts"
	@echo "  make docker-up   - Start local environment with Docker Compose"
	@echo "  make docker-down - Stop local environment"
	@echo "  make help        - Display this help message"
	@echo ""

# Build the application
build:
	@echo "Building $(BINARY_NAME)..."
	@mkdir -p bin
	$(GO) build $(GOFLAGS) $(LDFLAGS) -o $(BINARY_PATH) $(MAIN_PACKAGE)
	@echo "Build complete: $(BINARY_PATH)"

# Run the application
run: build
	@echo "Running $(BINARY_NAME)..."
	$(BINARY_PATH)

# Run all tests
test:
	@echo "Running tests..."
	$(GO) test -v -race -cover -timeout 10m ./...

# Run unit tests only
test-unit:
	@echo "Running unit tests..."
	$(GO) test -v -race -cover -short -timeout 5m ./...

# Run integration tests
test-int:
	@echo "Running integration tests..."
	$(GO) test -v -race -cover -run Integration -timeout 15m ./test/integration/...

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	$(GO) test -bench=. -benchmem -benchtime=10s ./...

# Run linter
lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found, installing..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	golangci-lint run ./...

# Format code
fmt:
	@echo "Formatting code..."
	$(GO) fmt ./...
	@echo "Code formatted"

# Run go vet
vet:
	@echo "Running go vet..."
	$(GO) vet ./...

# Tidy and verify dependencies
tidy:
	@echo "Tidying dependencies..."
	$(GO) mod tidy
	$(GO) mod verify
	@echo "Dependencies tidied"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	$(GO) clean
	rm -rf bin/
	rm -rf dist/
	rm -rf coverage/
	@echo "Clean complete"

# Start Docker Compose environment
docker-up:
	@echo "Starting Docker Compose environment..."
	docker-compose up -d
	@echo "Environment started"

# Stop Docker Compose environment
docker-down:
	@echo "Stopping Docker Compose environment..."
	docker-compose down
	@echo "Environment stopped"

# Generate code (protobuf, mocks, etc.)
generate:
	@echo "Generating code..."
	$(GO) generate ./...

# Install development dependencies
install-tools:
	@echo "Installing development tools..."
	$(GO) install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "Tools installed"

# Coverage analysis
coverage:
	@echo "Running tests with coverage..."
	$(GO) test -coverprofile=coverage/coverage.out -covermode=atomic ./...
	$(GO) tool cover -html=coverage/coverage.out -o coverage/coverage.html
	@echo "Coverage report: coverage/coverage.html"

# Check code quality
check: lint vet test
	@echo "All checks passed"

# Build optimized binary for production
build-release:
	@echo "Building production release..."
	@mkdir -p dist
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 $(GO) build $(LDFLAGS) -o dist/$(BINARY_NAME)-linux-amd64 $(MAIN_PACKAGE)
	GOOS=darwin GOARCH=amd64 CGO_ENABLED=0 $(GO) build $(LDFLAGS) -o dist/$(BINARY_NAME)-darwin-amd64 $(MAIN_PACKAGE)
	GOOS=darwin GOARCH=arm64 CGO_ENABLED=0 $(GO) build $(LDFLAGS) -o dist/$(BINARY_NAME)-darwin-arm64 $(MAIN_PACKAGE)
	GOOS=windows GOARCH=amd64 CGO_ENABLED=0 $(GO) build $(LDFLAGS) -o dist/$(BINARY_NAME)-windows-amd64.exe $(MAIN_PACKAGE)
	@echo "Release builds complete"

# Development environment setup
dev-setup: install-tools tidy build
	@echo "Development environment ready"

# CI/CD checks (used in pipelines)
ci-check: tidy lint vet test
	@echo "CI checks passed"
