.PHONY: clean run build test lint gqlgen gql-model gql generate

SHELL := /bin/sh
PATHINSTBIN = $(abspath ./bin)
export PATH := $(PATHINSTBIN):$(PATH)

BIN_NAME          ?= dq
DEFAULT_ARCH      := $(shell go env GOARCH)
DEFAULT_GOOS      := $(shell go env GOOS)
ARCH              ?= $(DEFAULT_ARCH)
GOOS              ?= $(DEFAULT_GOOS)
.DEFAULT_GOAL     := run

VERSION   := $(shell git describe --tags 2>/dev/null || echo "v0.0.0")
VER_CUT   := $(shell echo $(VERSION) | cut -c2-)

help: ## Show available targets
	@echo "\nSpecify a subcommand:\n"
	@grep -hE '^[0-9a-zA-Z_-]+:.*?## .*$$' ${MAKEFILE_LIST} | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[0;36m%-20s\033[m %s\n", $$1, $$2}'
	@echo ""

build: ## Build the binary (CGO_ENABLED=1 required for h3-go)
	@CGO_ENABLED=1 GOOS=$(GOOS) GOARCH=$(ARCH) \
		go build -o $(PATHINSTBIN)/$(BIN_NAME) ./cmd/$(BIN_NAME)

run: build ## Build and run the binary
	@./$(PATHINSTBIN)/$(BIN_NAME)

clean: ## Remove built binaries
	@rm -rf $(PATHINSTBIN)

tidy: ## Tidy go modules
	@go mod tidy

test: ## Run tests
	@go test -v ./...

lint: ## Run linter
	@golangci-lint run

docker: ## Build Docker image
	@docker build -f ./Dockerfile . -t dimozone/$(BIN_NAME):$(VER_CUT)
	@docker tag dimozone/$(BIN_NAME):$(VER_CUT) dimozone/$(BIN_NAME):latest

gqlgen: ## Generate gqlgen server code
	@go tool gqlgen generate

gql-model: ## Run model-garage codegen (signals schema + model + resolver stubs)
	@go tool codegen -generators=custom \
		-custom.output-file=schema/signals-events_gen.graphqls \
		-custom.template-file=./schema/signals-events.tmpl
	@go tool codegen -generators=custom \
		-custom.output-file=internal/graph/model/signalSetter_gen.go \
		-custom.template-file=./internal/graph/model/signalSetter.tmpl \
		-custom.format=true
	@go tool codegen -generators=custom \
		-custom.output-file=internal/graph/signals-events_gen.resolvers.go \
		-custom.template-file=./internal/graph/signals-events_gen.resolvers.tmpl \
		-custom.format=true

gql: gql-model gqlgen ## Run full code generation (model-garage → gqlgen)

generate: gql ## Run all code generators
	@go generate ./...
