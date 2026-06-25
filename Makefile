.PHONY: clean run build test test-gated lint gqlgen gql-model gql generate generate-grpc tools-protoc tools-protoc-plugins

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

PROTOC_VERSION             = 33.4
PROTOC_GEN_GO_GRPC_VERSION = v1.5.1

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

test-gated: ## Run the cutover-gate suites (PG concurrency, chaos, perf, MinIO). Suites without their env/flag/binary skip cleanly.
	@echo "== standard ==";        CGO_ENABLED=1 go test ./...
	@echo "== PG concurrency (set PG_CATALOG_DSN) =="; CGO_ENABLED=1 go test ./tests/ -run TestDuckLakePostgres -count=1
	@echo "== chaos (set DQ_CHAOS=1) ==";  CGO_ENABLED=1 go test ./tests/ -run Chaos -count=1
	@echo "== perf (-perf) ==";     CGO_ENABLED=1 go test ./tests/ -run Perf -perf -count=1
	@echo "== MinIO (install minio) =="; CGO_ENABLED=1 go test ./tests/ -run MinIO -count=1

lint: ## Run linter
	@golangci-lint run

docker: ## Build Docker image
	@docker build -f Dockerfile . -t dimozone/$(BIN_NAME):$(VER_CUT)
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

generate-grpc: ## Generate gRPC Go files from proto definitions
	@PATH=$$PATH protoc --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		pkg/grpc/*.proto

generate: gql generate-grpc ## Run all code generators
	@go generate ./...

tools-protoc: ## Install protoc
	@mkdir -p $(PATHINSTBIN)
	rm -rf $(PATHINSTBIN)/protoc
ifeq ($(shell uname | tr A-Z a-z), darwin)
	curl -L https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-osx-x86_64.zip > bin/protoc.zip
endif
ifeq ($(shell uname | tr A-Z a-z), linux)
	curl -L https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip > bin/protoc.zip
endif
	unzip -o $(PATHINSTBIN)/protoc.zip -d $(PATHINSTBIN)/protoclib
	mv -f $(PATHINSTBIN)/protoclib/bin/protoc $(PATHINSTBIN)/protoc
	rm -rf $(PATHINSTBIN)/include
	mv $(PATHINSTBIN)/protoclib/include $(PATHINSTBIN)/
	rm $(PATHINSTBIN)/protoc.zip

tools-protoc-plugins: ## Install protoc-gen-go and protoc-gen-go-grpc
	@mkdir -p $(PATHINSTBIN)
	GOBIN=$(PATHINSTBIN) go install google.golang.org/protobuf/cmd/protoc-gen-go
	GOBIN=$(PATHINSTBIN) go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@${PROTOC_GEN_GO_GRPC_VERSION}
