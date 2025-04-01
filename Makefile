GOLANG_CI_LINT_VER=v1.61.0
VERSION=$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-redshift.version=${VERSION}'" -o conduit-connector-redshift cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) -v -race ./...

.PHONY: gofumpt
gofumpt:
	go install mvdan.cc/gofumpt@latest

.PHONY: fmt
fmt: gofumpt
	gofumpt -l -w .

.PHONY: golangci-lint-install
golangci-lint-install:
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@$(GOLANG_CI_LINT_VER)

.PHONY: lint
lint: 
	golangci-lint run

.PHONY: dep
dep:
	go mod download
	go mod tidy

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools/go.mod
	@go list -modfile=tools/go.mod tool | xargs -I % go list -modfile=tools/go.mod -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
