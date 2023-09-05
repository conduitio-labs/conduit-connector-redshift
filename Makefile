GOLANG_CI_LINT_VER=v1.54.2

.PHONY: build
build:
	go build -o conduit-connector-redshift cmd/connector/main.go

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
lint: golangci-lint-install
	golangci-lint run -v

.PHONY: dep
dep:
	go mod download
	go mod tidy

.PHONY: mockgen
mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
