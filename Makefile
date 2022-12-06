.PHONY: build test

build:
	go build -o conduit-connector-redshift cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -v -race ./...

lint:
	golangci-lint run --config .golangci.yml

dep:
	go mod download
	go mod tidy

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
