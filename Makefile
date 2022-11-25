.PHONY: build test

build:
	go build -o conduit-connector-redshift cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) -v -count=1 -race ./...

lint:
	golangci-lint run --config .golangci.yml

dep:
	go mod download
	go mod tidy