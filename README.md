# Conduit Connector Amazon Redshift

## General

Amazon Redshift connector is one of [Conduit](https://github.com/ConduitIO/conduit) plugins. It provides both, a Source
and a Destination Amazon Redshift connectors.

## Prerequisites

- [Go](https://go.dev/) 1.19
- (optional) [golangci-lint](https://github.com/golangci/golangci-lint) 1.50.1

## How to build it

Run `make build`.

## Testing

Run `make test` to run all unit and integration tests. To run the integration test, set the Amazon Redshift database URL
to the environment variables as an `REDSHIFT_URL`.