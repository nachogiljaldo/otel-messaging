.PHONY: test deps fmt lint clean

default: deps fmt lint test

test: deps
	@go clean -cache
	@go test ./pkg/...

fmt:
	@go fmt ./pkg/...

lint: utils/golangci-lint
	./utils/golangci-lint run ./pkg/...

utils/golangci-lint:
	@GOBIN=$(PWD)/utils go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest

deps:
	@go mod vendor
	@go mod tidy

clean:
	@rm utils/golangci-lint
	@rm -rf vendor
	@go clean -cache
