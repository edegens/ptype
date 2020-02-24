test:
	go test -v --race ./... -timeout=35s

lint:
	gofmt -w -s */**.go
	golangci-lint run
