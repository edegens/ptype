test:
	go test -v --race ./... -timeout=15s

lint:
	gofmt -w -s */**.go
	golangci-lint run
