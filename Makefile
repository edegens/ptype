test:
	go test -v --race ./... -timeout=25s

lint:
	gofmt -w -s */**.go
	golangci-lint run
