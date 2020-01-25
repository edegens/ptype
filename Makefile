test:
	go test -v --race ./...

lint:
	gofmt -w -s */**.go
	golangci-lint run
