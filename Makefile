test:
	go test -v --race ./...

simplify:
	gofmt -w -s */**.go
	golangci-lint run
